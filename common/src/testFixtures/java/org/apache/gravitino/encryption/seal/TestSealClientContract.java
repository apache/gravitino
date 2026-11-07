/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.encryption.seal;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Common wrap/unwrap contract for implemented seal clients. */
public abstract class TestSealClientContract {

  private static final SecureRandom RANDOM = new SecureRandom();

  /**
   * Returns a new client under test. Each call must return an independent instance so a test can
   * close it without affecting other tests.
   *
   * @return the client
   * @throws Exception if the client cannot be created
   */
  protected abstract SealClient newClient() throws Exception;

  @Test
  void testWrapUnwrapRoundTrip() throws Exception {
    SealClient client = newClient();
    try {
      for (int size : new int[] {1, 16, 32, 1024}) {
        byte[] plaintext = randomBytes(size);
        byte[] wrapped = client.wrap(plaintext);
        Assertions.assertNotNull(wrapped, "Seal clients must not return null");
        Assertions.assertArrayEquals(plaintext, client.unwrap(wrapped));
      }
    } finally {
      client.close();
    }
  }

  @Test
  void testWrapReturnsDistinctBlobsForSamePlaintext() throws Exception {
    SealClient client = newClient();
    try {
      byte[] plaintext = randomBytes(32);

      byte[] first = client.wrap(plaintext);
      byte[] second = client.wrap(plaintext);

      Assertions.assertFalse(
          Arrays.equals(first, second),
          "Each wrap must use fresh randomness, producing a distinct blob");
      Assertions.assertArrayEquals(plaintext, client.unwrap(first));
      Assertions.assertArrayEquals(plaintext, client.unwrap(second));
    } finally {
      client.close();
    }
  }

  @Test
  void testWrappedBlobDoesNotContainPlaintext() throws Exception {
    SealClient client = newClient();
    try {
      byte[] plaintext = randomBytes(32);

      byte[] wrapped = client.wrap(plaintext);

      Assertions.assertFalse(
          containsBytes(wrapped, plaintext),
          "A wrapped blob must not contain plaintext key material");
    } finally {
      client.close();
    }
  }

  @Test
  void testRejectsNullOrEmptyPlaintext() throws Exception {
    SealClient client = newClient();
    try {
      Assertions.assertThrows(IllegalArgumentException.class, () -> client.wrap(null));
      Assertions.assertThrows(IllegalArgumentException.class, () -> client.wrap(new byte[0]));
    } finally {
      client.close();
    }
  }

  @Test
  void testRejectsNullBlob() throws Exception {
    SealClient client = newClient();
    try {
      Assertions.assertThrows(IllegalArgumentException.class, () -> client.unwrap(null));
    } finally {
      client.close();
    }
  }

  @Test
  void testRejectsTamperedBlob() throws Exception {
    SealClient client = newClient();
    try {
      byte[] tampered = client.wrap(randomBytes(32));
      int last = tampered.length - 1;
      tampered[last] = (byte) (tampered[last] ^ 0x01);

      Assertions.assertThrows(SealOperationException.class, () -> client.unwrap(tampered));
    } finally {
      client.close();
    }
  }

  @Test
  void testCloseIsIdempotentAndDisablesOperations() throws Exception {
    SealClient client = newClient();
    byte[] wrapped = client.wrap(randomBytes(32));

    client.close();
    client.close();

    Assertions.assertThrows(IllegalStateException.class, () -> client.wrap(randomBytes(32)));
    Assertions.assertThrows(IllegalStateException.class, () -> client.unwrap(wrapped));
  }

  @Test
  void testConcurrentWrapUnwrap() throws Exception {
    SealClient client = newClient();
    int threads = 8;
    int wrapsPerThread = 25;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<Void>> results = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        results.add(
            executor.submit(
                (Callable<Void>)
                    () -> {
                      for (int j = 0; j < wrapsPerThread; j++) {
                        byte[] plaintext = randomBytes(32);
                        Assertions.assertArrayEquals(
                            plaintext, client.unwrap(client.wrap(plaintext)));
                      }
                      return null;
                    }));
      }
      for (Future<Void> result : results) {
        Assertions.assertDoesNotThrow(() -> result.get());
      }
    } finally {
      executor.shutdownNow();
      client.close();
    }
  }

  private static byte[] randomBytes(int length) {
    byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  private static boolean containsBytes(byte[] blob, byte[] plaintext) {
    for (int offset = 0; offset + plaintext.length <= blob.length; offset++) {
      int index = 0;
      while (index < plaintext.length && blob[offset + index] == plaintext[index]) {
        index++;
      }
      if (index == plaintext.length) {
        return true;
      }
    }
    return false;
  }
}
