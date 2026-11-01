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
package org.apache.gravitino.encryption.seal.local;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import org.apache.gravitino.encryption.seal.SealClient;
import org.apache.gravitino.encryption.seal.SealConfigurationException;
import org.apache.gravitino.encryption.seal.SealOperationException;
import org.apache.gravitino.encryption.seal.TestSealClientContract;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestLocalSealClient extends TestSealClientContract {

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final String PROVIDER = "local";
  private static final int IV_BYTES = 12;
  private static final int TAG_BYTES = 16;

  @TempDir Path tempDir;

  @Override
  protected SealClient newClient() throws Exception {
    return new LocalSealClient(writeKeyFile(32));
  }

  @Test
  void testRoundTripForSupportedKeySizes() throws Exception {
    for (int keyBytes : new int[] {16, 24, 32}) {
      try (SealClient client = new LocalSealClient(writeKeyFile(keyBytes))) {
        byte[] plaintext = randomBytes(32);
        Assertions.assertArrayEquals(plaintext, client.unwrap(client.wrap(plaintext)));
      }
    }
  }

  @Test
  void testWrappedBlobLayout() throws Exception {
    try (SealClient client = newClient()) {
      byte[] plaintext = randomBytes(32);

      byte[] wrapped = client.wrap(plaintext);

      Assertions.assertEquals(IV_BYTES + plaintext.length + TAG_BYTES, wrapped.length);
    }
  }

  @Test
  void testRejectsMalformedBlob() throws Exception {
    try (SealClient client = newClient()) {
      Assertions.assertThrows(IllegalArgumentException.class, () -> client.unwrap(new byte[0]));
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> client.unwrap(new byte[IV_BYTES + TAG_BYTES - 1]));
    }
  }

  @Test
  void testRejectsBlobWrappedByAnotherKey() throws Exception {
    try (SealClient client = newClient();
        SealClient other = newClient()) {
      byte[] foreign = other.wrap(randomBytes(32));

      Assertions.assertThrows(SealOperationException.class, () -> client.unwrap(foreign));
    }
  }

  @Test
  void testFactoryCreatesClientFromKeyFileProperty() throws Exception {
    Path keyFile = writeKeyFile(32);
    LocalSealClientFactory factory = new LocalSealClientFactory();

    try (SealClient client =
        factory.create(PROVIDER, Map.of(LocalSealClientFactory.KEY_FILE, keyFile.toString()))) {
      byte[] plaintext = randomBytes(32);
      Assertions.assertArrayEquals(plaintext, client.unwrap(client.wrap(plaintext)));
    }
  }

  @Test
  void testFactoryAcceptsBase64KeyFileWithWhitespace() throws Exception {
    String base64 = Base64.getEncoder().encodeToString(randomBytes(32));
    Path keyFile = tempDir.resolve("wrapped-key");
    Files.writeString(keyFile, "  " + base64.substring(0, 10) + "\n" + base64.substring(10) + "\n");

    LocalSealClientFactory factory = new LocalSealClientFactory();
    try (SealClient client =
        factory.create(PROVIDER, Map.of(LocalSealClientFactory.KEY_FILE, keyFile.toString()))) {
      byte[] plaintext = randomBytes(32);
      Assertions.assertArrayEquals(plaintext, client.unwrap(client.wrap(plaintext)));
    }
  }

  @Test
  void testFactoryRejectsInvalidConfiguration() throws Exception {
    LocalSealClientFactory factory = new LocalSealClientFactory();

    Assertions.assertThrows(
        SealConfigurationException.class, () -> factory.create(PROVIDER, Map.of()));
    Assertions.assertThrows(
        SealConfigurationException.class,
        () -> factory.create(PROVIDER, Map.of(LocalSealClientFactory.KEY_FILE, "  ")));
    Assertions.assertThrows(
        SealConfigurationException.class,
        () ->
            factory.create(
                PROVIDER,
                Map.of(LocalSealClientFactory.KEY_FILE, tempDir.resolve("missing").toString())));
    Assertions.assertThrows(
        SealConfigurationException.class,
        () ->
            factory.create(PROVIDER, Map.of(LocalSealClientFactory.KEY_FILE, tempDir.toString())));

    Path notBase64 = tempDir.resolve("not-base64");
    Files.writeString(notBase64, "!!! not base64 !!!");
    Assertions.assertThrows(
        SealConfigurationException.class,
        () ->
            factory.create(
                PROVIDER, Map.of(LocalSealClientFactory.KEY_FILE, notBase64.toString())));

    Path shortKey = tempDir.resolve("short-key");
    Files.writeString(shortKey, Base64.getEncoder().encodeToString(randomBytes(10)));
    Assertions.assertThrows(
        SealConfigurationException.class,
        () ->
            factory.create(PROVIDER, Map.of(LocalSealClientFactory.KEY_FILE, shortKey.toString())));
  }

  private Path writeKeyFile(int keyBytes) throws Exception {
    Path keyFile = Files.createTempFile(tempDir, "seal-key", ".b64");
    Files.writeString(keyFile, Base64.getEncoder().encodeToString(randomBytes(keyBytes)));
    return keyFile;
  }

  private static byte[] randomBytes(int length) {
    byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    return bytes;
  }
}
