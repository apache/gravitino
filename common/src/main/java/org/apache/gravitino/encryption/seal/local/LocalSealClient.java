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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.gravitino.encryption.seal.SealClient;
import org.apache.gravitino.encryption.seal.SealConfigurationException;
import org.apache.gravitino.encryption.seal.SealOperationException;

/**
 * Seal client backed by a symmetric AES key read from a local file. Intended for development and
 * tests only: file permissions are the only protection for the key, which provides no trust
 * boundary separate from the data it protects.
 *
 * <p>The key file holds the Base64 encoding of a 128, 192, or 256-bit AES key; surrounding and
 * embedded whitespace is ignored. A wrapped blob is {@code iv ‖ ciphertext ‖ tag} with a 96-bit
 * random IV per wrap and a 128-bit GCM authentication tag.
 *
 * <p>The decoded key bytes are the only long-lived copy and are zeroed by {@link #close()}; each
 * operation builds a short-lived {@link SecretKeySpec} that is discarded when the operation
 * completes. Operations are synchronized so a concurrent {@code close()} cannot race a wrap or
 * unwrap in flight.
 */
final class LocalSealClient implements SealClient {

  private static final String TRANSFORMATION = "AES/GCM/NoPadding";
  private static final int GCM_TAG_BITS = 128;
  private static final int GCM_IV_BYTES = 12;
  private static final int MIN_BLOB_BYTES = GCM_IV_BYTES + GCM_TAG_BITS / Byte.SIZE;
  private static final int MAX_KEY_FILE_BYTES = 1024;

  private final byte[] keyBytes;
  private final SecureRandom random = new SecureRandom();
  private boolean closed;

  LocalSealClient(Path keyFile) {
    this.keyBytes = readKey(keyFile);
  }

  @Override
  public synchronized byte[] wrap(byte[] plaintext) {
    checkOpen();
    if (plaintext == null || plaintext.length == 0) {
      throw new IllegalArgumentException("Plaintext to wrap cannot be null or empty");
    }

    try {
      byte[] iv = new byte[GCM_IV_BYTES];
      random.nextBytes(iv);
      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(
          Cipher.ENCRYPT_MODE,
          new SecretKeySpec(keyBytes, "AES"),
          new GCMParameterSpec(GCM_TAG_BITS, iv));
      byte[] ciphertext = cipher.doFinal(plaintext);

      byte[] blob = new byte[iv.length + ciphertext.length];
      System.arraycopy(iv, 0, blob, 0, iv.length);
      System.arraycopy(ciphertext, 0, blob, iv.length, ciphertext.length);
      return blob;
    } catch (GeneralSecurityException e) {
      throw new SealOperationException(
          e, "Local seal failed to wrap %d bytes of key material", plaintext.length);
    }
  }

  @Override
  public synchronized byte[] unwrap(byte[] wrapped) {
    checkOpen();
    if (wrapped == null || wrapped.length < MIN_BLOB_BYTES) {
      throw new IllegalArgumentException(
          "Wrapped blob is null or too short to be a local seal blob");
    }

    try {
      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(
          Cipher.DECRYPT_MODE,
          new SecretKeySpec(keyBytes, "AES"),
          new GCMParameterSpec(GCM_TAG_BITS, wrapped, 0, GCM_IV_BYTES));
      return cipher.doFinal(wrapped, GCM_IV_BYTES, wrapped.length - GCM_IV_BYTES);
    } catch (GeneralSecurityException e) {
      throw new SealOperationException(
          e,
          "Local seal cannot unwrap a %d-byte blob: wrong key or corrupted data",
          wrapped.length);
    }
  }

  /** Zeroes the in-memory copy of the key. This operation is idempotent. */
  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    Arrays.fill(keyBytes, (byte) 0);
  }

  private void checkOpen() {
    if (closed) {
      throw new IllegalStateException("Local seal client is closed");
    }
  }

  private static byte[] readKey(Path keyFile) {
    byte[] fileBytes;
    try {
      if (!Files.isRegularFile(keyFile)) {
        throw new SealConfigurationException(
            "Local seal key file '%s' does not exist or is not a regular file", keyFile);
      }
      fileBytes = Files.readAllBytes(keyFile);
    } catch (IOException e) {
      throw new SealConfigurationException(e, "Local seal cannot read key file '%s'", keyFile);
    }

    try {
      if (fileBytes.length == 0 || fileBytes.length > MAX_KEY_FILE_BYTES) {
        throw new SealConfigurationException(
            "Local seal key file '%s' is not a Base64-encoded AES key file", keyFile);
      }

      byte[] base64Bytes = stripWhitespace(fileBytes);
      byte[] keyBytes;
      try {
        keyBytes = Base64.getDecoder().decode(base64Bytes);
      } catch (IllegalArgumentException e) {
        throw new SealConfigurationException(
            e, "Local seal key file '%s' is not valid Base64", keyFile);
      } finally {
        Arrays.fill(base64Bytes, (byte) 0);
      }

      if (!isSupportedKeyLength(keyBytes.length)) {
        Arrays.fill(keyBytes, (byte) 0);
        throw new SealConfigurationException(
            "Local seal key file '%s' must hold a Base64-encoded 128, 192, or 256-bit AES key",
            keyFile);
      }
      return keyBytes;
    } finally {
      // The encoded key must not linger in memory either.
      Arrays.fill(fileBytes, (byte) 0);
    }
  }

  private static boolean isSupportedKeyLength(int keyBytes) {
    return keyBytes == 16 || keyBytes == 24 || keyBytes == 32;
  }

  /**
   * Removes ASCII whitespace (the characters matched by {@code \s}) at the byte level, so the
   * encoded key never materializes as an un-wipeable {@link String}.
   */
  private static byte[] stripWhitespace(byte[] bytes) {
    byte[] compact = new byte[bytes.length];
    int size = 0;
    for (byte b : bytes) {
      if (b != ' ' && b != '\t' && b != '\n' && b != '\r' && b != '\f' && b != 0x0B) {
        compact[size++] = b;
      }
    }
    byte[] stripped = Arrays.copyOf(compact, size);
    Arrays.fill(compact, (byte) 0);
    return stripped;
  }
}
