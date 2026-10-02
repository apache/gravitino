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
package org.apache.gravitino.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestPasswordEncryptor {

  private static final String MASTER_KEY = "test-master-key-for-encryption";
  private static final String SYS_PROP_KEY = PasswordEncryptor.ENCRYPTION_KEY_SYSTEM_PROPERTY;

  @BeforeEach
  public void setUp() {
    System.clearProperty(SYS_PROP_KEY);
    // Use system property since we cannot set env vars directly in JVM
    System.setProperty(SYS_PROP_KEY, MASTER_KEY);
  }

  @AfterEach
  public void tearDown() {
    System.clearProperty(SYS_PROP_KEY);
  }

  @Test
  public void testIsEncrypted() {
    Assertions.assertFalse(PasswordEncryptor.isEncrypted(null));
    Assertions.assertFalse(PasswordEncryptor.isEncrypted(""));
    Assertions.assertFalse(PasswordEncryptor.isEncrypted("  "));
    Assertions.assertFalse(PasswordEncryptor.isEncrypted("plain-password"));
    Assertions.assertFalse(PasswordEncryptor.isEncrypted("ENC(incomplete"));
    Assertions.assertFalse(PasswordEncryptor.isEncrypted("incomplete)"));
    Assertions.assertTrue(PasswordEncryptor.isEncrypted("ENC(base64data)"));
    Assertions.assertTrue(PasswordEncryptor.isEncrypted("  ENC(base64data)  "));
  }

  @Test
  public void testEncryptAndDecrypt() {
    String plaintext = "mySecretPassword123!";
    String encrypted = PasswordEncryptor.encrypt(plaintext, MASTER_KEY);

    Assertions.assertTrue(PasswordEncryptor.isEncrypted(encrypted));
    Assertions.assertNotEquals(plaintext, encrypted);

    String decrypted = PasswordEncryptor.decrypt(encrypted, MASTER_KEY);
    Assertions.assertEquals(plaintext, decrypted);
  }

  @Test
  public void testEncryptProducesDifferentCiphertext() {
    String plaintext = "samePassword";
    String encrypted1 = PasswordEncryptor.encrypt(plaintext, MASTER_KEY);
    String encrypted2 = PasswordEncryptor.encrypt(plaintext, MASTER_KEY);

    Assertions.assertNotEquals(encrypted1, encrypted2);

    Assertions.assertEquals(plaintext, PasswordEncryptor.decrypt(encrypted1, MASTER_KEY));
    Assertions.assertEquals(plaintext, PasswordEncryptor.decrypt(encrypted2, MASTER_KEY));
  }

  @Test
  public void testDecryptWithWrongMasterKeyFails() {
    String plaintext = "myPassword";
    String encrypted = PasswordEncryptor.encrypt(plaintext, MASTER_KEY);

    assertThrows(RuntimeException.class, () -> PasswordEncryptor.decrypt(encrypted, "wrong-key"));
  }

  @Test
  public void testDecryptTamperedCiphertextFails() {
    String plaintext = "myPassword";
    String encrypted = PasswordEncryptor.encrypt(plaintext, MASTER_KEY);

    // Tamper with the base64 content
    String tampered = encrypted.substring(0, encrypted.length() - 2) + "AB)";

    assertThrows(RuntimeException.class, () -> PasswordEncryptor.decrypt(tampered, MASTER_KEY));
  }

  @Test
  public void testDecryptIfNeededWithPlaintext() {
    String plaintext = "plain-password-value";
    String result = PasswordEncryptor.decryptIfNeeded(plaintext);
    Assertions.assertEquals(plaintext, result);
  }

  @Test
  public void testDecryptIfNeededWithEncrypted() {
    String plaintext = "plain-password-value";
    String encrypted = PasswordEncryptor.encrypt(plaintext, MASTER_KEY);

    String result = PasswordEncryptor.decryptIfNeeded(encrypted);
    Assertions.assertEquals(plaintext, result);
  }

  @Test
  public void testDecryptIfNeededWithBlankAndNull() {
    Assertions.assertNull(PasswordEncryptor.decryptIfNeeded(null));
    Assertions.assertEquals("", PasswordEncryptor.decryptIfNeeded(""));
    Assertions.assertEquals("  ", PasswordEncryptor.decryptIfNeeded("  "));
  }

  @Test
  public void testDecryptIfNeededWithDefaultMasterKey() {
    String envKey = System.getenv(PasswordEncryptor.ENCRYPTION_KEY_ENV);
    if (envKey != null && !envKey.isEmpty()) {
      // Env var is set; use it as the master key instead of the default
      String plaintext = "gravitino";
      String encrypted = PasswordEncryptor.encrypt(plaintext, envKey);
      String result = PasswordEncryptor.decryptIfNeeded(encrypted);
      Assertions.assertEquals(plaintext, result);
      return;
    }
    System.clearProperty(SYS_PROP_KEY);
    String plaintext = "gravitino";
    String encrypted =
        PasswordEncryptor.encrypt(plaintext, PasswordEncryptor.DEFAULT_ENCRYPTION_KEY);

    // When no env var or system property is set, the default key should be used
    String result = PasswordEncryptor.decryptIfNeeded(encrypted);
    Assertions.assertEquals(plaintext, result);
  }

  @Test
  public void testEncryptWithSpecialCharacters() {
    String plaintext = "p@ssw0rd!#$%^&*()_+-=[]{}|;':\",./<>?`~";
    String encrypted = PasswordEncryptor.encrypt(plaintext, MASTER_KEY);
    String decrypted = PasswordEncryptor.decrypt(encrypted, MASTER_KEY);
    Assertions.assertEquals(plaintext, decrypted);
  }

  @Test
  public void testEncryptWithUnicode() {
    String plaintext = "密码123password";
    String encrypted = PasswordEncryptor.encrypt(plaintext, MASTER_KEY);
    String decrypted = PasswordEncryptor.decrypt(encrypted, MASTER_KEY);
    Assertions.assertEquals(plaintext, decrypted);
  }

  @Test
  public void testEncryptWithEmptyStringFails() {
    assertThrows(IllegalArgumentException.class, () -> PasswordEncryptor.encrypt("", MASTER_KEY));
  }

  @Test
  public void testEncryptWithBlankMasterKeyFails() {
    assertThrows(IllegalArgumentException.class, () -> PasswordEncryptor.encrypt("plaintext", ""));
  }

  @Test
  public void testDecryptNonEncryptedValueFails() {
    assertThrows(
        IllegalArgumentException.class, () -> PasswordEncryptor.decrypt("plain-text", MASTER_KEY));
  }

  @Test
  public void testResolveMasterPasswordFallbackToDefault() {
    // This test assumes GRAVITINO_PASSWORD_ENCRYPTION_KEY env var is not set in the
    // test environment. If it is set, the fallback-to-default behavior cannot be verified.
    String envKey = System.getenv(PasswordEncryptor.ENCRYPTION_KEY_ENV);
    System.clearProperty(SYS_PROP_KEY);
    if (envKey != null && !envKey.isEmpty()) {
      // Env var is set; skip fallback test but verify system property still wins
      System.setProperty(SYS_PROP_KEY, "sys-prop-key");
      Assertions.assertEquals("sys-prop-key", PasswordEncryptor.resolveMasterPassword());
      System.clearProperty(SYS_PROP_KEY);
      return;
    }
    // When no env var or system property is set, returns the default key
    Assertions.assertEquals(
        PasswordEncryptor.DEFAULT_ENCRYPTION_KEY, PasswordEncryptor.resolveMasterPassword());

    System.setProperty(SYS_PROP_KEY, "sys-prop-key");
    Assertions.assertEquals("sys-prop-key", PasswordEncryptor.resolveMasterPassword());
  }
}
