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

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Utility class for encrypting and decrypting password strings using AES-256-GCM.
 *
 * <p>Encrypted values are wrapped in {@code ENC(...)} format. The {@link #decryptIfNeeded} method
 * transparently decrypts such values while passing through plain-text values unchanged, ensuring
 * backward compatibility.
 *
 * <p>The master encryption key is resolved from (in priority order):
 *
 * <ol>
 *   <li>Environment variable {@code GRAVITINO_PASSWORD_ENCRYPTION_KEY}
 *   <li>System property {@code gravitino.password.encryption.key}
 *   <li>Default key: {@value #DEFAULT_ENCRYPTION_KEY}
 * </ol>
 *
 * <p>Encryption parameters:
 *
 * <ul>
 *   <li>Algorithm: AES/GCM/NoPadding (256-bit key)
 *   <li>Key derivation: PBKDF2WithHmacSHA256 (65536 iterations)
 *   <li>Salt: 16 random bytes (per-encryption)
 *   <li>IV: 12 random bytes (per-encryption)
 *   <li>Authentication tag: 128 bits
 * </ul>
 *
 * <p>Encrypted format: {@code ENC(base64(salt[16] + iv[12] + ciphertext + tag))}
 */
public class PasswordEncryptor {

  private static volatile boolean defaultKeyWarningLogged = false;

  /** Prefix that marks a value as encrypted. */
  public static final String ENCRYPTED_PREFIX = "ENC(";

  /** Suffix that marks a value as encrypted. */
  public static final String ENCRYPTED_SUFFIX = ")";

  /** Environment variable name for the master encryption key. */
  public static final String ENCRYPTION_KEY_ENV = "GRAVITINO_PASSWORD_ENCRYPTION_KEY";

  /** System property name for the master encryption key. */
  public static final String ENCRYPTION_KEY_SYSTEM_PROPERTY = "gravitino.password.encryption.key";

  /** Default master encryption key used when no key is explicitly configured. */
  public static final String DEFAULT_ENCRYPTION_KEY = "gravitino";

  private static final String CIPHER_ALGORITHM = "AES/GCM/NoPadding";
  private static final String KEY_DERIVATION_ALGORITHM = "PBKDF2WithHmacSHA256";
  private static final String KEY_ALGORITHM = "AES";
  private static final int KEY_LENGTH_BITS = 256;
  private static final int SALT_LENGTH = 16;
  private static final int IV_LENGTH = 12;
  private static final int TAG_LENGTH_BITS = 128;
  private static final int PBKDF2_ITERATIONS = 65536;

  private PasswordEncryptor() {}

  /**
   * Checks whether the given value is in encrypted {@code ENC(...)} format.
   *
   * @param value The value to check.
   * @return {@code true} if the value is encrypted, {@code false} otherwise.
   */
  public static boolean isEncrypted(String value) {
    if (isBlank(value)) {
      return false;
    }
    String trimmed = value.trim();
    return trimmed.startsWith(ENCRYPTED_PREFIX) && trimmed.endsWith(ENCRYPTED_SUFFIX);
  }

  /**
   * Encrypts a plain-text password using AES-256-GCM.
   *
   * @param plaintext The plain-text password to encrypt.
   * @param masterPassword The master password used for key derivation.
   * @return The encrypted value in {@code ENC(base64(...))} format.
   */
  public static String encrypt(String plaintext, String masterPassword) {
    if (isBlank(plaintext)) {
      throw new IllegalArgumentException("Plaintext to encrypt must not be blank");
    }
    if (isBlank(masterPassword)) {
      throw new IllegalArgumentException("Master encryption key must not be blank");
    }

    try {
      byte[] salt = new byte[SALT_LENGTH];
      byte[] iv = new byte[IV_LENGTH];
      SecureRandom random = new SecureRandom();
      random.nextBytes(salt);
      random.nextBytes(iv);

      SecretKey key = deriveKey(masterPassword, salt);

      Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
      GCMParameterSpec spec = new GCMParameterSpec(TAG_LENGTH_BITS, iv);
      cipher.init(Cipher.ENCRYPT_MODE, key, spec);
      byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

      byte[] combined = new byte[SALT_LENGTH + IV_LENGTH + ciphertext.length];
      System.arraycopy(salt, 0, combined, 0, SALT_LENGTH);
      System.arraycopy(iv, 0, combined, SALT_LENGTH, IV_LENGTH);
      System.arraycopy(ciphertext, 0, combined, SALT_LENGTH + IV_LENGTH, ciphertext.length);

      return ENCRYPTED_PREFIX + Base64.getEncoder().encodeToString(combined) + ENCRYPTED_SUFFIX;
    } catch (Exception e) {
      throw new RuntimeException("Failed to encrypt password", e);
    }
  }

  /**
   * Decrypts an {@code ENC(...)} formatted value using AES-256-GCM.
   *
   * @param encryptedValue The encrypted value in {@code ENC(base64(...))} format.
   * @param masterPassword The master password used for key derivation.
   * @return The decrypted plain-text password.
   */
  public static String decrypt(String encryptedValue, String masterPassword) {
    if (!isEncrypted(encryptedValue)) {
      throw new IllegalArgumentException("Value is not in encrypted ENC(...) format");
    }
    if (isBlank(masterPassword)) {
      throw new IllegalArgumentException("Master encryption key must not be blank");
    }

    try {
      String base64Content = extractEncryptedContent(encryptedValue.trim());
      byte[] decoded = Base64.getDecoder().decode(base64Content);

      if (decoded.length < SALT_LENGTH + IV_LENGTH + 1) {
        throw new IllegalArgumentException("Encrypted data is too short");
      }

      byte[] salt = Arrays.copyOfRange(decoded, 0, SALT_LENGTH);
      byte[] iv = Arrays.copyOfRange(decoded, SALT_LENGTH, SALT_LENGTH + IV_LENGTH);
      byte[] ciphertext = Arrays.copyOfRange(decoded, SALT_LENGTH + IV_LENGTH, decoded.length);

      SecretKey key = deriveKey(masterPassword, salt);

      Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
      GCMParameterSpec spec = new GCMParameterSpec(TAG_LENGTH_BITS, iv);
      cipher.init(Cipher.DECRYPT_MODE, key, spec);
      byte[] plaintext = cipher.doFinal(ciphertext);

      return new String(plaintext, StandardCharsets.UTF_8);
    } catch (AEADBadTagException e) {
      throw new RuntimeException(
          "Failed to decrypt password: the master encryption key is incorrect or the encrypted"
              + " data has been tampered with. Please verify that the master key matches the one"
              + " used during encryption.",
          e);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to decrypt password", e);
    }
  }

  /**
   * Decrypts the value if it is in {@code ENC(...)} format; otherwise returns it unchanged.
   *
   * <p>The master encryption key is resolved from the environment variable, system property, or the
   * default key ({@value #DEFAULT_ENCRYPTION_KEY}).
   *
   * @param value The value that may or may not be encrypted.
   * @return The decrypted plain-text value, or the original value if not encrypted.
   * @throws RuntimeException If decryption fails.
   */
  public static String decryptIfNeeded(String value) {
    if (!isEncrypted(value)) {
      return value;
    }

    String masterPassword = resolveMasterPassword();
    return decrypt(value, masterPassword);
  }

  /**
   * Resolves the master encryption key from environment variable or system property.
   *
   * <p>Priority: environment variable {@code GRAVITINO_PASSWORD_ENCRYPTION_KEY} > system property
   * {@code gravitino.password.encryption.key} > default key {@value #DEFAULT_ENCRYPTION_KEY}.
   *
   * @return The master encryption key, never {@code null}.
   */
  static String resolveMasterPassword() {
    String key = System.getenv(ENCRYPTION_KEY_ENV);
    if (isBlank(key)) {
      key = System.getProperty(ENCRYPTION_KEY_SYSTEM_PROPERTY);
    }
    if (isBlank(key)) {
      return DEFAULT_ENCRYPTION_KEY;
    }
    return key;
  }

  /**
   * Checks whether the given config value is encrypted and the default master key is currently in
   * use. If both conditions are met, returns {@code true} only once per JVM so the caller can log a
   * warning. This prevents false warnings for plain-text passwords that never trigger decryption.
   *
   * @param configValue The raw config value (may be plain text or {@code ENC(...)}).
   * @return {@code true} if the value is encrypted, the default key is in use, and this is the
   *     first such call; {@code false} otherwise.
   */
  public static boolean shouldWarnDefaultKey(String configValue) {
    if (!isEncrypted(configValue)) {
      return false;
    }
    if (!DEFAULT_ENCRYPTION_KEY.equals(resolveMasterPassword())) {
      return false;
    }
    if (!defaultKeyWarningLogged) {
      synchronized (PasswordEncryptor.class) {
        if (!defaultKeyWarningLogged) {
          defaultKeyWarningLogged = true;
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Extracts the Base64 content from an {@code ENC(base64...)} formatted string.
   *
   * @param encryptedValue The encrypted value.
   * @return The Base64 content string.
   */
  private static String extractEncryptedContent(String encryptedValue) {
    return encryptedValue.substring(
        ENCRYPTED_PREFIX.length(), encryptedValue.length() - ENCRYPTED_SUFFIX.length());
  }

  /**
   * Derives an AES-256 secret key from the master password and salt using PBKDF2WithHmacSHA256.
   *
   * @param masterPassword The master password.
   * @param salt The salt bytes.
   * @return The derived {@link SecretKey}.
   * @throws Exception If key derivation fails.
   */
  private static SecretKey deriveKey(String masterPassword, byte[] salt) throws Exception {
    PBEKeySpec keySpec =
        new PBEKeySpec(masterPassword.toCharArray(), salt, PBKDF2_ITERATIONS, KEY_LENGTH_BITS);
    try {
      SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEY_DERIVATION_ALGORITHM);
      byte[] keyBytes = keyFactory.generateSecret(keySpec).getEncoded();
      return new SecretKeySpec(keyBytes, KEY_ALGORITHM);
    } finally {
      keySpec.clearPassword();
    }
  }

  /**
   * Checks if the string is null, empty, or contains only whitespace.
   *
   * @param str The string to check.
   * @return {@code true} if the string is blank, {@code false} otherwise.
   */
  private static boolean isBlank(String str) {
    return str == null || str.trim().isEmpty();
  }
}
