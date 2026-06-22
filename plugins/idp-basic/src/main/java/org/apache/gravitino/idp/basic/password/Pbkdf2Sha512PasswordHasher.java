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

package org.apache.gravitino.idp.basic.password;

import com.google.common.base.Preconditions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.apache.commons.lang3.StringUtils;

/** PBKDF2-HMAC-SHA512 password hasher backed by the JDK JCA provider. */
public class Pbkdf2Sha512PasswordHasher implements PasswordHasher {

  private static final String PHC_PREFIX = Pbkdf2Sha512Defaults.PHC_PREFIX;
  private static final String INVALID_HASH_FORMAT = "Invalid PBKDF2 hash format";
  private static final String UNSUPPORTED_HASH_PARAMETERS = "Unsupported PBKDF2 hash parameters";
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  private static final SecretKeyFactory SECRET_KEY_FACTORY;

  static {
    try {
      SECRET_KEY_FACTORY = SecretKeyFactory.getInstance(Pbkdf2Sha512Defaults.PBKDF2_ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Override
  public String hash(String plainPassword) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(plainPassword), "Plain password must not be blank");

    byte[] salt = new byte[Pbkdf2Sha512Defaults.DEFAULT_SALT_LENGTH];
    SECURE_RANDOM.nextBytes(salt);
    try {
      byte[] hash =
          deriveKey(
              plainPassword,
              salt,
              Pbkdf2Sha512Defaults.DEFAULT_ITERATIONS,
              Pbkdf2Sha512Defaults.DEFAULT_HASH_LENGTH);
      return toPhcString(salt, hash);
    } finally {
      Arrays.fill(salt, (byte) 0);
    }
  }

  @Override
  public boolean verify(String plainPassword, String hashedPassword) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(plainPassword), "Plain password must not be blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(hashedPassword), "Hashed password must not be blank");

    ParsedHash parsedHash = parse(hashedPassword);
    byte[] actualHash =
        deriveKey(plainPassword, parsedHash.salt, parsedHash.iterations, parsedHash.hash.length);
    try {
      return MessageDigest.isEqual(actualHash, parsedHash.hash);
    } finally {
      Arrays.fill(actualHash, (byte) 0);
      Arrays.fill(parsedHash.salt, (byte) 0);
      Arrays.fill(parsedHash.hash, (byte) 0);
    }
  }

  private static byte[] deriveKey(
      String plainPassword, byte[] salt, int iterations, int hashLength) {
    PBEKeySpec spec = new PBEKeySpec(plainPassword.toCharArray(), salt, iterations, hashLength * 8);
    try {
      return SECRET_KEY_FACTORY.generateSecret(spec).getEncoded();
    } catch (InvalidKeySpecException e) {
      throw new IllegalStateException("Failed to derive password hash", e);
    } finally {
      spec.clearPassword();
    }
  }

  private static String toPhcString(byte[] salt, byte[] hash) {
    Base64.Encoder encoder = Base64.getEncoder().withoutPadding();
    return PHC_PREFIX
        + "i="
        + Pbkdf2Sha512Defaults.DEFAULT_ITERATIONS
        + "$"
        + encoder.encodeToString(salt)
        + "$"
        + encoder.encodeToString(hash);
  }

  private static ParsedHash parse(String hashedPassword) {
    Preconditions.checkArgument(hashedPassword.startsWith(PHC_PREFIX), INVALID_HASH_FORMAT);
    String[] parts = hashedPassword.split("\\$");
    Preconditions.checkArgument(parts.length == 5, INVALID_HASH_FORMAT);
    Preconditions.checkArgument("pbkdf2-sha512".equals(parts[1]), INVALID_HASH_FORMAT);
    Preconditions.checkArgument(parts[2].startsWith("i="), INVALID_HASH_FORMAT);

    int iterations;
    try {
      iterations = Integer.parseInt(parts[2].substring(2));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(INVALID_HASH_FORMAT);
    }

    byte[] salt = decodeBase64(parts[3], Pbkdf2Sha512Defaults.DEFAULT_SALT_LENGTH);
    byte[] hash = decodeBase64(parts[4], Pbkdf2Sha512Defaults.DEFAULT_HASH_LENGTH);
    Preconditions.checkArgument(
        iterations == Pbkdf2Sha512Defaults.DEFAULT_ITERATIONS, UNSUPPORTED_HASH_PARAMETERS);
    Preconditions.checkArgument(
        salt.length == Pbkdf2Sha512Defaults.DEFAULT_SALT_LENGTH, INVALID_HASH_FORMAT);
    Preconditions.checkArgument(
        hash.length == Pbkdf2Sha512Defaults.DEFAULT_HASH_LENGTH, INVALID_HASH_FORMAT);
    return new ParsedHash(iterations, salt, hash);
  }

  private static byte[] decodeBase64(String value, int maxDecodedLength) {
    int fullGroups = maxDecodedLength / 3;
    int decodedRemainder = maxDecodedLength % 3;
    int maxEncodedLength = fullGroups * 4 + (decodedRemainder == 0 ? 0 : decodedRemainder + 1);
    Preconditions.checkArgument(value.length() <= maxEncodedLength, INVALID_HASH_FORMAT);
    int remainder = value.length() % 4;
    Preconditions.checkArgument(remainder != 1, INVALID_HASH_FORMAT);
    String paddedValue = remainder == 0 ? value : value + "====".substring(0, 4 - remainder);
    try {
      return Base64.getDecoder().decode(paddedValue);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(INVALID_HASH_FORMAT);
    }
  }

  private static class ParsedHash {
    private final int iterations;
    private final byte[] salt;
    private final byte[] hash;

    private ParsedHash(int iterations, byte[] salt, byte[] hash) {
      this.iterations = iterations;
      this.salt = salt;
      this.hash = hash;
    }
  }
}
