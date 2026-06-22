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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import org.apache.commons.lang3.StringUtils;

/**
 * SHA3-512 password hasher backed by the JDK {@link MessageDigest} API.
 *
 * <p>Built-in IdP password hashing is limited to the JDK JCA provider to reduce EAR Category 5D002
 * export-control scope for ASF binary releases.
 */
public class Sha3512PasswordHasher implements PasswordHasher {

  private static final String PHC_PREFIX = Sha3512Defaults.PHC_PREFIX;
  private static final String INVALID_HASH_FORMAT = "Invalid SHA3-512 hash format";
  private static final String UNSUPPORTED_HASH_PARAMETERS = "Unsupported SHA3-512 hash parameters";
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  @Override
  public String hash(String plainPassword) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(plainPassword), "Plain password must not be blank");

    byte[] salt = new byte[Sha3512Defaults.DEFAULT_SALT_LENGTH];
    SECURE_RANDOM.nextBytes(salt);
    byte[] passwordBytes = plainPassword.getBytes(StandardCharsets.UTF_8);
    try {
      byte[] hash =
          digestPassword(
              passwordBytes,
              salt,
              Sha3512Defaults.DEFAULT_ITERATIONS,
              Sha3512Defaults.DEFAULT_HASH_LENGTH);
      return toPhcString(salt, hash);
    } finally {
      Arrays.fill(passwordBytes, (byte) 0);
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
    byte[] passwordBytes = plainPassword.getBytes(StandardCharsets.UTF_8);
    byte[] actualHash = null;
    try {
      actualHash =
          digestPassword(
              passwordBytes, parsedHash.salt, parsedHash.iterations, parsedHash.hash.length);
      return MessageDigest.isEqual(actualHash, parsedHash.hash);
    } finally {
      Arrays.fill(passwordBytes, (byte) 0);
      if (actualHash != null) {
        Arrays.fill(actualHash, (byte) 0);
      }
      Arrays.fill(parsedHash.salt, (byte) 0);
      Arrays.fill(parsedHash.hash, (byte) 0);
    }
  }

  private static byte[] digestPassword(
      byte[] passwordBytes, byte[] salt, int iterations, int hashLength) {
    MessageDigest digest = newDigest();
    digest.update(salt);
    digest.update(passwordBytes);
    byte[] hash = digest.digest();
    for (int i = 1; i < iterations; i++) {
      hash = digest.digest(hash);
    }
    if (hash.length == hashLength) {
      return hash;
    }
    return Arrays.copyOf(hash, hashLength);
  }

  private static MessageDigest newDigest() {
    try {
      return MessageDigest.getInstance(Sha3512Defaults.DIGEST_ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA3-512 is not supported by the JCA provider", e);
    }
  }

  private static String toPhcString(byte[] salt, byte[] hash) {
    Base64.Encoder encoder = Base64.getEncoder().withoutPadding();
    return PHC_PREFIX
        + "i="
        + Sha3512Defaults.DEFAULT_ITERATIONS
        + "$"
        + encoder.encodeToString(salt)
        + "$"
        + encoder.encodeToString(hash);
  }

  private static ParsedHash parse(String hashedPassword) {
    Preconditions.checkArgument(hashedPassword.startsWith(PHC_PREFIX), INVALID_HASH_FORMAT);
    String[] parts = hashedPassword.split("\\$");
    Preconditions.checkArgument(parts.length == 5, INVALID_HASH_FORMAT);
    Preconditions.checkArgument("sha3-512".equals(parts[1]), INVALID_HASH_FORMAT);
    Preconditions.checkArgument(parts[2].startsWith("i="), INVALID_HASH_FORMAT);

    int iterations;
    try {
      iterations = Integer.parseInt(parts[2].substring(2));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(INVALID_HASH_FORMAT);
    }

    byte[] salt = decodeBase64(parts[3], Sha3512Defaults.DEFAULT_SALT_LENGTH);
    byte[] hash = decodeBase64(parts[4], Sha3512Defaults.DEFAULT_HASH_LENGTH);
    Preconditions.checkArgument(
        iterations == Sha3512Defaults.DEFAULT_ITERATIONS, UNSUPPORTED_HASH_PARAMETERS);
    Preconditions.checkArgument(
        salt.length == Sha3512Defaults.DEFAULT_SALT_LENGTH, INVALID_HASH_FORMAT);
    Preconditions.checkArgument(
        hash.length == Sha3512Defaults.DEFAULT_HASH_LENGTH, INVALID_HASH_FORMAT);
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
