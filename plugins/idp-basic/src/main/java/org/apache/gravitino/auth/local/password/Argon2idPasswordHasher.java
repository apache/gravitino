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

package org.apache.gravitino.auth.local.password;

import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.crypto.generators.Argon2BytesGenerator;
import org.bouncycastle.crypto.params.Argon2Parameters;
import org.bouncycastle.util.Arrays;

/** Argon2id-based password hasher. */
public class Argon2idPasswordHasher implements PasswordHasher {

  private static final String PHC_PREFIX = "$argon2id$";
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  @Override
  public String hash(String plainPassword) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(plainPassword), "Plain password must not be blank");

    byte[] salt = new byte[Argon2idDefaults.DEFAULT_SALT_LENGTH];
    SECURE_RANDOM.nextBytes(salt);
    byte[] passwordBytes = plainPassword.getBytes(StandardCharsets.UTF_8);
    byte[] hash = new byte[Argon2idDefaults.DEFAULT_HASH_LENGTH];
    try {
      generateHash(
          passwordBytes,
          salt,
          Argon2idDefaults.DEFAULT_ITERATIONS,
          Argon2idDefaults.DEFAULT_MEMORY_KB,
          Argon2idDefaults.DEFAULT_PARALLELISM,
          Argon2idDefaults.DEFAULT_VERSION,
          hash);
      return toPhcString(
          salt,
          hash,
          Argon2idDefaults.DEFAULT_ITERATIONS,
          Argon2idDefaults.DEFAULT_MEMORY_KB,
          Argon2idDefaults.DEFAULT_PARALLELISM,
          Argon2idDefaults.DEFAULT_VERSION);
    } finally {
      Arrays.clear(passwordBytes);
      Arrays.clear(salt);
      Arrays.clear(hash);
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
    byte[] actualHash = new byte[parsedHash.hash.length];
    try {
      generateHash(
          passwordBytes,
          parsedHash.salt,
          parsedHash.iterations,
          parsedHash.memoryKb,
          parsedHash.parallelism,
          parsedHash.version,
          actualHash);
      return MessageDigest.isEqual(actualHash, parsedHash.hash);
    } finally {
      Arrays.clear(passwordBytes);
      Arrays.clear(actualHash);
      Arrays.clear(parsedHash.salt);
      Arrays.clear(parsedHash.hash);
    }
  }

  private static void generateHash(
      byte[] passwordBytes,
      byte[] salt,
      int iterations,
      int memoryKb,
      int parallelism,
      int version,
      byte[] output) {
    Argon2Parameters parameters =
        new Argon2Parameters.Builder(Argon2idDefaults.DEFAULT_TYPE)
            .withVersion(version)
            .withIterations(iterations)
            .withMemoryAsKB(memoryKb)
            .withParallelism(parallelism)
            .withSalt(salt)
            .build();
    Argon2BytesGenerator generator = new Argon2BytesGenerator();
    generator.init(parameters);
    generator.generateBytes(passwordBytes, output);
  }

  private static String toPhcString(
      byte[] salt, byte[] hash, int iterations, int memoryKb, int parallelism, int version) {
    Base64.Encoder encoder = Base64.getEncoder().withoutPadding();
    return PHC_PREFIX
        + "v="
        + version
        + "$m="
        + memoryKb
        + ",t="
        + iterations
        + ",p="
        + parallelism
        + "$"
        + encoder.encodeToString(salt)
        + "$"
        + encoder.encodeToString(hash);
  }

  private static ParsedHash parse(String hashedPassword) {
    Preconditions.checkArgument(hashedPassword.startsWith(PHC_PREFIX), invalidHashFormatMessage());
    String[] parts = hashedPassword.split("\\$");
    Preconditions.checkArgument(parts.length == 6, invalidHashFormatMessage());
    Preconditions.checkArgument("argon2id".equals(parts[1]), invalidHashFormatMessage());
    Preconditions.checkArgument(parts[2].startsWith("v="), invalidHashFormatMessage());
    Preconditions.checkArgument(parts[3].startsWith("m="), invalidHashFormatMessage());

    String[] parameterParts = parts[3].split(",");
    Preconditions.checkArgument(parameterParts.length == 3, invalidHashFormatMessage());
    Preconditions.checkArgument(parameterParts[1].startsWith("t="), invalidHashFormatMessage());
    Preconditions.checkArgument(parameterParts[2].startsWith("p="), invalidHashFormatMessage());

    ParsedHash parsedHash =
        new ParsedHash(
            parseInteger(parts[2].substring(2)),
            parseInteger(parameterParts[0].substring(2)),
            parseInteger(parameterParts[1].substring(2)),
            parseInteger(parameterParts[2].substring(2)),
            decodeBase64(parts[4], Argon2idDefaults.DEFAULT_SALT_LENGTH),
            decodeBase64(parts[5], Argon2idDefaults.DEFAULT_HASH_LENGTH));
    validateSupportedParameters(parsedHash);
    return parsedHash;
  }

  private static byte[] decodeBase64(String value, int maxDecodedLength) {
    Preconditions.checkArgument(
        value.length() <= maxEncodedLength(maxDecodedLength), invalidHashFormatMessage());
    int remainder = value.length() % 4;
    Preconditions.checkArgument(remainder != 1, invalidHashFormatMessage());
    String paddedValue = remainder == 0 ? value : value + "====".substring(0, 4 - remainder);
    try {
      return Base64.getDecoder().decode(paddedValue);
    } catch (IllegalArgumentException e) {
      throw invalidHashFormat();
    }
  }

  private static int maxEncodedLength(int decodedLength) {
    int fullGroups = decodedLength / 3;
    int remainder = decodedLength % 3;
    return fullGroups * 4 + (remainder == 0 ? 0 : remainder + 1);
  }

  private static int parseInteger(String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw invalidHashFormat();
    }
  }

  private static void validateSupportedParameters(ParsedHash parsedHash) {
    Preconditions.checkArgument(
        parsedHash.version == Argon2idDefaults.DEFAULT_VERSION,
        "Unsupported Argon2id hash parameters");
    Preconditions.checkArgument(
        parsedHash.memoryKb == Argon2idDefaults.DEFAULT_MEMORY_KB,
        "Unsupported Argon2id hash parameters");
    Preconditions.checkArgument(
        parsedHash.iterations == Argon2idDefaults.DEFAULT_ITERATIONS,
        "Unsupported Argon2id hash parameters");
    Preconditions.checkArgument(
        parsedHash.parallelism == Argon2idDefaults.DEFAULT_PARALLELISM,
        "Unsupported Argon2id hash parameters");
    Preconditions.checkArgument(
        parsedHash.salt.length == Argon2idDefaults.DEFAULT_SALT_LENGTH, invalidHashFormatMessage());
    Preconditions.checkArgument(
        parsedHash.hash.length == Argon2idDefaults.DEFAULT_HASH_LENGTH, invalidHashFormatMessage());
  }

  private static IllegalArgumentException invalidHashFormat() {
    return new IllegalArgumentException(invalidHashFormatMessage());
  }

  private static String invalidHashFormatMessage() {
    return "Invalid Argon2id hash format";
  }

  private static class ParsedHash {
    private final int version;
    private final int memoryKb;
    private final int iterations;
    private final int parallelism;
    private final byte[] salt;
    private final byte[] hash;

    private ParsedHash(
        int version, int memoryKb, int iterations, int parallelism, byte[] salt, byte[] hash) {
      this.version = version;
      this.memoryKb = memoryKb;
      this.iterations = iterations;
      this.parallelism = parallelism;
      this.salt = salt;
      this.hash = hash;
    }
  }
}
