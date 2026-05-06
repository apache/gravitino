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
import org.bouncycastle.util.Arrays;

/** Argon2id-based password hasher. */
public class Argon2idPasswordHasher implements PasswordHasher {

  private static final String PHC_PREFIX = "$argon2id$";
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  @Override
  public String hash(String plainPassword) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(plainPassword), "Plain password must not be blank");

    byte[] salt = new byte[Argon2Parameters.DEFAULT_SALT_LENGTH];
    SECURE_RANDOM.nextBytes(salt);
    byte[] passwordBytes = plainPassword.getBytes(StandardCharsets.UTF_8);
    byte[] hash = new byte[Argon2Parameters.DEFAULT_HASH_LENGTH];
    try {
      generateHash(
          passwordBytes,
          salt,
          Argon2Parameters.DEFAULT_ITERATIONS,
          Argon2Parameters.DEFAULT_MEMORY_KB,
          Argon2Parameters.DEFAULT_PARALLELISM,
          Argon2Parameters.DEFAULT_VERSION,
          hash);
      return toPhcString(
          salt,
          hash,
          Argon2Parameters.DEFAULT_ITERATIONS,
          Argon2Parameters.DEFAULT_MEMORY_KB,
          Argon2Parameters.DEFAULT_PARALLELISM,
          Argon2Parameters.DEFAULT_VERSION);
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
    org.bouncycastle.crypto.params.Argon2Parameters parameters =
        new org.bouncycastle.crypto.params.Argon2Parameters.Builder(Argon2Parameters.DEFAULT_TYPE)
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
    Preconditions.checkArgument(
        hashedPassword.startsWith(PHC_PREFIX), "Invalid Argon2id hash format");
    String[] parts = hashedPassword.split("\\$");
    Preconditions.checkArgument(parts.length == 6, "Invalid Argon2id hash format");
    Preconditions.checkArgument("argon2id".equals(parts[1]), "Invalid Argon2id hash format");
    Preconditions.checkArgument(parts[2].startsWith("v="), "Invalid Argon2id hash format");
    Preconditions.checkArgument(parts[3].startsWith("m="), "Invalid Argon2id hash format");

    String[] parameterParts = parts[3].split(",");
    Preconditions.checkArgument(parameterParts.length == 3, "Invalid Argon2id hash format");
    return new ParsedHash(
        Integer.parseInt(parts[2].substring(2)),
        Integer.parseInt(parameterParts[0].substring(2)),
        Integer.parseInt(parameterParts[1].substring(2)),
        Integer.parseInt(parameterParts[2].substring(2)),
        decodeBase64(parts[4]),
        decodeBase64(parts[5]));
  }

  private static byte[] decodeBase64(String value) {
    int remainder = value.length() % 4;
    String paddedValue = remainder == 0 ? value : value + "====".substring(0, 4 - remainder);
    return Base64.getDecoder().decode(paddedValue);
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
