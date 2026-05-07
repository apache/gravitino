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

import java.util.Base64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestArgon2idPasswordHasher {

  private final PasswordHasher passwordHasher = new Argon2idPasswordHasher();

  @Test
  public void testHashProducesArgon2idPhcString() {
    String hashedPassword = passwordHasher.hash("test-password");

    String[] parts = hashedPassword.split("\\$");

    Assertions.assertEquals(6, parts.length);
    Assertions.assertEquals("", parts[0]);
    Assertions.assertEquals("argon2id", parts[1]);
    Assertions.assertEquals("v=" + Argon2idDefaults.DEFAULT_VERSION, parts[2]);
    Assertions.assertEquals(
        "m="
            + Argon2idDefaults.DEFAULT_MEMORY_KB
            + ",t="
            + Argon2idDefaults.DEFAULT_ITERATIONS
            + ",p="
            + Argon2idDefaults.DEFAULT_PARALLELISM,
        parts[3]);

    byte[] salt = decodeBase64(parts[4]);
    byte[] hash = decodeBase64(parts[5]);
    Assertions.assertEquals(Argon2idDefaults.DEFAULT_SALT_LENGTH, salt.length);
    Assertions.assertEquals(Argon2idDefaults.DEFAULT_HASH_LENGTH, hash.length);
  }

  @Test
  public void testVerifyMatchesExpectedPassword() {
    String hashedPassword = passwordHasher.hash("test-password");

    Assertions.assertTrue(passwordHasher.verify("test-password", hashedPassword));
    Assertions.assertFalse(passwordHasher.verify("wrong-password", hashedPassword));
  }

  @Test
  public void testFactoryCreatesArgon2idHasher() {
    Assertions.assertTrue(PasswordHasherFactory.create() instanceof Argon2idPasswordHasher);
  }

  @Test
  public void testHashRejectsBlankPassword() {
    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> passwordHasher.hash(" "));

    Assertions.assertEquals("Plain password must not be blank", exception.getMessage());
  }

  @Test
  public void testVerifyRejectsBlankPlainPassword() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> passwordHasher.verify(" ", passwordHasher.hash("test-password")));

    Assertions.assertEquals("Plain password must not be blank", exception.getMessage());
  }

  @Test
  public void testVerifyRejectsBlankHashedPassword() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> passwordHasher.verify("test-password", " "));

    Assertions.assertEquals("Hashed password must not be blank", exception.getMessage());
  }

  @Test
  public void testVerifyRejectsMalformedPhcString() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> passwordHasher.verify("test-password", "$argon2id$v=19$m=65536,x=3,p=1$abc$abc"));

    Assertions.assertEquals("Invalid Argon2id hash format", exception.getMessage());
  }

  @Test
  public void testVerifyRejectsOversizedBase64PhcString() {
    String oversizedBase64 = "A".repeat(100);
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                passwordHasher.verify(
                    "test-password",
                    "$argon2id$v=19$m=65536,t=3,p=1$" + oversizedBase64 + "$" + oversizedBase64));

    Assertions.assertEquals("Invalid Argon2id hash format", exception.getMessage());
  }

  @Test
  public void testVerifyRejectsInvalidBase64PhcString() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> passwordHasher.verify("test-password", "$argon2id$v=19$m=65536,t=3,p=1$a$abc"));

    Assertions.assertEquals("Invalid Argon2id hash format", exception.getMessage());
  }

  @Test
  public void testVerifyRejectsUnexpectedArgon2CostParameters() {
    String hashedPassword = passwordHasher.hash("test-password");
    String unsupportedHash =
        hashedPassword.replace(
            "m="
                + Argon2idDefaults.DEFAULT_MEMORY_KB
                + ",t="
                + Argon2idDefaults.DEFAULT_ITERATIONS
                + ",p="
                + Argon2idDefaults.DEFAULT_PARALLELISM,
            "m=131072,t=3,p=1");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> passwordHasher.verify("test-password", unsupportedHash));

    Assertions.assertEquals("Unsupported Argon2id hash parameters", exception.getMessage());
  }

  private static byte[] decodeBase64(String value) {
    int remainder = value.length() % 4;
    String paddedValue = remainder == 0 ? value : value + "====".substring(0, 4 - remainder);
    return Base64.getDecoder().decode(paddedValue);
  }
}
