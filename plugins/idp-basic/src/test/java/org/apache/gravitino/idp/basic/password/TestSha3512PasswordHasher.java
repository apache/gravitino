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

import java.util.Base64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSha3512PasswordHasher {

  private final PasswordHasher passwordHasher = new Sha3512PasswordHasher();

  @Test
  public void testHashFormat() {
    String hashedPassword = passwordHasher.hash("test-password");

    String[] parts = hashedPassword.split("\\$");

    Assertions.assertEquals(5, parts.length);
    Assertions.assertEquals("", parts[0]);
    Assertions.assertEquals("sha3-512", parts[1]);
    Assertions.assertEquals("i=" + Sha3512Defaults.DEFAULT_ITERATIONS, parts[2]);

    byte[] salt = decodeBase64(parts[3]);
    byte[] hash = decodeBase64(parts[4]);
    Assertions.assertEquals(Sha3512Defaults.DEFAULT_SALT_LENGTH, salt.length);
    Assertions.assertEquals(Sha3512Defaults.DEFAULT_HASH_LENGTH, hash.length);
  }

  @Test
  public void testVerifyMatch() {
    String hashedPassword = passwordHasher.hash("test-password");

    Assertions.assertTrue(passwordHasher.verify("test-password", hashedPassword));
    Assertions.assertFalse(passwordHasher.verify("wrong-password", hashedPassword));
  }

  @Test
  public void testFactoryCreatesHasher() {
    Assertions.assertTrue(PasswordHasherFactory.create() instanceof Sha3512PasswordHasher);
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
            () -> passwordHasher.verify("test-password", "$sha3-512$i=100000$abc"));

    Assertions.assertEquals("Invalid SHA3-512 hash format", exception.getMessage());
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
                    "$sha3-512$i=100000$" + oversizedBase64 + "$" + oversizedBase64));

    Assertions.assertEquals("Invalid SHA3-512 hash format", exception.getMessage());
  }

  @Test
  public void testVerifyRejectsInvalidBase64PhcString() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> passwordHasher.verify("test-password", "$sha3-512$i=100000$a$abc"));

    Assertions.assertEquals("Invalid SHA3-512 hash format", exception.getMessage());
  }

  @Test
  public void testVerifyRejectsUnexpectedIterations() {
    String hashedPassword = passwordHasher.hash("test-password");
    String unsupportedHash =
        hashedPassword.replace(
            "i=" + Sha3512Defaults.DEFAULT_ITERATIONS,
            "i=" + (Sha3512Defaults.DEFAULT_ITERATIONS + 1));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> passwordHasher.verify("test-password", unsupportedHash));

    Assertions.assertEquals("Unsupported SHA3-512 hash parameters", exception.getMessage());
  }

  private static byte[] decodeBase64(String value) {
    int remainder = value.length() % 4;
    String paddedValue = remainder == 0 ? value : value + "====".substring(0, 4 - remainder);
    return Base64.getDecoder().decode(paddedValue);
  }
}
