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

public class TestPbkdf2Sha512PasswordHasher {

  private final PasswordHasher passwordHasher = new Pbkdf2Sha512PasswordHasher();

  @Test
  public void testHashFormat() {
    String hashedPassword = passwordHasher.hash("test-password");

    String[] parts = hashedPassword.split("\\$");

    Assertions.assertEquals(5, parts.length);
    Assertions.assertEquals("", parts[0]);
    Assertions.assertEquals("pbkdf2-sha512", parts[1]);
    Assertions.assertEquals("i=" + Pbkdf2Sha512Defaults.DEFAULT_ITERATIONS, parts[2]);

    byte[] salt = decodeBase64(parts[3]);
    byte[] hash = decodeBase64(parts[4]);
    Assertions.assertEquals(Pbkdf2Sha512Defaults.DEFAULT_SALT_LENGTH, salt.length);
    Assertions.assertEquals(Pbkdf2Sha512Defaults.DEFAULT_HASH_LENGTH, hash.length);
  }

  @Test
  public void testVerifyMatch() {
    String hashedPassword = passwordHasher.hash("test-password");

    Assertions.assertTrue(passwordHasher.verify("test-password", hashedPassword));
    Assertions.assertFalse(passwordHasher.verify("wrong-password", hashedPassword));
  }

  @Test
  public void testUniqueSalt() {
    String firstHash = passwordHasher.hash("test-password");
    String secondHash = passwordHasher.hash("test-password");

    Assertions.assertNotEquals(firstHash, secondHash);
    Assertions.assertTrue(passwordHasher.verify("test-password", firstHash));
    Assertions.assertTrue(passwordHasher.verify("test-password", secondHash));
  }

  @Test
  public void testFactory() {
    Assertions.assertTrue(PasswordHasherFactory.create() instanceof Pbkdf2Sha512PasswordHasher);
  }

  @Test
  public void testHashBlank() {
    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> passwordHasher.hash(" "));

    Assertions.assertEquals("Plain password must not be blank", exception.getMessage());
  }

  @Test
  public void testVerifyBlankPlain() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> passwordHasher.verify(" ", passwordHasher.hash("test-password")));

    Assertions.assertEquals("Plain password must not be blank", exception.getMessage());
  }

  @Test
  public void testVerifyBlankHash() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> passwordHasher.verify("test-password", " "));

    Assertions.assertEquals("Hashed password must not be blank", exception.getMessage());
  }

  @Test
  public void testVerifyMalformed() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> passwordHasher.verify("test-password", "$pbkdf2-sha512$i=210000$abc"));

    Assertions.assertEquals("Invalid PBKDF2 hash format", exception.getMessage());
  }

  @Test
  public void testVerifyOversizedB64() {
    String oversizedBase64 = "A".repeat(100);
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                passwordHasher.verify(
                    "test-password",
                    "$pbkdf2-sha512$i=210000$" + oversizedBase64 + "$" + oversizedBase64));

    Assertions.assertEquals("Invalid PBKDF2 hash format", exception.getMessage());
  }

  @Test
  public void testVerifyInvalidB64() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> passwordHasher.verify("test-password", "$pbkdf2-sha512$i=210000$a$abc"));

    Assertions.assertEquals("Invalid PBKDF2 hash format", exception.getMessage());
  }

  @Test
  public void testVerifyBadIterations() {
    String hashedPassword = passwordHasher.hash("test-password");
    String unsupportedHash =
        hashedPassword.replace("i=" + Pbkdf2Sha512Defaults.DEFAULT_ITERATIONS, "i=420000");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> passwordHasher.verify("test-password", unsupportedHash));

    Assertions.assertEquals("Unsupported PBKDF2 hash parameters", exception.getMessage());
  }

  private static byte[] decodeBase64(String value) {
    int remainder = value.length() % 4;
    String paddedValue = remainder == 0 ? value : value + "====".substring(0, 4 - remainder);
    return Base64.getDecoder().decode(paddedValue);
  }
}
