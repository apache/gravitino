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
    Assertions.assertEquals("v=" + Argon2Parameters.DEFAULT_VERSION, parts[2]);
    Assertions.assertEquals(
        "m="
            + Argon2Parameters.DEFAULT_MEMORY_KB
            + ",t="
            + Argon2Parameters.DEFAULT_ITERATIONS
            + ",p="
            + Argon2Parameters.DEFAULT_PARALLELISM,
        parts[3]);

    byte[] salt = decodeBase64(parts[4]);
    byte[] hash = decodeBase64(parts[5]);
    Assertions.assertEquals(Argon2Parameters.DEFAULT_SALT_LENGTH, salt.length);
    Assertions.assertEquals(Argon2Parameters.DEFAULT_HASH_LENGTH, hash.length);
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

  private static byte[] decodeBase64(String value) {
    int remainder = value.length() % 4;
    String paddedValue = remainder == 0 ? value : value + "====".substring(0, 4 - remainder);
    return Base64.getDecoder().decode(paddedValue);
  }
}
