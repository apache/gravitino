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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestArgon2idPasswordHasher {

  private final PasswordHasher passwordHasher = new Argon2idPasswordHasher();

  @Test
  public void testHashProducesArgon2idPhcString() {
    String hashedPassword = passwordHasher.hash("test-password");

    Assertions.assertTrue(hashedPassword.startsWith("$argon2id$"));
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
}
