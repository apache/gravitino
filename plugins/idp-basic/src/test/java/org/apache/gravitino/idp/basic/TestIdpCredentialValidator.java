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

package org.apache.gravitino.idp.basic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpCredentialValidator {

  private static final String VALID_PASSWORD = "Passw0rd-1234";

  @Test
  public void testValidateUsername() {
    Assertions.assertDoesNotThrow(() -> IdpCredentialValidator.validateUsername("alice"));

    IllegalArgumentException blankException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> IdpCredentialValidator.validateUsername(" "));
    Assertions.assertEquals(
        "\"user\" field is required and cannot be empty", blankException.getMessage());

    IllegalArgumentException colonException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> IdpCredentialValidator.validateUsername("user:name"));
    Assertions.assertEquals("User name cannot contain a colon (:)", colonException.getMessage());
  }

  @Test
  public void testValidatePassword() {
    Assertions.assertDoesNotThrow(() -> IdpCredentialValidator.validatePassword(VALID_PASSWORD));
    Assertions.assertDoesNotThrow(
        () ->
            IdpCredentialValidator.validatePassword(
                "a".repeat(IdpCredentialValidator.MIN_PASSWORD_LENGTH)));
    Assertions.assertDoesNotThrow(
        () ->
            IdpCredentialValidator.validatePassword(
                "a".repeat(IdpCredentialValidator.MAX_PASSWORD_LENGTH)));
    Assertions.assertDoesNotThrow(
        () -> IdpCredentialValidator.validatePassword("pass:word:with:colons"));

    IllegalArgumentException blankException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> IdpCredentialValidator.validatePassword(" "));
    Assertions.assertEquals(
        "\"password\" field is required and cannot be empty", blankException.getMessage());

    IllegalArgumentException tooShortException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> IdpCredentialValidator.validatePassword("short"));
    Assertions.assertEquals(
        "Password must be at least 12 characters long and at most 64 characters long",
        tooShortException.getMessage());

    IllegalArgumentException tooLongException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> IdpCredentialValidator.validatePassword("a".repeat(65)));
    Assertions.assertEquals(
        "Password must be at least 12 characters long and at most 64 characters long",
        tooLongException.getMessage());
  }
}
