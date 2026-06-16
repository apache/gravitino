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

package org.apache.gravitino.auth;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.junit.jupiter.api.Test;

class TestIdpBasicAuthConfigValidator {

  @Test
  void testBasicWithExtensionOk() {
    Config config =
        newConfig(
            Lists.newArrayList(AuthenticatorType.BASIC.name().toLowerCase()),
            Lists.newArrayList(IdpBasicAuthConfigValidator.IDP_REST_EXTENSION_PACKAGE));

    assertDoesNotThrow(() -> IdpBasicAuthConfigValidator.validate(config));
  }

  @Test
  void testBasicWithOAuthAndExtensionOk() {
    Config config =
        newConfig(
            Lists.newArrayList(
                AuthenticatorType.BASIC.name().toLowerCase(),
                AuthenticatorType.OAUTH.name().toLowerCase()),
            Lists.newArrayList(IdpBasicAuthConfigValidator.IDP_REST_EXTENSION_PACKAGE));

    assertDoesNotThrow(() -> IdpBasicAuthConfigValidator.validate(config));
  }

  @Test
  void testBasicWithoutExtensionFails() {
    Config config =
        newConfig(
            Lists.newArrayList(AuthenticatorType.BASIC.name().toLowerCase()), Lists.newArrayList());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> IdpBasicAuthConfigValidator.validate(config));
    assertTrue(exception.getMessage().contains("extensionPackages"));
  }

  @Test
  void testExtensionWithoutBasicFails() {
    Config config =
        newConfig(
            Lists.newArrayList(AuthenticatorType.OAUTH.name().toLowerCase()),
            Lists.newArrayList(IdpBasicAuthConfigValidator.IDP_REST_EXTENSION_PACKAGE));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> IdpBasicAuthConfigValidator.validate(config));
    assertTrue(exception.getMessage().contains("gravitino.authenticators"));
  }

  @Test
  void testSimpleWithBasicFails() {
    Config config =
        newConfig(
            Lists.newArrayList(
                AuthenticatorType.SIMPLE.name().toLowerCase(),
                AuthenticatorType.BASIC.name().toLowerCase()),
            Lists.newArrayList(IdpBasicAuthConfigValidator.IDP_REST_EXTENSION_PACKAGE));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> IdpBasicAuthConfigValidator.validate(config));
    assertTrue(exception.getMessage().contains("simple"));
  }

  @Test
  void testDefaultSimpleWithoutExtensionOk() {
    Config config = new Config(false) {};

    assertDoesNotThrow(() -> IdpBasicAuthConfigValidator.validate(config));
  }

  private static Config newConfig(List<String> authenticators, List<String> extensionPackages) {
    Config config = new Config(false) {};
    config.set(Configs.AUTHENTICATORS, authenticators);
    config.set(Configs.REST_API_EXTENSION_PACKAGES, extensionPackages);
    return config;
  }
}
