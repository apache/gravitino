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

package org.apache.gravitino.idp.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.junit.jupiter.api.Test;

class TestIdpConfigurationValidator {

  @Test
  void testSimpleAndAuthFails() {
    Config config = newConfig(true, AuthenticatorType.SIMPLE.name().toLowerCase());

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> IdpConfigurationValidator.validate(config));

    assertTrue(exception.getMessage().contains("cannot be used with Simple authentication"));
  }

  @Test
  void testDefaultSimpleAndAuthFails() {
    Config config = new Config(false) {};
    config.set(Configs.ENABLE_AUTHORIZATION, true);

    assertThrows(IllegalStateException.class, () -> IdpConfigurationValidator.validate(config));
  }

  @Test
  void testSimpleNoAuthOk() {
    Config config = newConfig(false, AuthenticatorType.SIMPLE.name().toLowerCase());

    assertDoesNotThrow(() -> IdpConfigurationValidator.validate(config));
  }

  @Test
  void testOAuthAndAuthOk() {
    Config config = newConfig(true, AuthenticatorType.OAUTH.name().toLowerCase());

    assertDoesNotThrow(() -> IdpConfigurationValidator.validate(config));
  }

  private static Config newConfig(boolean authorizationEnabled, String... authenticators) {
    Config config = new Config(false) {};
    config.set(Configs.ENABLE_AUTHORIZATION, authorizationEnabled);
    config.set(Configs.AUTHENTICATORS, Lists.newArrayList(authenticators));
    return config;
  }
}
