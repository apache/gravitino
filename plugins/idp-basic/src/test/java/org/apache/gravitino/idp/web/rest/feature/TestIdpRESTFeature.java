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

package org.apache.gravitino.idp.web.rest.feature;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.junit.jupiter.api.Test;

class TestIdpRESTFeature {

  @Test
  void testSimpleWithBasicFails() {
    Config config = new Config(false) {};
    config.set(
        Configs.AUTHENTICATORS,
        Lists.newArrayList(
            AuthenticatorType.SIMPLE.name().toLowerCase(),
            AuthenticatorType.BASIC.name().toLowerCase()));
    config.set(
        Configs.REST_API_EXTENSION_PACKAGES,
        Lists.newArrayList(IdpRESTFeature.IDP_REST_EXTENSION_PACKAGE));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> IdpRESTFeature.validateConfiguration(config));

    assertTrue(exception.getMessage().contains("simple"));
  }

  @Test
  void testBasicWithoutExtensionFails() {
    Config config = new Config(false) {};
    config.set(
        Configs.AUTHENTICATORS, Lists.newArrayList(AuthenticatorType.BASIC.name().toLowerCase()));

    assertThrows(
        IllegalArgumentException.class, () -> IdpRESTFeature.validateConfiguration(config));
  }

  @Test
  void testBasicWithExtensionOk() {
    Config config =
        newConfig(
            AuthenticatorType.BASIC.name().toLowerCase(),
            IdpRESTFeature.IDP_REST_EXTENSION_PACKAGE);

    assertDoesNotThrow(() -> IdpRESTFeature.validateConfiguration(config));
  }

  private static Config newConfig(String authenticator, String extensionPackage) {
    Config config = new Config(false) {};
    config.set(Configs.AUTHENTICATORS, Lists.newArrayList(authenticator));
    config.set(Configs.REST_API_EXTENSION_PACKAGES, Lists.newArrayList(extensionPackage));
    return config;
  }
}
