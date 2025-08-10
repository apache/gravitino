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
package org.apache.gravitino.server.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Config;
import org.junit.jupiter.api.Test;

public class TestOAuthConfig {

  @Test
  public void testValidProviderTypes() {
    Config config = new Config(false) {};

    // Test valid provider types (case insensitive)
    Map<String, String> properties = new HashMap<>();

    properties.put("gravitino.authenticator.oauth.provider", "default");
    config.loadFromMap(properties, k -> true);
    assertEquals("default", config.get(OAuthConfig.PROVIDER));

    properties.clear();
    properties.put("gravitino.authenticator.oauth.provider", "OIDC");
    config.loadFromMap(properties, k -> true);
    assertEquals("OIDC", config.get(OAuthConfig.PROVIDER));
  }

  @Test
  public void testInvalidProviderType() {
    Config config = new Config(false) {};

    Map<String, String> properties = new HashMap<>();
    properties.put("gravitino.authenticator.oauth.provider", "invalid_provider");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              config.loadFromMap(properties, k -> true);
              config.get(OAuthConfig.PROVIDER); // This triggers validation
            });

    assertTrue(exception.getMessage().contains("Invalid OAuth provider type"));
    assertTrue(exception.getMessage().contains("default"));
    assertTrue(exception.getMessage().contains("oidc"));
  }

  @Test
  public void testDefaultProviderValue() {
    Config config = new Config(false) {};

    // No provider specified, should use default
    Map<String, String> properties = new HashMap<>();
    config.loadFromMap(properties, k -> true);
    assertEquals("default", config.get(OAuthConfig.PROVIDER));
  }
}
