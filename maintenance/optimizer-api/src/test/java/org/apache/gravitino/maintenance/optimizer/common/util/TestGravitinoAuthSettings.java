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

package org.apache.gravitino.maintenance.optimizer.common.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestGravitinoAuthSettings {

  @Test
  void fromReturnsNoneWhenUnset() {
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(new OptimizerConfig());
    Assertions.assertEquals(GravitinoAuthSettings.TYPE_NONE, settings.authType());
    Assertions.assertFalse(settings.hasAuth());
  }

  @Test
  void applyToUsesBasicAuth() {
    Map<String, String> properties = new HashMap<>();
    properties.put(OptimizerConfig.AUTH_TYPE, "basic");
    properties.put(OptimizerConfig.AUTH_USERNAME, "admin");
    properties.put(OptimizerConfig.AUTH_PASSWORD, "YourSecureGravitinoPassword");
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(new OptimizerConfig(properties));

    @SuppressWarnings("unchecked")
    GravitinoClient.ClientBuilder builder = Mockito.mock(GravitinoClient.ClientBuilder.class);
    Mockito.when(builder.withBasicAuth(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(builder);

    settings.applyTo(builder);
    Mockito.verify(builder).withBasicAuth("admin", "YourSecureGravitinoPassword");
  }

  @Test
  void applyToUsesSimpleAuth() {
    Map<String, String> properties = new HashMap<>();
    properties.put(OptimizerConfig.AUTH_TYPE, "simple");
    properties.put(OptimizerConfig.AUTH_USERNAME, "alice");
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(new OptimizerConfig(properties));

    @SuppressWarnings("unchecked")
    GravitinoClient.ClientBuilder builder = Mockito.mock(GravitinoClient.ClientBuilder.class);
    Mockito.when(builder.withSimpleAuth(Mockito.anyString())).thenReturn(builder);

    settings.applyTo(builder);
    Mockito.verify(builder).withSimpleAuth("alice");
  }

  @Test
  void oauthRequiresPath() {
    Map<String, String> properties = new HashMap<>();
    properties.put(OptimizerConfig.AUTH_TYPE, "oauth");
    properties.put(OptimizerConfig.AUTH_OAUTH_SERVER_URI, "http://idp");
    properties.put(OptimizerConfig.AUTH_OAUTH_CREDENTIAL, "id:secret");
    properties.put(OptimizerConfig.AUTH_OAUTH_SCOPE, "catalog");
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(new OptimizerConfig(properties));

    @SuppressWarnings("unchecked")
    GravitinoClient.ClientBuilder builder = Mockito.mock(GravitinoClient.ClientBuilder.class);
    Assertions.assertThrows(IllegalArgumentException.class, () -> settings.applyTo(builder));
  }

  @Test
  void oauthRequiresCredential() {
    Map<String, String> properties = new HashMap<>();
    properties.put(OptimizerConfig.AUTH_TYPE, "oauth");
    properties.put(OptimizerConfig.AUTH_OAUTH_SERVER_URI, "http://idp");
    properties.put(OptimizerConfig.AUTH_OAUTH_PATH, "oauth2/token");
    properties.put(OptimizerConfig.AUTH_OAUTH_SCOPE, "catalog");
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(new OptimizerConfig(properties));

    @SuppressWarnings("unchecked")
    GravitinoClient.ClientBuilder builder = Mockito.mock(GravitinoClient.ClientBuilder.class);
    Assertions.assertThrows(IllegalArgumentException.class, () -> settings.applyTo(builder));
  }

  @Test
  void basicAuthRequiresPassword() {
    Map<String, String> properties = new HashMap<>();
    properties.put(OptimizerConfig.AUTH_TYPE, "basic");
    properties.put(OptimizerConfig.AUTH_USERNAME, "admin");
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(new OptimizerConfig(properties));

    @SuppressWarnings("unchecked")
    GravitinoClient.ClientBuilder builder = Mockito.mock(GravitinoClient.ClientBuilder.class);
    Assertions.assertThrows(IllegalArgumentException.class, () -> settings.applyTo(builder));
  }

  @Test
  void copyAliasesPromotesShortNames() {
    Map<String, String> properties = new HashMap<>();
    properties.put("auth_type", "basic");
    properties.put("username", "admin");
    properties.put("password", "YourSecureGravitinoPassword");
    GravitinoAuthSettings.copyAliases(properties);
    Assertions.assertEquals("basic", properties.get(OptimizerConfig.AUTH_TYPE));
    Assertions.assertEquals("admin", properties.get(OptimizerConfig.AUTH_USERNAME));
    Assertions.assertEquals(
        "YourSecureGravitinoPassword", properties.get(OptimizerConfig.AUTH_PASSWORD));
  }
}
