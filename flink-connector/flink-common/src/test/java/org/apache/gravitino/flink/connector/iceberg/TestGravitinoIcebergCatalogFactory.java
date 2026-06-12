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

package org.apache.gravitino.flink.connector.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Test;

class TestGravitinoIcebergCatalogFactory {

  @Test
  void testToRestAuthPropertiesOAuth2() {
    Map<String, String> clientConfig =
        ImmutableMap.of(
            GravitinoCatalogStoreFactoryOptions.AUTH_TYPE,
                GravitinoCatalogStoreFactoryOptions.OAUTH2,
            GravitinoCatalogStoreFactoryOptions.OAUTH2_SERVER_URI, "http://oauth-server",
            GravitinoCatalogStoreFactoryOptions.OAUTH2_TOKEN_PATH, "/token",
            GravitinoCatalogStoreFactoryOptions.OAUTH2_CREDENTIAL, "client:secret",
            GravitinoCatalogStoreFactoryOptions.OAUTH2_SCOPE, "catalog");

    Map<String, String> authProperties =
        GravitinoIcebergCatalogFactory.toRestAuthProperties(clientConfig);

    assertEquals(AuthProperties.AUTH_TYPE_OAUTH2, authProperties.get(AuthProperties.AUTH_TYPE));
    assertEquals(
        "http://oauth-server/token", authProperties.get(OAuth2Properties.OAUTH2_SERVER_URI));
    assertEquals("client:secret", authProperties.get(OAuth2Properties.CREDENTIAL));
    assertEquals("catalog", authProperties.get(OAuth2Properties.SCOPE));
  }

  @Test
  void testToRestAuthPropertiesOAuth2WithoutTokenPath() {
    Map<String, String> clientConfig =
        ImmutableMap.of(
            GravitinoCatalogStoreFactoryOptions.AUTH_TYPE,
                GravitinoCatalogStoreFactoryOptions.OAUTH2,
            GravitinoCatalogStoreFactoryOptions.OAUTH2_SERVER_URI, "http://oauth-server/token",
            GravitinoCatalogStoreFactoryOptions.OAUTH2_CREDENTIAL, "client:secret");

    Map<String, String> authProperties =
        GravitinoIcebergCatalogFactory.toRestAuthProperties(clientConfig);

    assertEquals(
        "http://oauth-server/token", authProperties.get(OAuth2Properties.OAUTH2_SERVER_URI));
    assertFalse(authProperties.containsKey(OAuth2Properties.SCOPE));
  }

  @Test
  void testToRestAuthPropertiesBasic() {
    Map<String, String> clientConfig =
        ImmutableMap.of(
            GravitinoCatalogStoreFactoryOptions.AUTH_TYPE,
                GravitinoCatalogStoreFactoryOptions.BASIC,
            GravitinoCatalogStoreFactoryOptions.BASIC_USERNAME, "user",
            GravitinoCatalogStoreFactoryOptions.BASIC_PASSWORD, "password");

    Map<String, String> authProperties =
        GravitinoIcebergCatalogFactory.toRestAuthProperties(clientConfig);

    assertEquals(AuthProperties.AUTH_TYPE_BASIC, authProperties.get(AuthProperties.AUTH_TYPE));
    assertEquals("user", authProperties.get(AuthProperties.BASIC_USERNAME));
    assertEquals("password", authProperties.get(AuthProperties.BASIC_PASSWORD));
  }

  @Test
  void testToRestAuthPropertiesNoAuth() {
    assertTrue(
        GravitinoIcebergCatalogFactory.toRestAuthProperties(ImmutableMap.of()).isEmpty(),
        "No auth type configured should yield no REST auth properties");
  }

  @Test
  void testToRestAuthPropertiesNormalizesTokenEndpointSlashes() {
    Map<String, String> clientConfig =
        ImmutableMap.of(
            GravitinoCatalogStoreFactoryOptions.AUTH_TYPE,
                GravitinoCatalogStoreFactoryOptions.OAUTH2,
            GravitinoCatalogStoreFactoryOptions.OAUTH2_SERVER_URI, "http://oauth-server/",
            GravitinoCatalogStoreFactoryOptions.OAUTH2_TOKEN_PATH, "/token",
            GravitinoCatalogStoreFactoryOptions.OAUTH2_CREDENTIAL, "client:secret");

    Map<String, String> authProperties =
        GravitinoIcebergCatalogFactory.toRestAuthProperties(clientConfig);

    assertEquals(
        "http://oauth-server/token", authProperties.get(OAuth2Properties.OAUTH2_SERVER_URI));
  }

  @Test
  void testToRestAuthPropertiesJoinsTokenEndpointWithoutSlashes() {
    Map<String, String> clientConfig =
        ImmutableMap.of(
            GravitinoCatalogStoreFactoryOptions.AUTH_TYPE,
                GravitinoCatalogStoreFactoryOptions.OAUTH2,
            GravitinoCatalogStoreFactoryOptions.OAUTH2_SERVER_URI, "http://oauth-server",
            GravitinoCatalogStoreFactoryOptions.OAUTH2_TOKEN_PATH, "token",
            GravitinoCatalogStoreFactoryOptions.OAUTH2_CREDENTIAL, "client:secret");

    Map<String, String> authProperties =
        GravitinoIcebergCatalogFactory.toRestAuthProperties(clientConfig);

    assertEquals(
        "http://oauth-server/token", authProperties.get(OAuth2Properties.OAUTH2_SERVER_URI));
  }

  @Test
  void testInjectRestAuthInjectsForRestBackend() {
    Map<String, String> icebergCatalogOptions = new HashMap<>();
    Map<String, String> clientConfig =
        ImmutableMap.of(
            GravitinoCatalogStoreFactoryOptions.AUTH_TYPE,
                GravitinoCatalogStoreFactoryOptions.BASIC,
            GravitinoCatalogStoreFactoryOptions.BASIC_USERNAME, "user",
            GravitinoCatalogStoreFactoryOptions.BASIC_PASSWORD, "password");

    GravitinoIcebergCatalogFactory.injectRestAuth(
        icebergCatalogOptions,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
        clientConfig);

    assertEquals(
        AuthProperties.AUTH_TYPE_BASIC, icebergCatalogOptions.get(AuthProperties.AUTH_TYPE));
    assertEquals("user", icebergCatalogOptions.get(AuthProperties.BASIC_USERNAME));
  }

  @Test
  void testInjectRestAuthSkipsNonRestBackend() {
    Map<String, String> icebergCatalogOptions = new HashMap<>();
    Map<String, String> clientConfig =
        ImmutableMap.of(
            GravitinoCatalogStoreFactoryOptions.AUTH_TYPE,
                GravitinoCatalogStoreFactoryOptions.BASIC,
            GravitinoCatalogStoreFactoryOptions.BASIC_USERNAME, "user",
            GravitinoCatalogStoreFactoryOptions.BASIC_PASSWORD, "password");

    GravitinoIcebergCatalogFactory.injectRestAuth(
        icebergCatalogOptions,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE,
        clientConfig);

    assertTrue(
        icebergCatalogOptions.isEmpty(),
        "Auth must not leak into a non-REST backend's catalog options");
  }

  @Test
  void testInjectRestAuthDoesNotOverrideExplicitAuth() {
    Map<String, String> icebergCatalogOptions = new HashMap<>();
    icebergCatalogOptions.put(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_OAUTH2);
    Map<String, String> clientConfig =
        ImmutableMap.of(
            GravitinoCatalogStoreFactoryOptions.AUTH_TYPE,
                GravitinoCatalogStoreFactoryOptions.BASIC,
            GravitinoCatalogStoreFactoryOptions.BASIC_USERNAME, "user",
            GravitinoCatalogStoreFactoryOptions.BASIC_PASSWORD, "password");

    GravitinoIcebergCatalogFactory.injectRestAuth(
        icebergCatalogOptions,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
        clientConfig);

    assertEquals(
        AuthProperties.AUTH_TYPE_OAUTH2,
        icebergCatalogOptions.get(AuthProperties.AUTH_TYPE),
        "User-configured REST auth must be preserved");
    assertFalse(icebergCatalogOptions.containsKey(AuthProperties.BASIC_USERNAME));
  }
}
