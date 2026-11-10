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

package org.apache.gravitino.flink.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Unit tests for Flink catalog store Basic authentication client configuration. */
public class TestGravitinoCatalogManagerBasicAuth {

  private static final String GRAVITINO_URI = "http://127.0.0.1:8090";
  private static final String METALAKE = "flink_basic_auth";

  @AfterEach
  void tearDown() {
    try {
      GravitinoCatalogManager.get().close();
    } catch (IllegalStateException ignore) {
      // GravitinoCatalogManager was not created in this test.
    }
  }

  @Test
  void testCreateWithBasicAuth() {
    Map<String, String> config = basicAuthConfig("alice", "secret");
    GravitinoAdminClient client =
        GravitinoAdminClient.builder(GRAVITINO_URI)
            .withBasicAuth(
                config.get(GravitinoCatalogStoreFactoryOptions.BASIC_USERNAME),
                config.get(GravitinoCatalogStoreFactoryOptions.BASIC_PASSWORD))
            .build();
    assertNotNull(client);
  }

  @Test
  void testCreateWithBasicAuthMissingUsername() {
    Map<String, String> config = basicAuthConfig("alice", "secret");
    config.remove(GravitinoCatalogStoreFactoryOptions.BASIC_USERNAME);
    assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoCatalogManager.create(GRAVITINO_URI, METALAKE, config));
  }

  @Test
  void testCreateWithBasicAuthMissingPassword() {
    Map<String, String> config = basicAuthConfig("alice", "secret");
    config.remove(GravitinoCatalogStoreFactoryOptions.BASIC_PASSWORD);
    assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoCatalogManager.create(GRAVITINO_URI, METALAKE, config));
  }

  @Test
  void testCreateWithBasicAuthBlankCredentials() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoCatalogManager.create(
                GRAVITINO_URI, METALAKE, basicAuthConfig(" ", "secret")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoCatalogManager.create(GRAVITINO_URI, METALAKE, basicAuthConfig("alice", " ")));
  }

  private static Map<String, String> basicAuthConfig(String username, String password) {
    return new HashMap<>(
        ImmutableMap.of(
            GravitinoCatalogStoreFactoryOptions.AUTH_TYPE,
            GravitinoCatalogStoreFactoryOptions.BASIC,
            GravitinoCatalogStoreFactoryOptions.BASIC_USERNAME,
            username,
            GravitinoCatalogStoreFactoryOptions.BASIC_PASSWORD,
            password));
  }
}
