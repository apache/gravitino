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
package org.apache.gravitino.trino.connector.integration.test;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.Configs;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end integration test for Trino connector Basic authentication against the built-in IdP.
 * Verifies HTTP Basic credentials against Gravitino REST with the built-in IdP. Trino catalog
 * properties {@code gravitino.client.authType=basic}, {@code gravitino.client.basic.username}, and
 * {@code gravitino.client.basic.password} are mapped to {@link
 * GravitinoAdminClient.AdminClientBuilder#withBasicAuth(String, String)} by the connector;
 * connector-side config parsing is covered by {@code TestGravitinoAuthProvider#testBuildBasicAuth}.
 */
public class TrinoBasicAuthIT extends BaseIT {

  private static final String METALAKE = "trino_basic_auth";
  private static final String ADMIN = "admin";
  private static final String ADMIN_PASSWORD = "Passw0rd-For-Admin1";
  private static final String IDP_REST_EXTENSION_PACKAGE =
      "org.apache.gravitino.idp.web.rest.feature";

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    setEnv("GRAVITINO_INITIAL_ADMIN_PASSWORD", ADMIN_PASSWORD);
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(false));
    configs.put(Configs.CACHE_ENABLED.getKey(), String.valueOf(false));
    configs.put(Configs.SERVICE_ADMINS.getKey(), ADMIN);
    configs.put(Configs.REST_API_EXTENSION_PACKAGES.getKey(), IDP_REST_EXTENSION_PACKAGE);
    registerCustomConfigs(configs);
    super.startIntegrationTest();

    try (GravitinoAdminClient adminClient =
        GravitinoAdminClient.builder(serverUri).withBasicAuth(ADMIN, ADMIN_PASSWORD).build()) {
      adminClient.createMetalake(METALAKE, "Trino basic auth test metalake", Maps.newHashMap());
    }
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    super.stopIntegrationTest();
  }

  @Test
  public void testTrinoConnectorConnectsWithBasicAuth() {
    try (GravitinoAdminClient connectorClient =
        GravitinoAdminClient.builder(serverUri).withBasicAuth(ADMIN, ADMIN_PASSWORD).build()) {
      GravitinoMetalake metalake = connectorClient.loadMetalake(METALAKE);
      Assertions.assertEquals(METALAKE, metalake.name());
    }
  }
}
