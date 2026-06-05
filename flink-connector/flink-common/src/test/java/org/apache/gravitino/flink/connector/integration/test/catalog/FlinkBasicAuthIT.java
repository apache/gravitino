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
package org.apache.gravitino.flink.connector.integration.test.catalog;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.gravitino.Configs;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end integration test for Flink Basic authentication against the built-in IdP. Exercises
 * the full path: Flink catalog store → HTTP Basic credentials → Gravitino REST → built-in IdP
 * validation.
 */
public class FlinkBasicAuthIT extends BaseIT {

  private static final String METALAKE = "flink_basic_auth";
  private static final String ADMIN = "admin";
  private static final String ADMIN_PASSWORD = "Passw0rd-For-Admin1";
  private static final String IDP_REST_EXTENSION_PACKAGE =
      "org.apache.gravitino.idp.web.rest.feature";

  private static String gravitinoUri;

  private static TableEnvironment tableEnv;

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

    gravitinoUri = String.format("http://127.0.0.1:%d", getGravitinoServerPort());
    try (GravitinoAdminClient adminClient =
        GravitinoAdminClient.builder(gravitinoUri).withBasicAuth(ADMIN, ADMIN_PASSWORD).build()) {
      adminClient.createMetalake(METALAKE, "Flink basic auth test metalake", Maps.newHashMap());
    }

    Configuration configuration = new Configuration();
    configuration.setString(
        "table.catalog-store.kind", GravitinoCatalogStoreFactoryOptions.GRAVITINO);
    configuration.setString("table.catalog-store.gravitino.gravitino.metalake", METALAKE);
    configuration.setString("table.catalog-store.gravitino.gravitino.uri", gravitinoUri);
    configuration.setString(
        "table.catalog-store.gravitino." + GravitinoCatalogStoreFactoryOptions.AUTH_TYPE,
        GravitinoCatalogStoreFactoryOptions.BASIC);
    configuration.setString(
        "table.catalog-store.gravitino." + GravitinoCatalogStoreFactoryOptions.BASIC_USERNAME,
        ADMIN);
    configuration.setString(
        "table.catalog-store.gravitino." + GravitinoCatalogStoreFactoryOptions.BASIC_PASSWORD,
        ADMIN_PASSWORD);

    EnvironmentSettings.Builder builder =
        EnvironmentSettings.newInstance().withConfiguration(configuration);
    tableEnv = TableEnvironment.create(builder.inBatchMode().build());
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (tableEnv != null) {
      try {
        TableEnvironmentImpl env = (TableEnvironmentImpl) tableEnv;
        env.getCatalogManager().close();
      } catch (Exception e) {
        throw new RuntimeException("Failed to close Flink table environment", e);
      }
    }
    super.stopIntegrationTest();
  }

  @Test
  public void testFlinkConnectsWithBasicAuth() {
    Assertions.assertDoesNotThrow(() -> tableEnv.executeSql("SHOW CATALOGS").print());

    try (GravitinoAdminClient basicClient =
        GravitinoAdminClient.builder(gravitinoUri).withBasicAuth(ADMIN, ADMIN_PASSWORD).build()) {
      GravitinoMetalake loadedMetalake = basicClient.loadMetalake(METALAKE);
      Assertions.assertEquals(METALAKE, loadedMetalake.name());
    }
  }
}
