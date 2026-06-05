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
package org.apache.gravitino.spark.connector.integration.test.authorization;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end integration test for Spark Basic authentication against the built-in IdP. Exercises
 * the full path: Spark plugin → HTTP Basic credentials → Gravitino REST → built-in IdP validation.
 */
public class SparkBasicAuthIT extends BaseIT {

  private static final String METALAKE = "basic_auth_test_metalake";
  private static final String ADMIN = "admin";
  private static final String ADMIN_PASSWORD = "Passw0rd-For-Admin1";
  private static final String IDP_REST_EXTENSION_PACKAGE =
      "org.apache.gravitino.idp.web.rest.feature";

  private static SparkSession sparkSession;

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
      adminClient.createMetalake(METALAKE, "Basic auth test metalake", Maps.newHashMap());
    }

    SparkConf sparkConf =
        new SparkConf()
            .set("spark.plugins", GravitinoSparkPlugin.class.getName())
            .set(GravitinoSparkConfig.GRAVITINO_URI, serverUri)
            .set(GravitinoSparkConfig.GRAVITINO_METALAKE, METALAKE)
            .set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.BASIC_AUTH_TYPE)
            .set(GravitinoSparkConfig.GRAVITINO_BASIC_USERNAME, ADMIN)
            .set(GravitinoSparkConfig.GRAVITINO_BASIC_PASSWORD, ADMIN_PASSWORD);

    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("SparkBasicAuthIT")
            .config(sparkConf)
            .getOrCreate();
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (sparkSession != null) {
      sparkSession.close();
    }
    super.stopIntegrationTest();
  }

  @Test
  public void testSparkConnectsWithBasicAuth() throws Exception {
    Assertions.assertDoesNotThrow(() -> sparkSession.sql("SHOW CATALOGS").collect());

    try (GravitinoAdminClient basicClient =
        GravitinoAdminClient.builder(serverUri).withBasicAuth(ADMIN, ADMIN_PASSWORD).build()) {
      GravitinoMetalake metalake = basicClient.loadMetalake(METALAKE);
      Assertions.assertEquals(METALAKE, metalake.name());
    }
  }
}
