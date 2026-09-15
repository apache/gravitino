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

class TestGravitinoAuthSettings {

  @Test
  void testNoAuthWhenUnset() {
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(new OptimizerConfig(), key -> null);
    Assertions.assertFalse(settings.hasAuth());
    Assertions.assertTrue(settings.icebergRestCatalogConfigs("rest").isEmpty());
  }

  @Test
  void testBasicAuthFromConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put(OptimizerConfig.GRAVITINO_METALAKE, "ml");
    properties.put(OptimizerConfig.AUTH_TYPE, "basic");
    properties.put(OptimizerConfig.AUTH_USERNAME, "admin");
    properties.put(OptimizerConfig.AUTH_PASSWORD, "secret");
    GravitinoAuthSettings settings =
        GravitinoAuthSettings.from(new OptimizerConfig(properties), key -> null);

    Assertions.assertTrue(settings.hasAuth());
    Map<String, String> catalogConfigs = settings.icebergRestCatalogConfigs("iceberg_s3");
    Assertions.assertEquals(
        "basic", catalogConfigs.get("spark.sql.catalog.iceberg_s3.rest.auth.type"));
    Assertions.assertEquals(
        "admin", catalogConfigs.get("spark.sql.catalog.iceberg_s3.rest.auth.basic.username"));
    Assertions.assertEquals(
        "secret", catalogConfigs.get("spark.sql.catalog.iceberg_s3.rest.auth.basic.password"));

    GravitinoClient.ClientBuilder builder =
        GravitinoClient.builder("http://localhost:8090").withMetalake("ml");
    settings.applyTo(builder);
    Assertions.assertNotNull(builder);
  }

  @Test
  void testEnvFallbackWhenConfigMissing() {
    Map<String, String> env = new HashMap<>();
    env.put(GravitinoAuthSettings.ENV_AUTH_TYPE, "basic");
    env.put(GravitinoAuthSettings.ENV_AUTH_USERNAME, "env-user");
    env.put(GravitinoAuthSettings.ENV_AUTH_PASSWORD, "env-pass");
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(new OptimizerConfig(), env::get);

    Map<String, String> catalogConfigs = settings.icebergRestCatalogConfigs("rest");
    Assertions.assertEquals(
        "env-user", catalogConfigs.get("spark.sql.catalog.rest.rest.auth.basic.username"));
  }

  @Test
  void testOauthTokenCatalogConfigs() {
    Map<String, String> env = new HashMap<>();
    env.put(GravitinoAuthSettings.ENV_AUTH_TYPE, "oauth");
    env.put(GravitinoAuthSettings.ENV_AUTH_OAUTH_TOKEN, "tok");
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(null, env::get);

    Map<String, String> catalogConfigs = settings.icebergRestCatalogConfigs("rest");
    Assertions.assertEquals("oauth2", catalogConfigs.get("spark.sql.catalog.rest.rest.auth.type"));
    Assertions.assertEquals(
        "tok", catalogConfigs.get("spark.sql.catalog.rest.rest.auth.oauth2.token"));
  }

  @Test
  void testCopyAliases() {
    Map<String, String> properties = new HashMap<>();
    properties.put("auth_type", "basic");
    properties.put("username", "admin");
    properties.put("password", "secret");
    GravitinoAuthSettings.copyAliases(properties);
    Assertions.assertEquals("basic", properties.get(OptimizerConfig.AUTH_TYPE));
    Assertions.assertEquals("admin", properties.get(OptimizerConfig.AUTH_USERNAME));
    Assertions.assertEquals("secret", properties.get(OptimizerConfig.AUTH_PASSWORD));
  }

  @Test
  void testJobTemplateEnvironments() {
    Map<String, String> environments = GravitinoAuthSettings.jobTemplateEnvironments();
    Assertions.assertEquals(
        "{{gravitino_auth_type}}", environments.get(GravitinoAuthSettings.ENV_AUTH_TYPE));
    Assertions.assertEquals(
        "{{gravitino_auth_username}}", environments.get(GravitinoAuthSettings.ENV_AUTH_USERNAME));
    Assertions.assertEquals(
        "{{gravitino_auth_password}}", environments.get(GravitinoAuthSettings.ENV_AUTH_PASSWORD));
  }

  @Test
  void testOauthClientCredentialsJoinsTokenEndpoint() {
    Map<String, String> env = new HashMap<>();
    env.put(GravitinoAuthSettings.ENV_AUTH_TYPE, "oauth");
    env.put(GravitinoAuthSettings.ENV_AUTH_OAUTH_SERVER_URI, "http://idp/");
    env.put(GravitinoAuthSettings.ENV_AUTH_OAUTH_PATH, "/oauth2/token");
    env.put(GravitinoAuthSettings.ENV_AUTH_OAUTH_CREDENTIAL, "id:secret");
    env.put(GravitinoAuthSettings.ENV_AUTH_OAUTH_SCOPE, "catalog");
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(null, env::get);

    Map<String, String> catalogConfigs = settings.icebergRestCatalogConfigs("rest");
    Assertions.assertEquals("oauth2", catalogConfigs.get("spark.sql.catalog.rest.rest.auth.type"));
    Assertions.assertEquals(
        "http://idp/oauth2/token", catalogConfigs.get("spark.sql.catalog.rest.oauth2-server-uri"));
    Assertions.assertEquals("id:secret", catalogConfigs.get("spark.sql.catalog.rest.credential"));
    Assertions.assertEquals("catalog", catalogConfigs.get("spark.sql.catalog.rest.scope"));
  }

  @Test
  void testOauthClientCredentialsRequiresPath() {
    Map<String, String> env = new HashMap<>();
    env.put(GravitinoAuthSettings.ENV_AUTH_TYPE, "oauth");
    env.put(GravitinoAuthSettings.ENV_AUTH_OAUTH_SERVER_URI, "http://idp");
    env.put(GravitinoAuthSettings.ENV_AUTH_OAUTH_CREDENTIAL, "id:secret");
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(null, env::get);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> settings.icebergRestCatalogConfigs("rest"));
  }

  @Test
  void testSimpleAuthDefaultsUsernameForIcebergRest() {
    Map<String, String> env = new HashMap<>();
    env.put(GravitinoAuthSettings.ENV_AUTH_TYPE, "simple");
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(null, env::get);

    Map<String, String> catalogConfigs = settings.icebergRestCatalogConfigs("rest");
    Assertions.assertEquals("basic", catalogConfigs.get("spark.sql.catalog.rest.rest.auth.type"));
    Assertions.assertEquals(
        System.getProperty("user.name"),
        catalogConfigs.get("spark.sql.catalog.rest.rest.auth.basic.username"));
    Assertions.assertEquals(
        "dummy", catalogConfigs.get("spark.sql.catalog.rest.rest.auth.basic.password"));
  }

  @Test
  void testBasicAuthRequiresPassword() {
    Map<String, String> env = new HashMap<>();
    env.put(GravitinoAuthSettings.ENV_AUTH_TYPE, "basic");
    env.put(GravitinoAuthSettings.ENV_AUTH_USERNAME, "admin");
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(null, env::get);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> settings.icebergRestCatalogConfigs("rest"));
  }

  @Test
  void testCopyToJobConf() {
    Map<String, String> jobConf = new HashMap<>();
    Map<String, String> properties = new HashMap<>();
    properties.put(OptimizerConfig.AUTH_TYPE, "basic");
    properties.put(OptimizerConfig.AUTH_USERNAME, "admin");
    GravitinoAuthSettings.copyToJobConf(jobConf, new OptimizerConfig(properties));
    Assertions.assertEquals("basic", jobConf.get(GravitinoAuthSettings.JOB_CONF_AUTH_TYPE));
    Assertions.assertEquals("admin", jobConf.get(GravitinoAuthSettings.JOB_CONF_AUTH_USERNAME));
  }
}
