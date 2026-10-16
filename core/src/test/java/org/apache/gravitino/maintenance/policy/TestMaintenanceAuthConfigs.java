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
package org.apache.gravitino.maintenance.policy;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestMaintenanceAuthConfigs {

  @Test
  void gravitinoAuthConfigKeysUseCamelCase() {
    Assertions.assertEquals(
        MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + MaintenanceAuthConfigs.AUTH_TYPE_SUFFIX,
        MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_TYPE.getKey());
    Assertions.assertEquals(
        MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + MaintenanceAuthConfigs.OAUTH_PATH_SUFFIX,
        MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_OAUTH_PATH.getKey());
    Assertions.assertEquals(
        "gravitino.maintenance.gravitinoAuth.authType",
        MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_TYPE.getKey());
    Assertions.assertEquals(
        "gravitino.maintenance.gravitinoAuth.oauthServerUri",
        MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_OAUTH_SERVER_URI.getKey());
  }

  @Test
  void optionalGravitinoAuthValuesLoadFromConfig() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(
            MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_TYPE.getKey(),
            "oauth",
            MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_OAUTH_PATH.getKey(),
            "oauth2/token"),
        key -> key.startsWith("gravitino."));

    Assertions.assertEquals(
        "oauth",
        config.get(MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_TYPE).get());
    Assertions.assertEquals(
        "oauth2/token",
        config.get(MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_OAUTH_PATH).get());
    Assertions.assertTrue(
        config.get(MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_USERNAME).isEmpty());
  }

  @Test
  void loadGravitinoAuthMapsCamelCaseToUpdaterShortNames() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "authType",
            "oauth",
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "username",
            "tms",
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "password",
            "secret",
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "oauthServerUri",
            "https://idp.example.com",
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "oauthPath",
            "oauth2/token",
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "oauthCredential",
            "id:secret",
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "oauthScope",
            "catalog"),
        key -> key.startsWith("gravitino."));

    Assertions.assertEquals(
        ImmutableMap.<String, String>builder()
            .put("auth_type", "oauth")
            .put("username", "tms")
            .put("password", "secret")
            .put("oauth_server_uri", "https://idp.example.com")
            .put("oauth_path", "oauth2/token")
            .put("oauth_credential", "id:secret")
            .put("oauth_scope", "catalog")
            .build(),
        MaintenanceAuthConfigs.loadGravitinoAuth(config));
  }

  @Test
  void loadIrcAuthPassesThroughArbitrarySparkKeys() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(
            MaintenanceAuthConfigs.IRC_AUTH_PREFIX + "rest.auth.type",
            "oauth2",
            MaintenanceAuthConfigs.IRC_AUTH_PREFIX + "rest.auth.oauth2.token",
            "tok",
            MaintenanceAuthConfigs.IRC_AUTH_PREFIX + "spark.executor.memory",
            "4g"),
        key -> key.startsWith("gravitino."));

    Assertions.assertEquals(
        ImmutableMap.of(
            "rest.auth.type",
            "oauth2",
            "rest.auth.oauth2.token",
            "tok",
            "spark.executor.memory",
            "4g"),
        MaintenanceAuthConfigs.loadIrcAuth(config));
  }

  @Test
  void overlayAuthWinsOnDuplicateKeys() {
    Map<String, String> policyJob = ImmutableMap.of("uri", "http://irc", "rest.auth.type", "basic");
    Map<String, String> policyUpdater = ImmutableMap.of("metalake", "ml", "auth_type", "none");
    Map<String, String> ircAuth =
        ImmutableMap.of("rest.auth.type", "oauth2", "rest.auth.oauth2.token", "tok");
    Map<String, String> gravitinoAuth = ImmutableMap.of("auth_type", "oauth");

    Map<String, String> job = MaintenanceAuthConfigs.overlayJobOptions(policyJob, ircAuth);
    Map<String, String> updater =
        MaintenanceAuthConfigs.overlayUpdaterOptions(policyUpdater, gravitinoAuth);

    Assertions.assertEquals("oauth2", job.get("rest.auth.type"));
    Assertions.assertEquals("tok", job.get("rest.auth.oauth2.token"));
    Assertions.assertEquals("http://irc", job.get("uri"));
    Assertions.assertEquals("oauth", updater.get("auth_type"));
    Assertions.assertEquals("ml", updater.get("metalake"));
  }

  @Test
  void emptyAuthLeavesPolicyOnly() {
    Assertions.assertEquals(
        ImmutableMap.of("a", "1"),
        MaintenanceAuthConfigs.overlayJobOptions(
            ImmutableMap.of("a", "1"), Collections.emptyMap()));
    Assertions.assertEquals(
        ImmutableMap.of("b", "2"),
        MaintenanceAuthConfigs.overlayUpdaterOptions(
            ImmutableMap.of("b", "2"), Collections.emptyMap()));
  }

  @Test
  void overlayFromConfigLoadsAndMerges() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(
            MaintenanceAuthConfigs.IRC_AUTH_PREFIX + "rest.auth.type",
            "basic",
            MaintenanceAuthConfigs.IRC_AUTH_PREFIX + "rest.auth.basic.username",
            "irc",
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "authType",
            "simple",
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "username",
            "tms"),
        key -> key.startsWith("gravitino."));

    Assertions.assertEquals(
        ImmutableMap.of(
            "uri", "http://policy", "rest.auth.type", "basic", "rest.auth.basic.username", "irc"),
        MaintenanceAuthConfigs.overlayJobOptions(config, ImmutableMap.of("uri", "http://policy")));
    Assertions.assertEquals(
        ImmutableMap.of("metalake", "ml", "auth_type", "simple", "username", "tms"),
        MaintenanceAuthConfigs.overlayUpdaterOptions(config, ImmutableMap.of("metalake", "ml")));
  }

  @Test
  void emptyAuthValueRejected() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(MaintenanceAuthConfigs.IRC_AUTH_PREFIX + "rest.auth.type", "   "),
        key -> key.startsWith("gravitino."));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> MaintenanceAuthConfigs.loadIrcAuth(config));
  }

  @Test
  void unknownGravitinoAuthSuffixRejected() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "authTyp",
            "basic",
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "username",
            "tms"),
        key -> key.startsWith("gravitino."));
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> MaintenanceAuthConfigs.loadGravitinoAuth(config));
    Assertions.assertTrue(ex.getMessage().contains("Unknown Gravitino auth configuration"));
    Assertions.assertTrue(ex.getMessage().contains("authTyp"));
  }

  @Test
  void blankGravitinoAuthSuffixRejected() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX, "none"),
        key -> key.startsWith("gravitino."));
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> MaintenanceAuthConfigs.loadGravitinoAuth(config));
    Assertions.assertTrue(ex.getMessage().contains("blank suffix"));
  }

  @Test
  void blankIrcAuthSuffixRejected() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(MaintenanceAuthConfigs.IRC_AUTH_PREFIX, "none"),
        key -> key.startsWith("gravitino."));
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> MaintenanceAuthConfigs.loadIrcAuth(config));
    Assertions.assertTrue(ex.getMessage().contains("blank suffix"));
  }

  @Test
  void missingAuthTypeRejectedWhenOtherKeysPresent() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "username", "tms"),
        key -> key.startsWith("gravitino."));
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> MaintenanceAuthConfigs.loadGravitinoAuth(config));
    Assertions.assertTrue(ex.getMessage().contains("authType is required"));
  }

  @Test
  void unsupportedAuthTypeRejected() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "authType", "kerberos"),
        key -> key.startsWith("gravitino."));
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> MaintenanceAuthConfigs.loadGravitinoAuth(config));
    Assertions.assertTrue(ex.getMessage().contains("Unsupported"));
    Assertions.assertTrue(ex.getMessage().contains("kerberos"));
  }

  @Test
  void basicAuthRequiresUsernameAndPassword() {
    Config missingPassword = new Config(false) {};
    missingPassword.loadFromMap(
        Map.of(
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "authType",
            "basic",
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "username",
            "tms"),
        key -> key.startsWith("gravitino."));
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> MaintenanceAuthConfigs.loadGravitinoAuth(missingPassword));
    Assertions.assertTrue(ex.getMessage().contains("password is required"));
  }

  @Test
  void oauthAuthRequiresTokenSettings() {
    Config missingScope = new Config(false) {};
    missingScope.loadFromMap(
        Map.of(
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "authType",
            "oauth",
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "oauthServerUri",
            "https://idp.example.com",
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "oauthPath",
            "oauth2/token",
            MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + "oauthCredential",
            "id:secret"),
        key -> key.startsWith("gravitino."));
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> MaintenanceAuthConfigs.loadGravitinoAuth(missingScope));
    Assertions.assertTrue(ex.getMessage().contains("oauthScope is required"));
  }
}
