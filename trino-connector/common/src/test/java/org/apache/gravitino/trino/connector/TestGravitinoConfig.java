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
package org.apache.gravitino.trino.connector;

import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

public class TestGravitinoConfig {

  @Test
  public void testGravitinoConfig() {
    String gravitinoUrl = "http://127.0.0.1:8000";
    String metalake = "user_001";
    ImmutableMap<String, String> configMap =
        ImmutableMap.of("gravitino.uri", gravitinoUrl, "gravitino.metalake", metalake);

    GravitinoConfig config = new GravitinoConfig(configMap);

    assertEquals(gravitinoUrl, config.getURI());
    assertEquals(metalake, config.getMetalake());
  }

  @Test
  public void testMetalakeIsTrimmed() {
    GravitinoConfig config =
        new GravitinoConfig(ImmutableMap.of("gravitino.metalake", "  user_001  "));

    // The system tables filter by metalake name, so a stray space in the catalog properties file
    // must not make this value stop matching the name the load loop records states under.
    assertEquals("user_001", config.getMetalake());
  }

  @Test
  public void testMetalakeIsOptional() {
    String gravitinoUrl = "http://127.0.0.1:8000";
    GravitinoConfig config = new GravitinoConfig(ImmutableMap.of("gravitino.uri", gravitinoUrl));

    assertEquals(gravitinoUrl, config.getURI());
    assertFalse(config.hasMetalake());
    assertEquals("", config.getMetalake());
    // A blank value is the same as an unset one.
    assertFalse(new GravitinoConfig(ImmutableMap.of("gravitino.metalake", "  ")).hasMetalake());
    assertTrue(new GravitinoConfig(ImmutableMap.of("gravitino.metalake", "test")).hasMetalake());
  }

  @Test
  public void testLoadAllMetalakes() {
    // Without a metalake every metalake is loaded, whatever the naming mode says.
    assertTrue(new GravitinoConfig(ImmutableMap.of()).loadAllMetalakes());
    assertTrue(
        new GravitinoConfig(ImmutableMap.of("gravitino.catalog-name-with-metalake", "true"))
            .loadAllMetalakes());
    // With a metalake only the qualified naming mode loads every metalake.
    assertFalse(
        new GravitinoConfig(ImmutableMap.of("gravitino.metalake", "test")).loadAllMetalakes());
    assertTrue(
        new GravitinoConfig(
                ImmutableMap.of(
                    "gravitino.metalake", "test", "gravitino.catalog-name-with-metalake", "true"))
            .loadAllMetalakes());
  }

  @Test
  public void testCatalogNameWithMetalake() {
    assertFalse(new GravitinoConfig(ImmutableMap.of()).catalogNameWithMetalake());
    assertTrue(
        new GravitinoConfig(ImmutableMap.of("gravitino.catalog-name-with-metalake", "true"))
            .catalogNameWithMetalake());
    // The deprecated key still works when the new one is unset ...
    assertTrue(
        new GravitinoConfig(ImmutableMap.of("gravitino.use-single-metalake", "false"))
            .catalogNameWithMetalake());
    assertFalse(
        new GravitinoConfig(ImmutableMap.of("gravitino.use-single-metalake", "true"))
            .catalogNameWithMetalake());
    // ... and is ignored once the new one is set.
    assertFalse(
        new GravitinoConfig(
                ImmutableMap.of(
                    "gravitino.use-single-metalake", "false",
                    "gravitino.catalog-name-with-metalake", "false"))
            .catalogNameWithMetalake());

    assertTrue(
        new GravitinoConfig(
                ImmutableMap.of(
                    "gravitino.use-single-metalake", "true",
                    "gravitino.catalog-name-with-metalake", "true"))
            .catalogNameWithMetalake());

    TrinoException error =
        assertThrows(
            TrinoException.class,
            () ->
                new GravitinoConfig(ImmutableMap.of("gravitino.catalog-name-with-metalake", "yes"))
                    .catalogNameWithMetalake());
    assertEquals(GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), error.getErrorCode());
    // The deprecated key is parsed just as strictly: a typo must not flip the naming mode.
    error =
        assertThrows(
            TrinoException.class,
            () ->
                new GravitinoConfig(ImmutableMap.of("gravitino.use-single-metalake", "yes"))
                    .catalogNameWithMetalake());
    assertEquals(GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), error.getErrorCode());
  }

  @Test
  public void testUsesDeprecatedSingleMetalakeKey() {
    assertFalse(new GravitinoConfig(ImmutableMap.of()).usesDeprecatedSingleMetalakeKey());
    assertTrue(
        new GravitinoConfig(ImmutableMap.of("gravitino.use-single-metalake", "true"))
            .usesDeprecatedSingleMetalakeKey());
    assertFalse(
        new GravitinoConfig(
                ImmutableMap.of(
                    "gravitino.use-single-metalake", "false",
                    "gravitino.catalog-name-with-metalake", "true"))
            .usesDeprecatedSingleMetalakeKey());
  }

  @Test
  public void testGravitinoConfigWithSkipTrinoVersionValidation() {
    String gravitinoUrl = "http://127.0.0.1:8000";
    String metalake = "user_001";
    ImmutableMap<String, String> configMap =
        ImmutableMap.of("gravitino.uri", gravitinoUrl, "gravitino.metalake", metalake);
    GravitinoConfig config = new GravitinoConfig(configMap);

    assertEquals(config.isSkipTrinoVersionValidation(), false);

    ImmutableMap<String, String> configMapWithSkipValidation =
        ImmutableMap.of(
            "gravitino.uri",
            gravitinoUrl,
            "gravitino.metalake",
            metalake,
            "gravitino.trino.skip-version-validation",
            "true");
    GravitinoConfig configWithSkipValidation = new GravitinoConfig(configMapWithSkipValidation);

    assertEquals(configWithSkipValidation.isSkipTrinoVersionValidation(), true);
  }

  @Test
  public void testGravitinoConfigWithClientConfig() {
    String gravitinoUrl = "http://127.0.0.1:8000";
    String metalake = "user_001";
    ImmutableMap<String, String> configMap =
        ImmutableMap.of("gravitino.uri", gravitinoUrl, "gravitino.metalake", metalake);
    GravitinoConfig config = new GravitinoConfig(configMap);

    assertTrue(config.getClientConfig().isEmpty());

    ImmutableMap<String, String> configMapWithClientConfig =
        ImmutableMap.of(
            "gravitino.uri",
            gravitinoUrl,
            "gravitino.metalake",
            metalake,
            "gravitino.client.socketTimeoutMs",
            "10000",
            "gravitino.client.connectionTimeoutMs",
            "20000");
    GravitinoConfig configWithClientConfig = new GravitinoConfig(configMapWithClientConfig);
    Map<String, String> clientConfig = configWithClientConfig.getClientConfig();
    assertEquals(clientConfig.get("gravitino.client.socketTimeoutMs"), "10000");
    assertEquals(clientConfig.get("gravitino.client.connectionTimeoutMs"), "20000");
  }

  @Test
  public void testGravitinoConfigWithSkipCatalogPatterns() {
    String gravitinoUrl = "http://127.0.0.1:8000";
    String metalake = "user_001";
    ImmutableMap<String, String> configMap =
        ImmutableMap.of("gravitino.uri", gravitinoUrl, "gravitino.metalake", metalake);
    GravitinoConfig config = new GravitinoConfig(configMap);

    assertFalse(skipCatalog("test_catalog", config));

    ImmutableMap<String, String> configMapWithSkipCatalogList =
        ImmutableMap.of(
            "gravitino.uri",
            gravitinoUrl,
            "gravitino.metalake",
            metalake,
            "gravitino.trino.skip-catalog-patterns",
            "test_.*, test1\\.c.*");
    GravitinoConfig configWithSkipCatalogPatterns =
        new GravitinoConfig(configMapWithSkipCatalogList);
    assertTrue(skipCatalog("test_catalog", configWithSkipCatalogPatterns));
    assertTrue(skipCatalog("test1.catalog", configWithSkipCatalogPatterns));
    assertFalse(skipCatalog("test1_catalog", configWithSkipCatalogPatterns));
    assertFalse(skipCatalog("test2_catalog", configWithSkipCatalogPatterns));

    ImmutableMap<String, String> configMapWithInvalidSkipCatalogList =
        ImmutableMap.of(
            "gravitino.uri",
            gravitinoUrl,
            "gravitino.metalake",
            metalake,
            "gravitino.trino.skip-catalog-patterns",
            "test_.*, (abc");
    assertThrowsExactly(
        TrinoException.class,
        () -> new GravitinoConfig(configMapWithInvalidSkipCatalogList),
        "Config `gravitino.trino.skip-catalog-patterns` is invalid because it contains an illegal regular expression");
  }

  @Test
  public void testToCatalogConfigWithAuthProperties() {
    String gravitinoUrl = "http://127.0.0.1:8000";
    String metalake = "user_001";
    ImmutableMap<String, String> configMap =
        ImmutableMap.of(
            "gravitino.uri",
            gravitinoUrl,
            "gravitino.metalake",
            metalake,
            "gravitino.client.authType",
            "simple",
            "gravitino.user",
            "admin");
    GravitinoConfig config = new GravitinoConfig(configMap);

    String catalogConfig = config.toCatalogConfig();
    assertTrue(catalogConfig.contains("\"gravitino.client.authType\"='simple'"));
    assertTrue(catalogConfig.contains("\"gravitino.user\"='admin'"));
  }

  @Test
  public void testToCatalogConfigIncludesScopedIcebergRestUris() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.iceberg.rest-uri",
                "http://default-irc:9001/iceberg",
                "gravitino.iceberg.rest-uri.prod",
                "http://prod-irc:9001/iceberg",
                "gravitino.iceberg.rest-uri.dev",
                "http://dev-irc:9001/iceberg"));

    String catalogConfig = config.toCatalogConfig();
    assertTrue(
        catalogConfig.contains("\"gravitino.iceberg.rest-uri\"='http://default-irc:9001/iceberg'"));
    assertTrue(
        catalogConfig.contains(
            "\"gravitino.iceberg.rest-uri.prod\"='http://prod-irc:9001/iceberg'"));
    assertTrue(
        catalogConfig.contains("\"gravitino.iceberg.rest-uri.dev\"='http://dev-irc:9001/iceberg'"));
  }

  @Test
  public void testTrinoJdbcConfigDefaults() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of("gravitino.metalake", "user_001", "discovery.uri", "http://host:8080"));

    assertEquals("admin", config.getTrinoUser());
    assertEquals("", config.getTrinoPassword());
    assertFalse(config.isTrinoJdbcSslEnabled());
    assertEquals("FULL", config.getTrinoJdbcSslVerification());
    assertEquals("", config.getTrinoJdbcSslTruststorePath());
    assertEquals("", config.getTrinoJdbcSslTruststorePassword());
    assertEquals("", config.getTrinoJdbcSslTruststoreType());
    assertEquals("", config.getTrinoJdbcRoles());
    assertTrue(config.getTrinoJdbcExtraProperties().isEmpty());
  }

  @Test
  public void testTrinoJdbcSslEnabledDerivedFromDiscoveryUri() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake", "user_001", "discovery.uri", "https://host:8443"));

    assertTrue(config.isTrinoJdbcSslEnabled());
    assertEquals("jdbc:trino://host:8443", config.getTrinoJdbcURI());
  }

  @Test
  public void testTrinoJdbcExtraProperties() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake",
                "user_001",
                "discovery.uri",
                "http://host:8080",
                "trino.jdbc.properties.KerberosRemoteServiceName",
                "trino",
                "trino.jdbc.properties.SSLKeyStorePath",
                "/etc/trino/client.p12"));

    Map<String, String> extraProperties = config.getTrinoJdbcExtraProperties();
    assertEquals(2, extraProperties.size());
    assertEquals("trino", extraProperties.get("KerberosRemoteServiceName"));
    assertEquals("/etc/trino/client.p12", extraProperties.get("SSLKeyStorePath"));
  }

  @Test
  public void testToCatalogConfigExcludesTrinoJdbcProperties() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake",
                "user_001",
                "trino.jdbc.user",
                "admin",
                "trino.jdbc.password",
                "jdbc-secret",
                "trino.jdbc.ssl.truststore.password",
                "truststore-secret",
                "trino.jdbc.properties.SSLKeyStorePassword",
                "keystore-secret"));

    // The internal JDBC connection settings are coordinator only. They must never reach the
    // generated CREATE CATALOG statement, which is logged and persisted to the catalog files.
    String catalogConfig = config.toCatalogConfig();
    assertFalse(catalogConfig.contains("trino.jdbc."));
    assertFalse(catalogConfig.contains("secret"));
    assertTrue(catalogConfig.contains("\"gravitino.metalake\"='user_001'"));
  }

  @Test
  public void testTrinoJdbcUriUsesSchemeDefaultPort() {
    GravitinoConfig httpsConfig =
        new GravitinoConfig(
            ImmutableMap.of("gravitino.metalake", "user_001", "discovery.uri", "https://host"));
    assertEquals("jdbc:trino://host:443", httpsConfig.getTrinoJdbcURI());

    GravitinoConfig httpConfig =
        new GravitinoConfig(
            ImmutableMap.of("gravitino.metalake", "user_001", "discovery.uri", "http://host"));
    assertEquals("jdbc:trino://host:80", httpConfig.getTrinoJdbcURI());
  }

  @Test
  public void testBlankSslVerificationFallsBackToDefault() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake",
                "user_001",
                "discovery.uri",
                "http://host:8080",
                "trino.jdbc.ssl.verification",
                "  "));

    assertEquals("FULL", config.getTrinoJdbcSslVerification());
  }

  @Test
  public void testInvalidSslEnabledIsRejected() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake",
                "user_001",
                // An HTTPS discovery.uri would have derived true, so a typo must not silently
                // fall back to false.
                "discovery.uri",
                "https://host:8443",
                "trino.jdbc.ssl.enabled",
                "yes"));

    TrinoException e = assertThrows(TrinoException.class, config::isTrinoJdbcSslEnabled);
    assertTrue(e.getMessage().contains("trino.jdbc.ssl.enabled"));
    assertTrue(e.getMessage().contains("expected true or false"));
  }

  @Test
  public void testSslEnabledAcceptsMixedCase() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake", "user_001",
                "discovery.uri", "http://host:8080",
                "trino.jdbc.ssl.enabled", " TRUE "));

    assertTrue(config.isTrinoJdbcSslEnabled());
  }

  @Test
  public void testIcebergRestConfigDefaults() {
    GravitinoConfig config = new GravitinoConfig(ImmutableMap.of("gravitino.metalake", "user_001"));

    // Nothing configured, and nothing discovered yet.
    assertEquals("", config.getManualIcebergRestUri("user_001"));
    assertEquals("", config.getDiscoveredIcebergRestUri("user_001"));
    assertTrue(config.isIcebergRestRoutingEnabled());
    assertTrue(config.getIcebergRestCatalogConfig().isEmpty());
  }

  @Test
  public void testIcebergRestRoutingCanBeDisabled() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake", "user_001",
                "gravitino.iceberg.rest-routing-enabled", "false"));

    assertFalse(config.isIcebergRestRoutingEnabled());
    assertTrue(
        config.toCatalogConfig().contains("\"gravitino.iceberg.rest-routing-enabled\"='false'"));
  }

  @Test
  public void testIcebergRestRoutingRejectsInvalidBoolean() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake", "user_001",
                "gravitino.iceberg.rest-routing-enabled", "yes"));

    assertThrows(TrinoException.class, config::isIcebergRestRoutingEnabled);
  }

  @Test
  public void testIcebergRestConfig() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake", "user_001",
                "gravitino.iceberg.rest-uri", "http://127.0.0.1:9001/iceberg",
                "gravitino.iceberg.rest-catalog.security", "OAUTH2",
                "gravitino.iceberg.rest-catalog.oauth2.credential", "client_id:client_secret"));

    // The unscoped URI is the default for every metalake.
    assertEquals("http://127.0.0.1:9001/iceberg", config.getManualIcebergRestUri("user_001"));
    assertEquals("http://127.0.0.1:9001/iceberg", config.getManualIcebergRestUri("user_002"));

    Map<String, String> restCatalogConfig = config.getIcebergRestCatalogConfig();
    assertEquals(2, restCatalogConfig.size());
    assertEquals("OAUTH2", restCatalogConfig.get("iceberg.rest-catalog.security"));
    assertEquals(
        "client_id:client_secret", restCatalogConfig.get("iceberg.rest-catalog.oauth2.credential"));
  }

  @Test
  public void testIcebergRestConfigRejectsBasicAuthWithoutExplicitSecurity() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake", "user_001",
                "gravitino.client.authType", "basic",
                "gravitino.client.basic.username", "admin",
                "gravitino.client.basic.password", "admin-pass"));

    TrinoException e = assertThrows(TrinoException.class, config::getIcebergRestCatalogConfig);
    assertTrue(e.getMessage().contains("gravitino.client.authType=basic"));
    assertTrue(e.getMessage().contains("gravitino.iceberg.rest-catalog.security"));
  }

  @Test
  public void testIcebergRestConfigMapsSimpleAuthToNone() {
    GravitinoConfig simpleConfig =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake", "user_001",
                "gravitino.client.authType", "simple"));
    assertEquals(
        "NONE", simpleConfig.getIcebergRestCatalogConfig().get("iceberg.rest-catalog.security"));
  }

  @Test
  public void testIcebergRestConfigRejectsKerberosAuthWithoutExplicitSecurity() {
    GravitinoConfig kerberosConfig =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake", "user_001",
                "gravitino.client.authType", "KERBEROS"));
    assertThrows(TrinoException.class, kerberosConfig::getIcebergRestCatalogConfig);
  }

  @Test
  public void testIcebergRestConfigAllowsBasicAuthWithExplicitSecurityOverride() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake", "user_001",
                "gravitino.client.authType", "basic",
                "gravitino.iceberg.rest-catalog.security", "NONE"));

    Map<String, String> restCatalogConfig = config.getIcebergRestCatalogConfig();
    assertEquals("NONE", restCatalogConfig.get("iceberg.rest-catalog.security"));
  }

  @Test
  public void testIcebergRestOAuthDefaultsToGravitinoClientOAuth() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.<String, String>builder()
                .put("gravitino.metalake", "user_001")
                .put("gravitino.client.authType", "oauth2")
                .put("gravitino.client.oauth2.serverUri", "https://oauth.example.com/")
                .put("gravitino.client.oauth2.path", "/realms/gravitino/token")
                .put("gravitino.client.oauth2.credential", "shared-client:shared-secret")
                .put("gravitino.client.oauth2.scope", "openid")
                .build());

    Map<String, String> restCatalogConfig = config.getIcebergRestCatalogConfig();
    assertEquals("OAUTH2", restCatalogConfig.get("iceberg.rest-catalog.security"));
    assertEquals(
        "shared-client:shared-secret",
        restCatalogConfig.get("iceberg.rest-catalog.oauth2.credential"));
    assertEquals("openid", restCatalogConfig.get("iceberg.rest-catalog.oauth2.scope"));
    assertEquals(
        "https://oauth.example.com/realms/gravitino/token",
        restCatalogConfig.get("iceberg.rest-catalog.oauth2.server-uri"));
  }

  /** Verifies non-OAuth2 REST modes do not inherit Gravitino service credentials. */
  @Test
  public void testIcebergRestPassthroughDoesNotInheritServiceCredentials() {
    for (String security : new String[] {"OAUTH2_PASSTHROUGH", "NONE"}) {
      GravitinoConfig config =
          new GravitinoConfig(
              ImmutableMap.<String, String>builder()
                  .put("gravitino.metalake", "test")
                  .put("gravitino.client.authType", "oauth2")
                  .put("gravitino.client.oauth2.serverUri", "https://idp.example.com")
                  .put("gravitino.client.oauth2.path", "token")
                  .put("gravitino.client.oauth2.credential", "service:secret")
                  .put("gravitino.client.oauth2.scope", "openid")
                  .put("gravitino.iceberg.rest-catalog.security", security)
                  .put("gravitino.iceberg.rest-catalog.session", "NONE")
                  .build());
      assertEquals(
          ImmutableMap.of(
              "iceberg.rest-catalog.security", security, "iceberg.rest-catalog.session", "NONE"),
          config.getIcebergRestCatalogConfig());
      assertEquals(
          "service:secret", config.getClientConfig().get("gravitino.client.oauth2.credential"));
    }
  }

  @Test
  public void testIcebergRestOAuthOverridesGravitinoClientOAuthByField() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.<String, String>builder()
                .put("gravitino.metalake", "user_001")
                .put("gravitino.client.authType", "oauth2")
                .put("gravitino.client.oauth2.serverUri", "https://oauth.example.com")
                .put("gravitino.client.oauth2.path", "realms/gravitino/token")
                .put("gravitino.client.oauth2.credential", "shared-client:shared-secret")
                .put("gravitino.client.oauth2.scope", "openid")
                .put("gravitino.iceberg.rest-catalog.oauth2.credential", "irc-client:irc-secret")
                .put("gravitino.iceberg.rest-catalog.oauth2.scope", "irc-scope")
                .build());

    Map<String, String> restCatalogConfig = config.getIcebergRestCatalogConfig();
    assertEquals(
        "irc-client:irc-secret", restCatalogConfig.get("iceberg.rest-catalog.oauth2.credential"));
    assertEquals("irc-scope", restCatalogConfig.get("iceberg.rest-catalog.oauth2.scope"));
    assertEquals(
        "https://oauth.example.com/realms/gravitino/token",
        restCatalogConfig.get("iceberg.rest-catalog.oauth2.server-uri"));
  }

  @Test
  public void testScopedIcebergRestUriOverridesUnscopedPerMetalake() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.catalog-name-with-metalake",
                "true",
                "gravitino.iceberg.rest-uri",
                "http://unscoped:9001/iceberg",
                "gravitino.iceberg.rest-uri.metalake_a",
                "http://metalake-a:9001/iceberg"));

    // The scoped key wins for the metalake it names, the others fall back to the unscoped one,
    // whether or not gravitino.metalake is configured.
    assertEquals("http://metalake-a:9001/iceberg", config.getManualIcebergRestUri("metalake_a"));
    assertEquals("http://unscoped:9001/iceberg", config.getManualIcebergRestUri("metalake_b"));
    assertEquals(
        "",
        new GravitinoConfig(ImmutableMap.of("gravitino.metalake", "metalake_a"))
            .getManualIcebergRestUri("metalake_a"));
  }

  @Test
  public void testDiscoveredIcebergRestUriIsPerMetalake() {
    GravitinoConfig config = new GravitinoConfig(ImmutableMap.of("gravitino.metalake", "user_001"));

    config.setDiscoveredIcebergRestUri("metalake_a", "http://irc-a:9001/iceberg");
    config.setDiscoveredIcebergRestUri("metalake_b", "http://irc-b:9001/iceberg");

    assertEquals("http://irc-a:9001/iceberg", config.getDiscoveredIcebergRestUri("metalake_a"));
    assertEquals("http://irc-b:9001/iceberg", config.getDiscoveredIcebergRestUri("metalake_b"));
    assertEquals("", config.getDiscoveredIcebergRestUri("metalake_c"));
    config.setDiscoveredIcebergRestUri("metalake_a", null);
    assertEquals("", config.getDiscoveredIcebergRestUri("metalake_a"));
  }

  @Test
  public void testToCatalogConfigWithIcebergRestProperties() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake", "user_001",
                "gravitino.iceberg.rest-uri", "http://127.0.0.1:9001/iceberg",
                "gravitino.iceberg.rest-catalog.security", "OAUTH2"));

    String catalogConfig = config.toCatalogConfig();
    assertTrue(
        catalogConfig.contains("\"gravitino.iceberg.rest-uri\"='http://127.0.0.1:9001/iceberg'"));
    assertTrue(catalogConfig.contains("\"gravitino.iceberg.rest-catalog.security\"='OAUTH2'"));
  }

  @Test
  public void testToCatalogConfigPropagatesSecretReferencesInsteadOfSecretValues() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake", "user_001",
                "gravitino.client.oauth2.credential", "client:management-secret",
                "gravitino.iceberg.rest-catalog.oauth2.credential", "client:irc-secret",
                "gravitino.dynamic-catalog.environment-variable.gravitino.client.oauth2.credential",
                    "GRAVITINO_CLIENT_CREDENTIAL",
                "gravitino.dynamic-catalog.environment-variable.gravitino.iceberg.rest-catalog.oauth2.credential",
                    "IRC_CLIENT_CREDENTIAL"));

    String catalogConfig = config.toCatalogConfig();

    assertFalse(catalogConfig.contains("management-secret"));
    assertFalse(catalogConfig.contains("irc-secret"));
    assertTrue(
        catalogConfig.contains(
            "\"gravitino.client.oauth2.credential\"='${ENV:GRAVITINO_CLIENT_CREDENTIAL}'"));
    assertTrue(
        catalogConfig.contains(
            "\"gravitino.iceberg.rest-catalog.oauth2.credential\"='${ENV:IRC_CLIENT_CREDENTIAL}'"));
  }

  private static boolean skipCatalog(String catalogName, GravitinoConfig config) {
    for (Pattern pattern : config.getSkipCatalogPatterns()) {
      if (pattern.matcher(catalogName).matches()) {
        return true;
      }
    }
    return false;
  }
}
