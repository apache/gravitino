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

import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;
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
  public void testMissingConfig() {
    String gravitinoUrl = "http://127.0.0.1:8000";
    ImmutableMap<String, String> configMap = ImmutableMap.of("gravitino.uri", gravitinoUrl);
    try {
      GravitinoConfig config = new GravitinoConfig(configMap);
      assertEquals(gravitinoUrl, config.getURI());
    } catch (TrinoException e) {
      if (!GRAVITINO_MISSING_CONFIG.toErrorCode().equals(e.getErrorCode())) {
        throw e;
      }
    }
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

    assertTrue(config.isIcebergRestEnabled());
    assertEquals("", config.getIcebergRestUri());
    assertTrue(config.getIcebergRestCatalogConfig().isEmpty());
  }

  @Test
  public void testIcebergRestConfig() {
    ImmutableMap<String, String> configMap =
        ImmutableMap.of(
            "gravitino.metalake",
            "user_001",
            "gravitino.iceberg.rest-enabled",
            "false",
            "gravitino.iceberg.rest-uri",
            "http://127.0.0.1:9001/iceberg",
            "gravitino.iceberg.rest-catalog.security",
            "OAUTH2",
            "gravitino.iceberg.rest-catalog.oauth2.credential",
            "client_id:client_secret");
    GravitinoConfig config = new GravitinoConfig(configMap);

    assertFalse(config.isIcebergRestEnabled());
    assertEquals("http://127.0.0.1:9001/iceberg", config.getIcebergRestUri());

    Map<String, String> restCatalogConfig = config.getIcebergRestCatalogConfig();
    assertEquals(2, restCatalogConfig.size());
    assertEquals("OAUTH2", restCatalogConfig.get("iceberg.rest-catalog.security"));
    assertEquals(
        "client_id:client_secret", restCatalogConfig.get("iceberg.rest-catalog.oauth2.credential"));
  }

  @Test
  public void testToCatalogConfigWithIcebergRestProperties() {
    ImmutableMap<String, String> configMap =
        ImmutableMap.of(
            "gravitino.metalake",
            "user_001",
            "gravitino.iceberg.rest-uri",
            "http://127.0.0.1:9001/iceberg",
            "gravitino.iceberg.rest-catalog.security",
            "OAUTH2");
    GravitinoConfig config = new GravitinoConfig(configMap);

    String catalogConfig = config.toCatalogConfig();
    assertTrue(
        catalogConfig.contains("\"gravitino.iceberg.rest-uri\"='http://127.0.0.1:9001/iceberg'"));
    assertTrue(catalogConfig.contains("\"gravitino.iceberg.rest-catalog.security\"='OAUTH2'"));
  }

  @Test
  public void testToCatalogConfigPropagatesIcebergRestEnabled() {
    // The switch rides the exact-key loop rather than the prefix filter; if it fails to propagate,
    // the coordinator and the workers build different configs for the same catalog.
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake", "user_001", "gravitino.iceberg.rest-enabled", "false"));

    assertTrue(config.toCatalogConfig().contains("\"gravitino.iceberg.rest-enabled\"='false'"));
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
