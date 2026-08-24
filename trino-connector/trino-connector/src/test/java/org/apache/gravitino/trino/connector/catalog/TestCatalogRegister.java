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
package org.apache.gravitino.trino.connector.catalog;

import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT;
import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.TrinoException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestCatalogRegister {

  @TempDir private static Path tempDir;

  private static GravitinoConfig config(Map<String, String> extraConfig) {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("gravitino.metalake", "test");
    configMap.put("discovery.uri", "http://localhost:8080");
    configMap.putAll(extraConfig);
    return new GravitinoConfig(configMap);
  }

  private static Path createStoreFile() throws IOException {
    return Files.createTempFile(tempDir, "store", ".jks");
  }

  @Test
  public void testDefaultProperties() {
    Properties properties = CatalogRegister.buildJdbcProperties(config(Map.of()));

    assertEquals("admin", properties.get("user"));
    // An empty password must not be sent to the driver.
    assertNull(properties.get("password"));
    assertNull(properties.get("SSL"));
    assertNull(properties.get("roles"));
  }

  @Test
  public void testPasswordIsSetWhenNotEmpty() {
    Properties properties =
        CatalogRegister.buildJdbcProperties(
            config(Map.of("trino.jdbc.user", "gravitino", "trino.jdbc.password", "secret")));

    assertEquals("gravitino", properties.get("user"));
    assertEquals("secret", properties.get("password"));
  }

  @Test
  public void testSslEnabledWithTruststore() throws IOException {
    String truststore = createStoreFile().toString();
    Properties properties =
        CatalogRegister.buildJdbcProperties(
            config(
                Map.of(
                    "trino.jdbc.ssl.enabled", "true",
                    "trino.jdbc.ssl.truststore.path", truststore,
                    "trino.jdbc.ssl.truststore.password", "truststore-secret",
                    "trino.jdbc.ssl.truststore.type", "PKCS12")));

    assertEquals("true", properties.get("SSL"));
    assertEquals("FULL", properties.get("SSLVerification"));
    assertEquals(truststore, properties.get("SSLTrustStorePath"));
    assertEquals("truststore-secret", properties.get("SSLTrustStorePassword"));
    assertEquals("PKCS12", properties.get("SSLTrustStoreType"));
  }

  @Test
  public void testSslEnabledDerivedFromDiscoveryUri() {
    GravitinoConfig config = config(Map.of("discovery.uri", "https://localhost:8443"));
    assertTrue(config.isTrinoJdbcSslEnabled());

    Properties properties = CatalogRegister.buildJdbcProperties(config);
    assertEquals("true", properties.get("SSL"));
    assertEquals("FULL", properties.get("SSLVerification"));
    // Without a configured truststore the driver falls back to the default JVM truststore.
    assertNull(properties.get("SSLTrustStorePath"));
  }

  @Test
  public void testExplicitSslEnabledOverridesDiscoveryUri() {
    GravitinoConfig config =
        config(
            Map.of("discovery.uri", "https://localhost:8443", "trino.jdbc.ssl.enabled", "false"));
    assertFalse(config.isTrinoJdbcSslEnabled());
    assertNull(CatalogRegister.buildJdbcProperties(config).get("SSL"));
  }

  @Test
  public void testSslEnabledIgnoresSurroundingWhitespace() {
    GravitinoConfig config = config(Map.of("trino.jdbc.ssl.enabled", " true "));
    assertTrue(config.isTrinoJdbcSslEnabled());
    assertEquals("true", CatalogRegister.buildJdbcProperties(config).get("SSL"));
  }

  @Test
  public void testSslVerificationIsNormalized() {
    Properties properties =
        CatalogRegister.buildJdbcProperties(
            config(
                Map.of("trino.jdbc.ssl.enabled", "true", "trino.jdbc.ssl.verification", " none ")));

    assertEquals("NONE", properties.get("SSLVerification"));
  }

  @Test
  public void testRoles() {
    Properties properties =
        CatalogRegister.buildJdbcProperties(config(Map.of("trino.jdbc.roles", "system:sysadmin")));

    assertEquals("system:sysadmin", properties.get("roles"));
  }

  @Test
  public void testExtraPropertiesArePassedThroughAndOverride() {
    Properties properties =
        CatalogRegister.buildJdbcProperties(
            config(
                Map.of(
                    "trino.jdbc.ssl.enabled", "true",
                    "trino.jdbc.properties.KerberosRemoteServiceName", "trino",
                    "trino.jdbc.properties.SSLVerification", "CA")));

    assertEquals("trino", properties.get("KerberosRemoteServiceName"));
    // The escape hatch wins over the value derived from the dedicated configuration.
    assertEquals("CA", properties.get("SSLVerification"));
  }

  @Test
  public void testKeystoreIsPassedThrough() throws IOException {
    String keystore = createStoreFile().toString();
    Properties properties =
        CatalogRegister.buildJdbcProperties(
            config(
                Map.of(
                    "trino.jdbc.ssl.enabled", "true",
                    "trino.jdbc.ssl.keystore.path", keystore,
                    "trino.jdbc.ssl.keystore.password", "keystore-secret",
                    "trino.jdbc.ssl.keystore.type", "PKCS12")));

    assertEquals(keystore, properties.get("SSLKeyStorePath"));
    assertEquals("keystore-secret", properties.get("SSLKeyStorePassword"));
    assertEquals("PKCS12", properties.get("SSLKeyStoreType"));
  }

  @Test
  public void testKeystoreWithoutSslEnabled() throws IOException {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                CatalogRegister.buildJdbcProperties(
                    config(Map.of("trino.jdbc.ssl.keystore.path", createStoreFile().toString()))));

    assertEquals(GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("trino.jdbc.ssl.keystore.path"));
  }

  @Test
  public void testKeystorePasswordWithoutKeystorePath() {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                CatalogRegister.buildJdbcProperties(
                    config(
                        Map.of(
                            "trino.jdbc.ssl.enabled", "true",
                            "trino.jdbc.ssl.keystore.password", "keystore-secret"))));

    assertEquals(GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("trino.jdbc.ssl.keystore.path"));
  }

  @Test
  public void testKeystoreFileNotFound() {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                CatalogRegister.buildJdbcProperties(
                    config(
                        Map.of(
                            "trino.jdbc.ssl.enabled", "true",
                            "trino.jdbc.ssl.keystore.path", "/not/exists/keystore.p12"))));

    assertEquals(GRAVITINO_MISSING_CONFIG.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("does not exist"));
  }

  @Test
  public void testKeystoreWithVerificationNone() throws IOException {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                CatalogRegister.buildJdbcProperties(
                    config(
                        Map.of(
                            "trino.jdbc.ssl.enabled", "true",
                            "trino.jdbc.ssl.verification", "NONE",
                            "trino.jdbc.ssl.keystore.path", createStoreFile().toString()))));

    assertEquals(GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("NONE"));
    assertTrue(e.getMessage().contains("trino.jdbc.ssl.keystore.path"));
  }

  @Test
  public void testInvalidSslVerification() {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                CatalogRegister.buildJdbcProperties(
                    config(
                        Map.of(
                            "trino.jdbc.ssl.enabled", "true",
                            "trino.jdbc.ssl.verification", "PARTIAL"))));

    assertEquals(GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("trino.jdbc.ssl.verification"));
  }

  @Test
  public void testSslVerificationWithoutSslEnabled() {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                CatalogRegister.buildJdbcProperties(
                    config(Map.of("trino.jdbc.ssl.verification", "NONE"))));

    assertEquals(GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("trino.jdbc.ssl.enabled"));
  }

  @Test
  public void testTruststoreWithoutSslEnabled() throws IOException {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                CatalogRegister.buildJdbcProperties(
                    config(
                        Map.of("trino.jdbc.ssl.truststore.path", createStoreFile().toString()))));

    assertEquals(GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("trino.jdbc.ssl.truststore.path"));
  }

  @Test
  public void testTruststorePasswordWithoutSslEnabled() {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                CatalogRegister.buildJdbcProperties(
                    config(Map.of("trino.jdbc.ssl.truststore.password", "truststore-secret"))));

    assertEquals(GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("trino.jdbc.ssl.truststore.password"));
  }

  @Test
  public void testTruststoreTypeWithoutSslEnabled() {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                CatalogRegister.buildJdbcProperties(
                    config(Map.of("trino.jdbc.ssl.truststore.type", "PKCS12"))));

    assertEquals(GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("trino.jdbc.ssl.truststore.type"));
  }

  @Test
  public void testBlankSslVerificationFallsBackToDefault() {
    Properties properties =
        CatalogRegister.buildJdbcProperties(
            config(Map.of("trino.jdbc.ssl.enabled", "true", "trino.jdbc.ssl.verification", "")));

    assertEquals("FULL", properties.get("SSLVerification"));
  }

  @Test
  public void testTruststorePasswordWithoutTruststorePath() {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                CatalogRegister.buildJdbcProperties(
                    config(
                        Map.of(
                            "trino.jdbc.ssl.enabled", "true",
                            "trino.jdbc.ssl.truststore.password", "truststore-secret"))));

    assertEquals(GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("trino.jdbc.ssl.truststore.path"));
  }

  @Test
  public void testTruststoreTypeWithoutTruststorePath() {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                CatalogRegister.buildJdbcProperties(
                    config(
                        Map.of(
                            "trino.jdbc.ssl.enabled", "true",
                            "trino.jdbc.ssl.truststore.type", "PKCS12"))));

    assertEquals(GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("trino.jdbc.ssl.truststore.path"));
  }

  @Test
  public void testTruststoreWithVerificationNone() throws IOException {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                CatalogRegister.buildJdbcProperties(
                    config(
                        Map.of(
                            "trino.jdbc.ssl.enabled", "true",
                            "trino.jdbc.ssl.verification", "NONE",
                            "trino.jdbc.ssl.truststore.path", createStoreFile().toString()))));

    assertEquals(GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("NONE"));
  }

  @Test
  public void testTruststoreFileNotFound() {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                CatalogRegister.buildJdbcProperties(
                    config(
                        Map.of(
                            "trino.jdbc.ssl.enabled", "true",
                            "trino.jdbc.ssl.truststore.path", "/not/exists/truststore.jks"))));

    assertEquals(GRAVITINO_MISSING_CONFIG.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("does not exist"));
    assertTrue(e.getMessage().contains("trino.jdbc.ssl.truststore.path"));
  }
}
