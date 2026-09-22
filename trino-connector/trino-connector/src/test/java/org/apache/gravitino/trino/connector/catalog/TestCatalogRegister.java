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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.apache.gravitino.trino.connector.metadata.TestGravitinoCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestCatalogRegister {

  private static final String DISCOVERED_ICEBERG_REST_URI_PROPERTY = "__gravitino.iceberg.rest-uri";

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
  public void testHttpsDiscoveryUriWithTruststore() throws IOException {
    String truststore = createStoreFile().toString();
    Properties properties =
        CatalogRegister.buildJdbcProperties(
            config(
                Map.of(
                    "discovery.uri", "https://localhost:8443",
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
  public void testHttpsDiscoveryUriWithKeystore() throws IOException {
    String keystore = createStoreFile().toString();
    Properties properties =
        CatalogRegister.buildJdbcProperties(
            config(
                Map.of(
                    "discovery.uri", "https://localhost:8443",
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

  @Test
  public void testGenerateCreateCatalogCommandEmbedsDiscoveredUriForIcebergCatalog()
      throws Exception {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.uri", "http://127.0.0.1:8090", "gravitino.metalake", "test"));
    config.setDiscoveredIcebergRestUri("test", "http://irc-host:9001/iceberg");
    CatalogRegister catalogRegister = new CatalogRegister();
    catalogRegister.setConfigForTesting(config);
    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            "iceberg_catalog",
            "lakehouse-iceberg",
            "test catalog",
            Catalog.Type.RELATIONAL,
            Collections.emptyMap());

    String command =
        catalogRegister.generateCreateCatalogCommand(
            "iceberg_catalog", new GravitinoCatalog("test", mockCatalog));

    assertTrue(
        command.contains(
            "\"" + DISCOVERED_ICEBERG_REST_URI_PROPERTY + "\":\"http://irc-host:9001/iceberg\""));
  }

  @Test
  public void testGenerateCreateCatalogCommandDoesNotEmbedUriForNonIcebergCatalog()
      throws Exception {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.uri", "http://127.0.0.1:8090", "gravitino.metalake", "test"));
    config.setDiscoveredIcebergRestUri("test", "http://irc-host:9001/iceberg");
    CatalogRegister catalogRegister = new CatalogRegister();
    catalogRegister.setConfigForTesting(config);
    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            "hive_catalog",
            "hive",
            "test catalog",
            Catalog.Type.RELATIONAL,
            Collections.emptyMap());

    String command =
        catalogRegister.generateCreateCatalogCommand(
            "hive_catalog", new GravitinoCatalog("test", mockCatalog));

    assertFalse(command.contains(DISCOVERED_ICEBERG_REST_URI_PROPERTY));
  }

  @Test
  public void testRedactSecretsMasksSecretBearingProperties() {
    String command =
        "CREATE CATALOG c USING gravitino WITH ( "
            + "\"gravitino.iceberg.rest-catalog.oauth2.credential\"='client:secretvalue', "
            + "\"gravitino.iceberg.rest-catalog.uri\"='http://irc-host:9001/iceberg', "
            + "\"some.token\"='abc123', "
            + "\"trino.bypass.password\"='hunter2', "
            + "\"trino.bypass.passcode\"='letmein', "
            + "\"trino.bypass.passphrase\"='opensesame')";

    String redacted = CatalogRegister.redactSecrets(command);

    assertFalse(redacted.contains("secretvalue"));
    assertFalse(redacted.contains("abc123"));
    assertFalse(redacted.contains("hunter2"));
    assertFalse(redacted.contains("letmein"));
    assertFalse(redacted.contains("opensesame"));
    assertTrue(redacted.contains("\"gravitino.iceberg.rest-catalog.oauth2.credential\"='***'"));
    assertTrue(redacted.contains("\"some.token\"='***'"));
    assertTrue(redacted.contains("\"trino.bypass.password\"='***'"));
    assertTrue(redacted.contains("\"trino.bypass.passcode\"='***'"));
    assertTrue(redacted.contains("\"trino.bypass.passphrase\"='***'"));
    assertTrue(
        redacted.contains("\"gravitino.iceberg.rest-catalog.uri\"='http://irc-host:9001/iceberg'"));
  }

  @Test
  public void testRedactSecretsMasksJsonEmbeddedSecrets() {
    String command =
        "CREATE CATALOG c USING gravitino WITH ( "
            + "\"__gravitino.dynamic.connector.catalog.config\"="
            + "'{\"name\":\"hive_catalog\",\"properties\":"
            + "{\"jdbc-password\":\"hunter2\",\"s3-secret-key\":\"abc123\",\"jdbc-user\":\"admin\"}}')";

    String redacted = CatalogRegister.redactSecrets(command);

    assertFalse(redacted.contains("hunter2"));
    assertFalse(redacted.contains("abc123"));
    assertTrue(redacted.contains("\"jdbc-password\":\"***\""));
    assertTrue(redacted.contains("\"s3-secret-key\":\"***\""));
    // Non-secret properties must survive redaction unchanged.
    assertTrue(redacted.contains("\"jdbc-user\":\"admin\""));
  }

  @Test
  public void testRegisterCatalogPassesTheDriverFailureOnUnchanged() throws Exception {
    // The driver's exception travels to the caller with its type, message and stack intact:
    // redacting it here would have to rebuild every level of the cause chain as a different
    // exception type. Whatever a message carries is masked where it is rendered instead, which
    // for the load loop is CatalogConnectorManager.toErrorMessage().
    String secretValue = "hunter2";
    Statement statement = mock(Statement.class);
    when(statement.execute(eq("SHOW CATALOGS"))).thenReturn(true);
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.getResultSet()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);
    when(statement.execute(startsWith("CREATE CATALOG")))
        .thenThrow(
            new SQLException(
                "Query failed: \"gravitino.iceberg.rest-catalog.oauth2.credential\"='"
                    + secretValue
                    + "'"));

    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenReturn(statement);

    CatalogRegister catalogRegister = new CatalogRegister();
    catalogRegister.setConfigForTesting(
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.uri", "http://127.0.0.1:8090", "gravitino.metalake", "test")));
    setPrivateField(catalogRegister, "connection", connection);
    setPrivateField(catalogRegister, "catalogStoreDirectory", tempDir.toString());

    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            "hive_catalog", "hive", "test catalog", Catalog.Type.RELATIONAL, Map.of());
    GravitinoCatalog catalog = new GravitinoCatalog("test", mockCatalog);

    TrinoException e =
        assertThrows(
            TrinoException.class, () -> catalogRegister.registerCatalog("hive_catalog", catalog));

    // registerCatalog's generic wrapper -> executeSql's generic wrapper -> the driver exception.
    Throwable driverFailure = e.getCause().getCause();
    assertInstanceOf(SQLException.class, driverFailure);
    assertTrue(driverFailure.getMessage().contains(secretValue));
  }

  @Test
  public void testRegisterCatalogKeepsTheDeepCause() throws Exception {
    // The reason a registration failed usually sits at the bottom of the chain, e.g. a
    // connector's own configuration validation error, and that is what the status tables end up
    // reporting.
    Statement statement = mock(Statement.class);
    when(statement.execute(eq("SHOW CATALOGS"))).thenReturn(true);
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.getResultSet()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    RuntimeException deepCause =
        new RuntimeException("Configuration property 'unknown-direct-key' was not used");
    SQLException sqlException =
        new SQLException("Query failed: Failed to create connector: memory1", null, 0, deepCause);
    when(statement.execute(startsWith("CREATE CATALOG"))).thenThrow(sqlException);

    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenReturn(statement);

    CatalogRegister catalogRegister = new CatalogRegister();
    catalogRegister.setConfigForTesting(
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.uri", "http://127.0.0.1:8090", "gravitino.metalake", "test")));
    setPrivateField(catalogRegister, "connection", connection);
    setPrivateField(catalogRegister, "catalogStoreDirectory", tempDir.toString());

    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            "hive_catalog", "hive", "test catalog", Catalog.Type.RELATIONAL, Map.of());
    GravitinoCatalog catalog = new GravitinoCatalog("test", mockCatalog);

    TrinoException e =
        assertThrows(
            TrinoException.class, () -> catalogRegister.registerCatalog("hive_catalog", catalog));

    boolean deepCauseSurvived = false;
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (String.valueOf(t.getMessage()).contains("unknown-direct-key")) {
        deepCauseSurvived = true;
      }
    }
    assertTrue(deepCauseSurvived, "the deep cause must reach the caller");
  }

  @Test
  public void testDescribeMasksSecretsThroughTheWholeCauseChain() {
    // What a Jackson parse failure on the catalog JSON looks like: the source it echoes is the
    // very payload that carries the resolved secrets.
    RuntimeException deepCause =
        new RuntimeException(
            "Unexpected end-of-input at [Source: (String)\"{\"properties\":"
                + "{\"jdbc-password\":\"hunter2\"}}\"; line: 1]");
    Exception failure =
        new IllegalStateException("Failed on \"trino.bypass.password\"='letmein'", deepCause);

    String described = CatalogRegister.describe(failure);

    assertFalse(described.contains("hunter2"));
    assertFalse(described.contains("letmein"));
    assertTrue(described.contains("\"jdbc-password\":\"***\""));
    assertTrue(described.contains("\"trino.bypass.password\"='***'"));
    // The rendering keeps what makes it useful: the types, the chain and the frames.
    assertTrue(described.contains("IllegalStateException"));
    assertTrue(described.contains("Caused by"));
    assertTrue(described.contains("testDescribeMasksSecretsThroughTheWholeCauseChain"));
  }

  @Test
  public void testDescribeTerminatesOnACauseCycle() {
    // Java permits A -> B -> A. The JDK's own stack trace printer stops at the repeated node,
    // which is why rendering the throwable needs no cycle guard of its own.
    Exception first = new IllegalStateException("first");
    Exception second = new IllegalStateException("second", first);
    first.initCause(second);

    String described =
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> CatalogRegister.describe(first));

    assertTrue(described.contains("CIRCULAR REFERENCE"));
  }

  @Test
  public void testReachabilityIsProbedOnEveryCall() throws Exception {
    Statement statement = mock(Statement.class);
    when(statement.execute("SELECT 1"))
        .thenReturn(true)
        .thenThrow(new SQLException("Connection reset"));
    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenReturn(statement);

    CatalogRegister catalogRegister = new CatalogRegister();
    setPrivateField(catalogRegister, "connection", connection);

    // A reachability latched on the first success would keep the load loop issuing statements
    // over a connection that stopped working, and report every catalog failing separately
    // instead of the one reason they all did.
    assertTrue(catalogRegister.isTrinoReachable());
    assertNull(catalogRegister.getLastConnectionError());
    assertFalse(catalogRegister.isTrinoReachable());
    assertEquals("Connection reset", catalogRegister.getLastConnectionError());
  }

  private static void setPrivateField(Object target, String fieldName, Object value)
      throws Exception {
    Field field = CatalogRegister.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
