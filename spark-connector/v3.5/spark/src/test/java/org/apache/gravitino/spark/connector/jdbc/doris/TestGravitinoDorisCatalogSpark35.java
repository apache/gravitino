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

package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.secret.SupportsSecrets;
import org.apache.gravitino.spark.connector.authorization.AuthorizationTable;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

/** Unit tests for the governed Doris catalog boundary. */
public class TestGravitinoDorisCatalogSpark35 {

  @Test
  void testMissingVendedJdbcCredentialFailsClosed() {
    Catalog catalog = mock(Catalog.class);
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(catalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(new Credential[0]);

    assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoDorisCatalogSpark35.requiredJdbcCredential(catalog));
  }

  @Test
  void testMissingDriverClassRemainsDiagnosableWithoutConnectionDetails() {
    String driverClass = "org.example.MissingDorisJdbcDriver";
    String urlPassword = "test-url-secret";
    String jdbcUrl = "jdbc:mysql://fe.example.invalid:9030/db?password=" + urlPassword;
    String jdbcPassword = "test-vended-secret";
    GravitinoClient client = mock(GravitinoClient.class);

    GravitinoCatalogManager manager =
        GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> client);
    try {
      ClassNotFoundException failure =
          assertThrows(
              ClassNotFoundException.class,
              () ->
                  initializeCatalog(
                      client,
                      mock(TableCatalog.class),
                      Map.of("jdbc-url", jdbcUrl, "jdbc-driver", driverClass),
                      Map.of()));
      assertTrue(failure.getMessage().contains(driverClass));
      StringWriter stackTrace = new StringWriter();
      failure.printStackTrace(new PrintWriter(stackTrace));
      String diagnostics = stackTrace.toString();
      assertFalse(diagnostics.contains(jdbcUrl));
      assertFalse(diagnostics.contains(urlPassword));
      assertFalse(diagnostics.contains(jdbcPassword));
    } finally {
      manager.close();
    }
  }

  @Test
  void testFirstConnectionPreservesDriverPropertiesAndRedactsFailure() throws Exception {
    String jdbcUrl = "jdbc:doris-validator-test://host/db?password=test-url-secret";
    Driver driver = mock(Driver.class);
    when(driver.connect(eq(jdbcUrl), any(Properties.class))).thenReturn(null);
    TableCatalog tables = mock(TableCatalog.class);
    NameIdentifier table = NameIdentifier.of("db", "table");
    Set<Privilege.Name> privileges = Set.of(Privilege.Name.SELECT_TABLE);
    when(tables.loadTable(table, privileges)).thenReturn(mock(Table.class));
    GravitinoClient client = mock(GravitinoClient.class);
    GravitinoCatalogManager manager =
        GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> client);
    DorisTestDriver.setDelegate(driver);
    try {
      GravitinoDorisCatalogSpark35 catalog =
          initializeCatalog(
              client,
              tables,
              Map.of(
                  "jdbc-url",
                  jdbcUrl,
                  "jdbc-driver",
                  DorisTestDriver.class.getName(),
                  "spark.bypass.connectTimeout",
                  "10000",
                  "spark.bypass.sslMode",
                  "PREFERRED"),
              Map.of("connectTimeout", "2000", "sslMode", "VERIFY_IDENTITY", "fetchSize", "100"));
      IllegalArgumentException failure =
          assertThrows(
              IllegalArgumentException.class,
              () -> catalog.loadTable(Identifier.of(new String[] {"db"}, "table")));
      assertSafeLoadFailure(failure, jdbcUrl);

      ArgumentCaptor<Properties> properties = ArgumentCaptor.forClass(Properties.class);
      InOrder order = inOrder(tables, driver);
      order.verify(tables).loadTable(table, privileges);
      order.verify(driver).connect(eq(jdbcUrl), properties.capture());
      Properties actual = properties.getValue();
      assertEquals("2000", actual.getProperty("connectTimeout"));
      assertEquals("VERIFY_IDENTITY", actual.getProperty("sslMode"));
      assertEquals("vended-user", actual.getProperty("user"));
      assertEquals("test-vended-secret", actual.getProperty("password"));
      for (String key :
          List.of("connecttimeout", "sslmode", "fetchsize", "fetchSize", "driver", "dbtable")) {
        assertFalse(actual.containsKey(key), key);
      }
    } finally {
      DorisTestDriver.clearDelegate();
      manager.close();
    }
  }

  @Test
  void testPhysicalLoadOmitsDriverCauseAndSuppressedDetails() throws Exception {
    String jdbcUrl = "jdbc:doris-validator-test://host/db?password=test-url-secret";
    Driver driver = mock(Driver.class);
    RuntimeException driverFailure =
        new IllegalStateException("connection " + jdbcUrl, new SQLException("test-vended-secret"));
    driverFailure.addSuppressed(new IllegalArgumentException("test-url-secret"));
    when(driver.connect(eq(jdbcUrl), any(Properties.class))).thenThrow(driverFailure);
    TableCatalog tables = mock(TableCatalog.class);
    when(tables.loadTable(NameIdentifier.of("db", "table"), Set.of(Privilege.Name.SELECT_TABLE)))
        .thenReturn(mock(Table.class));
    GravitinoClient client = mock(GravitinoClient.class);
    GravitinoCatalogManager manager =
        GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> client);
    DorisTestDriver.setDelegate(driver);
    try {
      GravitinoDorisCatalogSpark35 catalog =
          initializeCatalog(
              client,
              tables,
              Map.of("jdbc-url", jdbcUrl, "jdbc-driver", DorisTestDriver.class.getName()),
              Map.of());
      assertSafeLoadFailure(
          assertThrows(
              IllegalArgumentException.class,
              () -> catalog.loadTable(Identifier.of(new String[] {"db"}, "table"))),
          jdbcUrl);
    } finally {
      DorisTestDriver.clearDelegate();
      manager.close();
    }
  }

  @Test
  void testAuthorizationDenialPreventsFirstPhysicalConnection() throws Exception {
    Driver driver = mock(Driver.class);
    TableCatalog tables = mock(TableCatalog.class);
    when(tables.loadTable(NameIdentifier.of("db", "table"), Set.of(Privilege.Name.SELECT_TABLE)))
        .thenThrow(new ForbiddenException("denied"));
    GravitinoClient client = mock(GravitinoClient.class);
    GravitinoCatalogManager manager =
        GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> client);
    DorisTestDriver.setDelegate(driver);
    try {
      GravitinoDorisCatalogSpark35 catalog =
          initializeCatalog(
              client,
              tables,
              Map.of(
                  "jdbc-url",
                  "jdbc:doris-validator-test://host/db",
                  "jdbc-driver",
                  DorisTestDriver.class.getName()),
              Map.of());
      assertTrue(
          catalog.loadTable(Identifier.of(new String[] {"db"}, "table"))
              instanceof AuthorizationTable);
      verifyNoInteractions(driver);
    } finally {
      AuthorizationTable.clear();
      DorisTestDriver.clearDelegate();
      manager.close();
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  void testWritesAndCatalogDdlAreRejectedBeforeAccessingTheBackend() {
    GravitinoClient client = mock(GravitinoClient.class);
    GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> client);
    try {
      GravitinoDorisCatalogSpark35 catalog = new GravitinoDorisCatalogSpark35();
      Identifier identifier = Identifier.of(new String[] {"db"}, "table");
      List<Executable> operations =
          List.of(
              () -> catalog.loadTable(identifier, Set.of()),
              () -> catalog.createTable(identifier, new StructType(), new Transform[0], Map.of()),
              () -> catalog.alterTable(identifier),
              () -> catalog.dropTable(identifier),
              () -> catalog.purgeTable(identifier),
              () -> catalog.renameTable(identifier, Identifier.of(new String[] {"db"}, "renamed")),
              () -> catalog.createNamespace(new String[] {"db"}, Map.of()),
              () -> catalog.alterNamespace(new String[] {"db"}),
              () -> catalog.dropNamespace(new String[] {"db"}, true));
      for (Executable operation : operations) {
        assertThrows(UnsupportedOperationException.class, operation);
      }
      verifyNoInteractions(client);
    } finally {
      GravitinoCatalogManager.get().close();
    }
  }

  private static GravitinoDorisCatalogSpark35 initializeCatalog(
      GravitinoClient client,
      TableCatalog tables,
      Map<String, String> properties,
      Map<String, String> options) {
    Catalog catalog = mock(Catalog.class);
    SupportsCredentials credentials = mock(SupportsCredentials.class);
    SupportsSecrets secrets = mock(SupportsSecrets.class);
    when(client.loadCatalog("doris")).thenReturn(catalog);
    when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(catalog.provider()).thenReturn("jdbc-doris");
    when(catalog.properties()).thenReturn(properties);
    when(catalog.asTableCatalog()).thenReturn(tables);
    when(catalog.supportsCredentials()).thenReturn(credentials);
    when(credentials.getCredentials())
        .thenReturn(new Credential[] {new JdbcCredential("vended-user", "test-vended-secret")});
    when(catalog.supportsSecrets()).thenReturn(secrets);
    when(secrets.getSecrets()).thenReturn(Map.of());
    GravitinoDorisCatalogSpark35 result = new GravitinoDorisCatalogSpark35();
    result.initialize("doris", new CaseInsensitiveStringMap(options));
    return result;
  }

  private static void assertSafeLoadFailure(IllegalArgumentException failure, String jdbcUrl) {
    assertTrue(failure.getMessage().contains("Spark JDBC schema load"));
    assertNull(failure.getCause());
    StringWriter stackTrace = new StringWriter();
    failure.printStackTrace(new PrintWriter(stackTrace));
    String diagnostics = stackTrace.toString();
    assertFalse(diagnostics.contains(jdbcUrl));
    assertFalse(diagnostics.contains("test-url-secret"));
    assertFalse(diagnostics.contains("test-vended-secret"));
  }
}
