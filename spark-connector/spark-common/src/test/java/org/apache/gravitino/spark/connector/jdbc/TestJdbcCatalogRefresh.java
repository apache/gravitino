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

package org.apache.gravitino.spark.connector.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

class TestJdbcCatalogRefresh {
  private static final String URL = "jdbc:h2:mem:jdbc_refresh;DB_CLOSE_DELAY=-1";

  private static Map<String, String> properties(String url) {
    return Map.of(
        "jdbc-url", url, "jdbc-user", "SA", "jdbc-password", "", "jdbc-driver", "org.h2.Driver");
  }

  private static Catalog source(TableCatalog tables) {
    Catalog source = mock(Catalog.class);
    when(source.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(source.provider()).thenReturn("jdbc-h2");
    when(source.properties()).thenReturn(properties(URL));
    when(source.supportsSecrets()).thenThrow(new UnsupportedOperationException());
    when(source.supportsCredentials()).thenThrow(new UnsupportedOperationException());
    when(source.asTableCatalog()).thenReturn(tables);
    return source;
  }

  @Test
  void rejectsStaleCreateAndAlterBeforeMutation() throws Exception {
    TableCatalog tables = mock(TableCatalog.class);
    Catalog source = source(tables);
    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("jdbc_refresh")).thenReturn(source);
    GravitinoCatalogManager manager =
        GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> client);
    try {
      GravitinoJdbcCatalog catalog = new GravitinoJdbcCatalog();
      catalog.initialize("jdbc_refresh", new CaseInsensitiveStringMap(Collections.emptyMap()));
      when(source.properties()).thenReturn(properties("jdbc:h2:mem:changed"));
      assertThrows(
          IllegalStateException.class,
          () ->
              catalog.createTable(
                  Identifier.of(new String[] {"PUBLIC"}, "T"),
                  new StructType().add("ID", DataTypes.IntegerType),
                  new Transform[0],
                  Collections.emptyMap()));
      assertThrows(
          IllegalStateException.class,
          () ->
              catalog.alterTable(
                  Identifier.of(new String[] {"PUBLIC"}, "T"),
                  TableChange.setProperty("key", "value")));
      verifyNoInteractions(tables);
    } finally {
      manager.close();
    }
  }

  @Test
  void rejectsRetainedDatasetAndWriteBuilder() throws Exception {
    try (Connection connection = DriverManager.getConnection(URL, "SA", "");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE IF NOT EXISTS PUBLIC.T (ID INT)");
      statement.execute("DELETE FROM PUBLIC.T");
      statement.execute("INSERT INTO PUBLIC.T VALUES (42)");
    }
    TableCatalog tables = mock(TableCatalog.class);
    Table metadata = mock(Table.class);
    when(metadata.name()).thenReturn("T");
    when(metadata.columns()).thenReturn(new Column[] {Column.of("ID", Types.IntegerType.get())});
    when(tables.loadTable(any(NameIdentifier.class), anySet())).thenReturn(metadata);
    Catalog source = source(tables);
    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("jdbc_refresh")).thenReturn(source);
    GravitinoCatalogManager manager =
        GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> client);
    try (SparkSession spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("jdbc_refresh")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalog.jdbc_refresh", GravitinoJdbcCatalog.class.getName())
            .getOrCreate()) {
      Dataset<Row> old = spark.table("jdbc_refresh.PUBLIC.T");
      old.schema();
      assertEquals(42, spark.table("jdbc_refresh.PUBLIC.T").collectAsList().get(0).getInt(0));
      GravitinoJdbcCatalog catalog =
          (GravitinoJdbcCatalog) spark.sessionState().catalogManager().catalog("jdbc_refresh");
      SparkJdbcTable retained =
          (SparkJdbcTable) catalog.loadTable(Identifier.of(new String[] {"PUBLIC"}, "T"));
      when(source.properties()).thenReturn(properties("jdbc:h2:mem:changed"));
      assertThrows(IllegalStateException.class, () -> retained.indexExists("idx"));
      assertThrows(
          IllegalStateException.class, () -> retained.createIndex("idx", null, null, null));
      assertThrows(IllegalStateException.class, () -> retained.dropIndex("idx"));
      assertThrows(IllegalStateException.class, retained::listIndexes);
      assertThrows(IllegalStateException.class, () -> spark.table("jdbc_refresh.PUBLIC.T"));
      assertTrue(
          assertThrows(IllegalStateException.class, old::collectAsList)
              .getMessage()
              .contains("Restart the Spark session"));
      assertTrue(
          assertThrows(IllegalStateException.class, () -> retained.newWriteBuilder(null))
              .getMessage()
              .contains("Restart the Spark session"));
    } finally {
      manager.close();
    }
  }

  @Test
  void newSessionUsesCurrentSettingsEvenWhenTheManagerHasAnOlderCatalog() throws Exception {
    Catalog previous = source(mock(TableCatalog.class));
    when(previous.properties()).thenReturn(properties("jdbc:h2:mem:previous"));
    Catalog current = source(mock(TableCatalog.class));
    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("jdbc_refresh")).thenReturn(previous, current);
    GravitinoCatalogManager manager =
        GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> client);
    try (Connection connection = DriverManager.getConnection(URL, "SA", "");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE IF NOT EXISTS PUBLIC.FRESH (ID INT)");
      manager.getGravitinoCatalogInfo("jdbc_refresh");
      GravitinoJdbcCatalog catalog = new GravitinoJdbcCatalog();
      catalog.initialize("jdbc_refresh", new CaseInsensitiveStringMap(Collections.emptyMap()));
      assertEquals(
          "ID",
          catalog
              .loadSparkTable(Identifier.of(new String[] {"PUBLIC"}, "FRESH"))
              .schema()
              .fields()[0]
              .name());
    } finally {
      manager.close();
    }
  }

  @Test
  void rejectsRotationOfMixedCaseBypassPassword() {
    Catalog source = source(mock(TableCatalog.class));
    Map<String, String> properties = new HashMap<>(properties(URL));
    properties.remove("jdbc-password");
    properties.put("spark.bypass.PaSsWoRd", "first");
    when(source.properties()).thenReturn(properties);
    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("jdbc_refresh")).thenReturn(source);
    GravitinoCatalogManager manager =
        GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> client);
    try {
      GravitinoJdbcCatalog catalog = new GravitinoJdbcCatalog();
      catalog.initialize("jdbc_refresh", new CaseInsensitiveStringMap(Collections.emptyMap()));
      properties.put("spark.bypass.PaSsWoRd", "second");
      IllegalStateException error =
          assertThrows(
              IllegalStateException.class,
              () -> catalog.loadSparkTable(Identifier.of(new String[] {"PUBLIC"}, "T")));
      assertTrue(error.getMessage().contains("Restart the Spark session"));
    } finally {
      manager.close();
    }
  }

  @Test
  void fetchesCredentialsOncePerSnapshot() throws Exception {
    TableCatalog tables = mock(TableCatalog.class);
    Catalog source = source(tables);
    SupportsCredentials credentials = mock(SupportsCredentials.class);
    doReturn(credentials).when(source).supportsCredentials();
    when(credentials.getCredentials()).thenReturn(new Credential[0]);
    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("jdbc_refresh")).thenReturn(source);
    GravitinoCatalogManager manager =
        GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> client);
    try {
      GravitinoJdbcCatalog catalog = new GravitinoJdbcCatalog();
      catalog.initialize("jdbc_refresh", new CaseInsensitiveStringMap(Collections.emptyMap()));
      verify(credentials).getCredentials();
      clearInvocations(credentials);
      when(source.properties()).thenReturn(properties("jdbc:h2:mem:changed"));
      assertThrows(
          IllegalStateException.class,
          () -> catalog.loadSparkTable(Identifier.of(new String[] {"PUBLIC"}, "T")));
      verify(credentials).getCredentials();
    } finally {
      manager.close();
    }
  }
}
