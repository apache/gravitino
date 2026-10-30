/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.catalog.starrocks.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import javax.sql.DataSource;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.starrocks.StarRocksCatalog;
import org.apache.gravitino.catalog.starrocks.StarRocksCatalogOperations;
import org.apache.gravitino.catalog.starrocks.StarRocksView;
import org.apache.gravitino.catalog.starrocks.converter.StarRocksColumnDefaultValueConverter;
import org.apache.gravitino.catalog.starrocks.converter.StarRocksExceptionConverter;
import org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter;
import org.apache.gravitino.catalog.starrocks.operations.StarRocksDatabaseOperations;
import org.apache.gravitino.catalog.starrocks.operations.StarRocksViewOperations;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestStarRocksViewOperations {
  private Connection connection;

  /** Statement and result for the information_schema.tables lookup (existence + comment). */
  private PreparedStatement statement;

  private ResultSet result;

  /** Statement and result for the information_schema.views lookup (SQL definition). */
  private PreparedStatement definitionStatement;

  private ResultSet definitionResult;

  /** JDBC metadata column listing. */
  private ResultSet columnResult;

  private StarRocksViewOperations tables;

  @BeforeEach
  void setUp() throws SQLException {
    DataSource source = mock(DataSource.class);
    connection = mock(Connection.class);
    statement = mock(PreparedStatement.class);
    result = mock(ResultSet.class);
    definitionStatement = mock(PreparedStatement.class);
    definitionResult = mock(ResultSet.class);
    columnResult = mock(ResultSet.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(source.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(connection.prepareStatement(StarRocksViewOperations.DEFINITION_SQL))
        .thenReturn(definitionStatement);
    when(statement.executeQuery()).thenReturn(result);
    when(definitionStatement.executeQuery()).thenReturn(definitionResult);
    // By default the name is a logical view: it has an information_schema.views row.
    when(definitionResult.next()).thenReturn(true);
    when(connection.getMetaData()).thenReturn(metaData);
    when(metaData.getColumns(any(), any(), anyString(), any())).thenReturn(columnResult);
    tables = new StarRocksViewOperations();
    tables.initialize(
        source,
        new StarRocksExceptionConverter(),
        new StarRocksTypeConverter(),
        new StarRocksColumnDefaultValueConverter(),
        Collections.emptyMap());
  }

  @Test
  void loadsNativeViewMetadataAndClosesResources() throws SQLException {
    when(result.next()).thenReturn(true, false);
    when(result.getString("TABLE_NAME")).thenReturn("mv_region_dws_funlink_app_day");
    when(result.getString("TABLE_COMMENT")).thenReturn("Application daily metrics");
    View view = tables.loadView("funlink_ads", "mv_region_dws_funlink_app_day");
    assertEquals("mv_region_dws_funlink_app_day", view.name());
    assertEquals("Application daily metrics", view.comment());
    assertEquals(AuditInfo.EMPTY, view.auditInfo());
    assertTrue(view.properties().isEmpty());
    assertEquals(0, view.columns().length);
    assertEquals(0, view.representations().length);
    assertEquals("funlink_ads", view.defaultSchema());
    verify(statement).setString(1, "funlink_ads");
    verify(statement).setString(2, view.name());
    verify(connection).setCatalog("funlink_ads");
    verify(result).close();
    verify(statement).close();
    verify(definitionResult).close();
    verify(definitionStatement).close();
    verify(columnResult).close();
    verify(connection).close();
  }

  @Test
  void exposesColumnsAndSqlDefinitionForSemanticModelSources() throws SQLException {
    when(result.next()).thenReturn(true, false);
    when(result.getString("TABLE_NAME")).thenReturn("v_orders");
    when(definitionResult.next()).thenReturn(true);
    when(definitionResult.getString("VIEW_DEFINITION"))
        .thenReturn("SELECT `id`, `name` FROM `orders`");
    // Two matching columns plus one from a similarly named table that must be skipped.
    when(columnResult.next()).thenReturn(true, true, true, false);
    when(columnResult.getString("TABLE_NAME")).thenReturn("v_orders", "v_orders", "v_orders_x");
    when(columnResult.getString("COLUMN_NAME")).thenReturn("id", "name");
    when(columnResult.getString("TYPE_NAME")).thenReturn("INT", "VARCHAR");
    when(columnResult.getInt("COLUMN_SIZE")).thenReturn(10, 128);
    when(columnResult.getString("REMARKS")).thenReturn("order id", "");
    when(columnResult.getBoolean("NULLABLE")).thenReturn(false, true);

    View view = tables.loadView("mart", "v_orders");

    Column[] columns = view.columns();
    assertEquals(2, columns.length);
    assertEquals("id", columns[0].name());
    assertEquals(Types.IntegerType.get(), columns[0].dataType());
    assertEquals("order id", columns[0].comment());
    assertFalse(columns[0].nullable());
    assertEquals("name", columns[1].name());
    assertEquals(Types.VarCharType.of(128), columns[1].dataType());
    assertNull(columns[1].comment());
    assertTrue(columns[1].nullable());

    SQLRepresentation sql = view.sqlFor(StarRocksView.DIALECT).orElseThrow(AssertionError::new);
    assertEquals("SELECT `id`, `name` FROM `orders`", sql.sql());
    assertEquals(1, view.representations().length);
    assertEquals("mart", view.defaultSchema());
    verify(definitionStatement).setString(1, "mart");
    verify(definitionStatement).setString(2, "v_orders");
  }

  @Test
  void asyncMaterializedViewIsNotAView() throws SQLException {
    // StarRocks reports async MVs as TABLE_TYPE 'VIEW', but they have no information_schema.views
    // row; they must not be served as views, and their columns are never read.
    when(result.next()).thenReturn(true, false);
    when(result.getString("TABLE_NAME")).thenReturn("mv_rollup");
    when(definitionResult.next()).thenReturn(false);
    assertThrows(NoSuchViewException.class, () -> tables.loadView("db", "mv_rollup"));
    verify(connection, never()).getMetaData();
    verify(connection).close();
  }

  @Test
  void listsOnlyLogicalViewsOfTheDatabase() throws SQLException {
    PreparedStatement listStatement = mock(PreparedStatement.class);
    ResultSet listResult = mock(ResultSet.class);
    when(connection.prepareStatement(StarRocksViewOperations.LIST_VIEWS_SQL))
        .thenReturn(listStatement);
    when(listStatement.executeQuery()).thenReturn(listResult);
    // Two views of funlink_ads plus a row of a differently cased schema that must be skipped.
    when(listResult.next()).thenReturn(true, true, true, false);
    when(listResult.getString("TABLE_SCHEMA"))
        .thenReturn("funlink_ads", "FUNLINK_ADS", "funlink_ads");
    when(listResult.getString("TABLE_NAME")).thenReturn("v_a", "v_b");
    assertEquals(Arrays.asList("v_a", "v_b"), tables.listViews("funlink_ads"));
    verify(listStatement).setString(1, "funlink_ads");
    assertTrue(StarRocksViewOperations.LIST_VIEWS_SQL.contains("information_schema.views"));
    verify(listResult).close();
    verify(listStatement).close();
    verify(connection).close();
  }

  @Test
  void catalogListViewsMapsNamesIntoTheSchemaNamespace() {
    StarRocksViewOperations delegate = mock(StarRocksViewOperations.class);
    when(delegate.listViews("db")).thenReturn(Arrays.asList("v1", "v2"));
    ViewCatalog catalog =
        new StarRocksCatalogOperations(
            new StarRocksExceptionConverter(),
            new StarRocksTypeConverter(),
            new StarRocksDatabaseOperations(),
            delegate,
            new StarRocksColumnDefaultValueConverter());
    Namespace ns = Namespace.of("metalake", "catalog", "db");
    assertEquals(
        Arrays.asList(NameIdentifier.of(ns, "v1"), NameIdentifier.of(ns, "v2")),
        Arrays.asList(catalog.listViews(ns)));
  }

  @Test
  void tableLoadOfAViewNameIsNoSuchTable() throws SQLException {
    // The table lookup must ask only for TABLE types, so a view name yields NoSuchTableException
    // (not a SHOW CREATE TABLE failure) and callers can fall back to the view API.
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet tableLookup = mock(ResultSet.class);
    when(metaData.getTables(any(), any(), anyString(), any())).thenReturn(tableLookup);
    when(tableLookup.next()).thenReturn(false);
    assertThrows(NoSuchTableException.class, () -> tables.load("funlink_ads", "mv_daily"));
    ArgumentCaptor<String[]> types = ArgumentCaptor.forClass(String[].class);
    verify(metaData).getTables(any(), any(), anyString(), types.capture());
    assertEquals(Arrays.asList("TABLE"), Arrays.asList(types.getValue()));
  }

  @Test
  void blankOrMissingDefinitionYieldsNoRepresentation() throws SQLException {
    when(result.next()).thenReturn(true, false);
    when(result.getString("TABLE_NAME")).thenReturn("v");
    when(definitionResult.next()).thenReturn(true);
    when(definitionResult.getString("VIEW_DEFINITION")).thenReturn("  ");
    assertEquals(0, tables.loadView("db", "v").representations().length);
  }

  @Test
  void rejectsMissingViewsAndNeverTreatsTablesAsViews() throws SQLException {
    when(result.next()).thenReturn(false);
    assertThrows(NoSuchViewException.class, () -> tables.loadView("funlink_ads", "base_table"));
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    // Only the existence check runs; a missing view never reads definitions or columns.
    verify(connection).prepareStatement(sql.capture());
    assertTrue(sql.getValue().contains("TABLE_TYPE = 'VIEW'"));
    verify(connection, never()).getMetaData();
    verify(result).close();
    verify(connection).close();
  }

  @Test
  void bindsNamesAsParametersAndPreservesNullComments() throws SQLException {
    String name = "view_'?_%";
    when(result.next()).thenReturn(true, false);
    when(result.getString("TABLE_NAME")).thenReturn(name);
    assertNull(tables.loadView("db", name).comment());
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    verify(connection, times(2)).prepareStatement(sql.capture());
    for (String statementSql : sql.getAllValues()) {
      assertFalse(statementSql.contains(name));
    }
    verify(statement).setString(2, name);
    verify(definitionStatement).setString(2, name);
  }

  @Test
  void doesNotHideSqlFailuresAsMissingViews() throws SQLException {
    when(statement.executeQuery()).thenThrow(new SQLException("connection unavailable", "08006"));
    RuntimeException failure =
        assertThrows(RuntimeException.class, () -> tables.loadView("db", "v"));
    assertFalse(failure instanceof NoSuchViewException);
    verify(statement).close();
    verify(connection).close();
  }

  @Test
  void factoryExposesViewCatalogAndRoutesTheFullIdentifierToTheDatabase() throws Exception {
    class Factory extends StarRocksCatalog {
      CatalogOperations operations() {
        return newOps(Collections.emptyMap());
      }
    }
    assertTrue(new Factory().operations() instanceof ViewCatalog);
    StarRocksViewOperations delegate = mock(StarRocksViewOperations.class);
    View expected = new StarRocksView("v", null);
    when(delegate.loadView("db", "v")).thenReturn(expected);
    ViewCatalog catalog =
        new StarRocksCatalogOperations(
            new StarRocksExceptionConverter(),
            new StarRocksTypeConverter(),
            new StarRocksDatabaseOperations(),
            delegate,
            new StarRocksColumnDefaultValueConverter());
    assertSame(expected, catalog.loadView(NameIdentifier.of("metalake", "catalog", "db", "v")));
    assertTrue(catalog.viewExists(NameIdentifier.of("metalake", "catalog", "db", "v")));
    // CatalogManager.CatalogWrapper routes view calls to ops() when ops() is a ViewCatalog; its
    // constructors are package-private on 2.0, so that routing is covered by the instanceof check
    // above rather than by building a wrapper here.
  }

  @Test
  void rejectsTableOperationsThatCannotServeViews() {
    JdbcTableOperations plainTableOperations = mock(JdbcTableOperations.class);
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new StarRocksCatalogOperations(
                    new StarRocksExceptionConverter(),
                    new StarRocksTypeConverter(),
                    new StarRocksDatabaseOperations(),
                    plainTableOperations,
                    new StarRocksColumnDefaultValueConverter()));
    assertTrue(failure.getMessage().contains("StarRocksViewOperations"));
  }
}
