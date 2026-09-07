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
package org.apache.gravitino.catalog.jdbc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.SqliteColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.SqliteDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.SqliteTableOperations;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class TestJdbcCatalogOperations {

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"  owner's comment  ", "line one\nline two"})
  void testCreateTablePreservesSourceComment(String comment) throws Exception {
    JdbcTableOperations tableOperations = mock(JdbcTableOperations.class);
    JdbcCatalogOperations operations = newTableCatalogOperations(tableOperations);
    Map<String, String> properties =
        StringIdentifier.newPropertiesWithId(StringIdentifier.fromId(42), Collections.emptyMap());
    Table table =
        operations.createTable(
            NameIdentifier.of("metalake", "catalog", "schema", "table"),
            new Column[0],
            comment,
            properties,
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            new SortOrder[0],
            Indexes.EMPTY_INDEXES);

    verify(tableOperations)
        .create(
            eq("schema"),
            eq("table"),
            any(JdbcColumn[].class),
            eq(comment),
            eq(Collections.emptyMap()),
            any(),
            any(),
            any(),
            any());
    Assertions.assertEquals(comment, table.comment());
    Assertions.assertFalse(table.properties().containsKey(StringIdentifier.ID_KEY));
    Assertions.assertEquals("gravitino.v1.uid42", properties.get(StringIdentifier.ID_KEY));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"  owner's comment  ", "line one\nline two"})
  void testLoadTableWithoutMarker(String comment) throws Exception {
    JdbcTableOperations tableOperations = mock(JdbcTableOperations.class);
    JdbcCatalogOperations operations = newTableCatalogOperations(tableOperations);
    when(tableOperations.load("schema", "table"))
        .thenReturn(JdbcTable.builder().withName("table").withComment(comment).build());
    Table table = operations.loadTable(NameIdentifier.of("metalake", "catalog", "schema", "table"));
    Assertions.assertEquals(comment, table.comment());
    Assertions.assertFalse(table.properties().containsKey(StringIdentifier.ID_KEY));
  }

  @Test
  void testLoadLegacyTableIdentifier() throws Exception {
    JdbcTableOperations tableOperations = mock(JdbcTableOperations.class);
    JdbcCatalogOperations operations = newTableCatalogOperations(tableOperations);
    StringIdentifier identifier = StringIdentifier.fromId(42);
    when(tableOperations.load("schema", "table"))
        .thenReturn(
            JdbcTable.builder()
                .withName("table")
                .withComment(StringIdentifier.addToComment(identifier, "old comment"))
                .build());
    Table table = operations.loadTable(NameIdentifier.of("metalake", "catalog", "schema", "table"));
    Assertions.assertEquals("old comment", table.comment());
    Assertions.assertEquals(
        identifier.id(), StringIdentifier.fromProperties(table.properties()).id());
  }

  @Test
  public void testExistingCatalogConnectionFailure() {
    SQLException cause = new SQLException("connection refused");
    SqliteDatabaseOperations databaseOperations =
        new SqliteDatabaseOperations("/unused") {
          @Override
          public List<String> listDatabases() {
            throw new GravitinoRuntimeException(cause, cause.getMessage());
          }
        };

    try (JdbcCatalogOperations catalogOperations =
        new JdbcCatalogOperations(
            new SqliteExceptionConverter(),
            new SqliteTypeConverter(),
            databaseOperations,
            new SqliteTableOperations(),
            new SqliteColumnDefaultValueConverter())) {
      ConnectionFailedException exception =
          Assertions.assertThrows(
              ConnectionFailedException.class,
              () -> catalogOperations.testConnection(NameIdentifier.of("metalake", "catalog")));
      Assertions.assertSame(cause, exception.getCause());
    }
  }

  @Test
  public void testConfigTestOnBorrow() throws SQLException {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put(JdbcConfig.TEST_ON_BORROW.getKey(), "false");

    DataSource dataSource =
        Assertions.assertDoesNotThrow(() -> DataSourceUtils.createDataSource(properties));
    Assertions.assertInstanceOf(BasicDataSource.class, dataSource);
    Assertions.assertFalse(((BasicDataSource) dataSource).getTestOnBorrow());
    ((BasicDataSource) dataSource).close();
  }

  @Test
  public void testCloseDoesNotThrow() {
    JdbcCatalogOperations catalogOperations =
        new JdbcCatalogOperations(
            new SqliteExceptionConverter(),
            new SqliteTypeConverter(),
            new SqliteDatabaseOperations("/illegal/path"),
            new SqliteTableOperations(),
            new SqliteColumnDefaultValueConverter());

    Assertions.assertDoesNotThrow(catalogOperations::close);
  }

  private JdbcCatalogOperations newTableCatalogOperations(JdbcTableOperations tableOperations)
      throws IllegalAccessException {
    JdbcCatalogOperations operations =
        new JdbcCatalogOperations(
            new SqliteExceptionConverter(),
            new SqliteTypeConverter(),
            mock(SqliteDatabaseOperations.class),
            tableOperations,
            new SqliteColumnDefaultValueConverter());
    FieldUtils.writeField(
        operations,
        "jdbcTablePropertiesMetadata",
        mock(JdbcTablePropertiesMetadata.class, Mockito.CALLS_REAL_METHODS),
        true);
    return operations;
  }
}
