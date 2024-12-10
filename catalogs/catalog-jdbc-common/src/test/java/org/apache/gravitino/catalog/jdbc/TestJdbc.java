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

import static org.apache.gravitino.dto.util.DTOConverters.toDTO;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.shaded.com.google.common.collect.Maps;

public abstract class TestJdbc {
  protected static JdbcDatabaseContainer<?> CONTAINER;

  protected static JdbcDatabaseOperations DATABASE_OPERATIONS;

  protected static JdbcTableOperations TABLE_OPERATIONS;

  protected static JdbcExceptionConverter JDBC_EXCEPTION_CONVERTER;

  protected static DataSource DATA_SOURCE;

  protected static final String TEST_DB_NAME = RandomNameUtils.genRandomName("test_db_");

  public static void startup() throws Exception {
    CONTAINER.start();
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), CONTAINER.getDriverClassName());
    properties.put(JdbcConfig.JDBC_URL.getKey(), CONTAINER.getJdbcUrl());
    properties.put(JdbcConfig.USERNAME.getKey(), CONTAINER.getUsername());
    properties.put(JdbcConfig.PASSWORD.getKey(), CONTAINER.getPassword());
    DATA_SOURCE = DataSourceUtils.createDataSource(properties);
  }

  protected void testBaseOperation(
      String databaseName, Map<String, String> properties, String comment) {
    // create database.
    DATABASE_OPERATIONS.create(databaseName, comment, properties);

    List<String> databases = DATABASE_OPERATIONS.listDatabases();
    Assertions.assertTrue(databases.contains(databaseName));
    // load database.
    JdbcSchema load = DATABASE_OPERATIONS.load(databaseName);

    Assertions.assertEquals(databaseName, load.name());
    Assertions.assertEquals(comment, load.comment());

    if (MapUtils.isNotEmpty(properties)) {
      Map<String, String> loadProperties = load.properties();
      properties.forEach(
          (key, value) -> {
            Assertions.assertTrue(loadProperties.containsKey(key));
            Assertions.assertEquals(loadProperties.get(key), value);
          });
    }
  }

  protected static void testDropDatabase(String databaseName) {
    List<String> databases;
    Assertions.assertTrue(
        DATABASE_OPERATIONS.delete(databaseName, true), "database should be dropped");

    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> DATABASE_OPERATIONS.load(databaseName));
    databases = DATABASE_OPERATIONS.listDatabases();
    Assertions.assertFalse(databases.contains(databaseName));
    Assertions.assertFalse(
        DATABASE_OPERATIONS.delete(databaseName, true), "database should be non-existent");
  }

  protected static void assertionsTableInfo(
      String tableName,
      String tableComment,
      List<JdbcColumn> columns,
      Map<String, String> properties,
      Index[] indexes,
      Transform[] partitioning,
      JdbcTable table) {
    Assertions.assertEquals(tableName, table.name());
    Assertions.assertEquals(tableComment, table.comment());
    Assertions.assertEquals(columns.size(), table.columns().length);
    for (int i = 0; i < columns.size(); i++) {
      assertColumn(columns.get(i), table.columns()[i]);
    }
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertEquals(entry.getValue(), table.properties().get(entry.getKey()));
    }
    if (ArrayUtils.isNotEmpty(indexes)) {
      Assertions.assertEquals(indexes.length, table.index().length);

      Map<String, Index> indexByName =
          Arrays.stream(indexes).collect(Collectors.toMap(Index::name, index -> index));

      for (int i = 0; i < table.index().length; i++) {
        Assertions.assertTrue(indexByName.containsKey(table.index()[i].name()));
        Assertions.assertEquals(
            indexByName.get(table.index()[i].name()).type(), table.index()[i].type());
        for (int j = 0; j < table.index()[i].fieldNames().length; j++) {
          Set<String> colNames =
              Arrays.stream(indexByName.get(table.index()[i].name()).fieldNames()[j])
                  .collect(Collectors.toSet());
          colNames.containsAll(Arrays.asList(table.index()[i].fieldNames()[j]));
        }
      }
    }
    Assertions.assertTrue(Arrays.deepEquals(table.partitioning(), partitioning));
  }

  public static void assertColumn(Column expected, Column actual) {
    if (!(actual instanceof ColumnDTO)) {
      actual = toDTO(actual);
    }
    if (!(expected instanceof ColumnDTO)) {
      expected = toDTO(expected);
    }

    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(
        expected.dataType(),
        actual.dataType(),
        String.format(
            "expected: %s, actual: %s",
            expected.dataType().simpleString(), actual.dataType().simpleString()));
    Assertions.assertEquals(expected.nullable(), actual.nullable());
    Assertions.assertEquals(expected.comment(), actual.comment());
    Assertions.assertEquals(expected.autoIncrement(), actual.autoIncrement());
    if (expected.defaultValue().equals(Column.DEFAULT_VALUE_NOT_SET) && expected.nullable()) {
      Assertions.assertEquals(LiteralDTO.NULL, actual.defaultValue());
    } else {
      Assertions.assertEquals(expected.defaultValue(), actual.defaultValue());
    }
  }

  @AfterAll
  public static void stop() {
    DataSourceUtils.closeDataSource(DATA_SOURCE);
    if (null != CONTAINER) {
      CONTAINER.stop();
    }
  }
}
