/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.datastrato.gravitino.catalog.jdbc.utils.DataSourceUtils;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.indexes.Index;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.shaded.com.google.common.collect.Maps;

public abstract class TestJdbcAbstractIT {
  protected static JdbcDatabaseContainer<?> CONTAINER;

  protected static JdbcDatabaseOperations DATABASE_OPERATIONS;

  protected static JdbcTableOperations TABLE_OPERATIONS;

  protected static JdbcExceptionConverter JDBC_EXCEPTION_CONVERTER;

  protected static DataSource DATA_SOURCE;

  protected static final String TEST_DB_NAME = GravitinoITUtils.genRandomName("test_db_");

  public static void startup() {
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
  }

  protected static void testDropDatabase(String databaseName) {
    List<String> databases;
    DATABASE_OPERATIONS.delete(databaseName, true);

    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> DATABASE_OPERATIONS.load(databaseName));
    databases = DATABASE_OPERATIONS.listDatabases();
    Assertions.assertFalse(databases.contains(databaseName));
  }

  protected static void assertionsTableInfo(
      String tableName,
      String tableComment,
      List<JdbcColumn> columns,
      Map<String, String> properties,
      Index[] indexes,
      JdbcTable table) {
    Assertions.assertEquals(tableName, table.name());
    Assertions.assertEquals(tableComment, table.comment());
    Assertions.assertEquals(columns.size(), table.columns().length);
    for (int i = 0; i < columns.size(); i++) {
      Assertions.assertEquals(columns.get(i).name(), table.columns()[i].name());
      Assertions.assertEquals(columns.get(i).dataType(), table.columns()[i].dataType());
      Assertions.assertEquals(columns.get(i).nullable(), table.columns()[i].nullable());
      Assertions.assertEquals(columns.get(i).comment(), table.columns()[i].comment());
      Assertions.assertEquals(columns.get(i).autoIncrement(), table.columns()[i].autoIncrement());
      // TODO: uncomment this after default value is supported.
      // Assertions.assertEquals(
      //    columns.get(i).getDefaultValue(), ((JdbcColumn) table.columns()[i]).getDefaultValue());
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
  }

  @AfterAll
  public static void stop() {
    DataSourceUtils.closeDataSource(DATA_SOURCE);
    if (null != CONTAINER) {
      CONTAINER.stop();
    }
  }
}
