/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql.integration.test;

import static com.datastrato.gravitino.catalog.postgresql.operation.PostgreSqlSchemaOperations.SYS_PG_DATABASE_NAMES;

import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.jdbc.utils.DataSourceUtils;
import com.datastrato.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import com.datastrato.gravitino.catalog.postgresql.operation.PostgreSqlSchemaOperations;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.utils.RandomNameUtils;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.Maps;

@Tag("gravitino-docker-it")
public class TestPostgreSqlSchemaOperations extends TestPostgreSqlAbstractIT {

  @Test
  public void testBaseOperationSchema() {
    String databaseName = RandomNameUtils.genRandomName("ct_db");
    Map<String, String> properties = new HashMap<>();
    // PostgreSql does not support filling in comments directly when creating a table.
    String comment = null;
    List<String> initDatabases = DATABASE_OPERATIONS.listDatabases();

    SYS_PG_DATABASE_NAMES.forEach(
        sysPgDatabaseName -> Assertions.assertFalse(initDatabases.contains(sysPgDatabaseName)));

    testBaseOperation(databaseName, properties, comment);
    // delete database.
    Assertions.assertDoesNotThrow(() -> DATABASE_OPERATIONS.delete(databaseName, true));

    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> DATABASE_OPERATIONS.load(databaseName));
    List<String> databases = DATABASE_OPERATIONS.listDatabases();
    Assertions.assertFalse(databases.contains(databaseName));
  }

  @Test
  public void testCreateMultipleSchema() throws SQLException {
    String testDbName = RandomNameUtils.genRandomName("test_db_");
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(connection, "CREATE DATABASE " + testDbName);
    }
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), CONTAINER.getDriverClassName());
    String jdbcUrl =
        StringUtils.substring(CONTAINER.getJdbcUrl(), 0, CONTAINER.getJdbcUrl().lastIndexOf("/"));
    properties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl + "/" + testDbName);
    properties.put(JdbcConfig.USERNAME.getKey(), CONTAINER.getUsername());
    properties.put(JdbcConfig.PASSWORD.getKey(), CONTAINER.getPassword());
    DataSource dataSource = DataSourceUtils.createDataSource(properties);
    PostgreSqlSchemaOperations postgreSqlSchemaOperations = new PostgreSqlSchemaOperations();
    Map<String, String> config =
        new HashMap<String, String>() {
          {
            put(JdbcConfig.JDBC_DATABASE.getKey(), testDbName);
          }
        };
    postgreSqlSchemaOperations.initialize(dataSource, JDBC_EXCEPTION_CONVERTER, config);

    String schema_1 = "schema_multiple_1";
    DATABASE_OPERATIONS.create(schema_1, null, null);

    List<String> schemaNames = postgreSqlSchemaOperations.listDatabases();
    Assertions.assertFalse(schemaNames.contains(schema_1));

    String schema_2 = "schema_multiple_2";
    postgreSqlSchemaOperations.create(schema_2, null, null);

    schemaNames = DATABASE_OPERATIONS.listDatabases();
    Assertions.assertFalse(schemaNames.contains(schema_2));

    postgreSqlSchemaOperations.delete(schema_2, true);

    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> {
          postgreSqlSchemaOperations.load(schema_2);
        });

    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> {
          DATABASE_OPERATIONS.load(schema_2);
        });

    postgreSqlSchemaOperations.create(schema_1, null, null);
    JdbcSchema load = postgreSqlSchemaOperations.load(schema_1);
    Assertions.assertEquals(schema_1, load.name());
  }
}
