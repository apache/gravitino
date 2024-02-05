/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc.postgresql;

import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlExceptionConverter;
import com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter;
import com.datastrato.gravitino.catalog.postgresql.operation.PostgreSqlSchemaOperations;
import com.datastrato.gravitino.catalog.postgresql.operation.PostgreSqlTableOperations;
import com.datastrato.gravitino.integration.test.catalog.jdbc.TestJdbcAbstractIT;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

public class TestPostgreSqlAbstractIT extends TestJdbcAbstractIT {

  @BeforeAll
  public static void startup() {
    CONTAINER =
        new PostgreSQLContainer<>(CatalogPostgreSqlIT.DEFAULT_POSTGRES_IMAGE)
            .withDatabaseName(TEST_DB_NAME)
            .withUsername("root")
            .withPassword("root");
    DATABASE_OPERATIONS = new PostgreSqlSchemaOperations();
    JDBC_EXCEPTION_CONVERTER = new PostgreSqlExceptionConverter();
    TestJdbcAbstractIT.startup();
    String jdbcUrl = CONTAINER.getJdbcUrl();
    try {
      String database =
          new URI(CONTAINER.getJdbcUrl().substring(jdbcUrl.lastIndexOf("/") + 1, jdbcUrl.length()))
              .getPath();
      Map<String, String> config =
          new HashMap<String, String>() {
            {
              put(JdbcConfig.JDBC_DATABASE.getKey(), database);
            }
          };
      TABLE_OPERATIONS = new PostgreSqlTableOperations();
      DATABASE_OPERATIONS.initialize(DATA_SOURCE, JDBC_EXCEPTION_CONVERTER, config);
      TABLE_OPERATIONS.initialize(
          DATA_SOURCE,
          JDBC_EXCEPTION_CONVERTER,
          new PostgreSqlTypeConverter(),
          new PostgreSqlColumnDefaultValueConverter(),
          config);
      DATABASE_OPERATIONS.create(TEST_DB_NAME, null, null);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
