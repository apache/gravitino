/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql;

import com.datastrato.gravitino.catalog.jdbc.JdbcCatalog;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlExceptionConverter;
import com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter;
import com.datastrato.gravitino.catalog.postgresql.operation.PostgreSqlSchemaOperations;
import com.datastrato.gravitino.catalog.postgresql.operation.PostgreSqlTableOperations;
import com.datastrato.gravitino.connector.CatalogOperations;
import java.util.Map;

public class PostgreSqlCatalog extends JdbcCatalog {

  @Override
  public String shortName() {
    return "jdbc-postgresql";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    JdbcTypeConverter jdbcTypeConverter = createJdbcTypeConverter();
    return new PostgreSQLCatalogOperations(
        createExceptionConverter(),
        jdbcTypeConverter,
        createJdbcDatabaseOperations(),
        createJdbcTableOperations(),
        createJdbcColumnDefaultValueConverter());
  }

  @Override
  protected JdbcExceptionConverter createExceptionConverter() {
    return new PostgreSqlExceptionConverter();
  }

  @Override
  protected JdbcTypeConverter createJdbcTypeConverter() {
    return new PostgreSqlTypeConverter();
  }

  @Override
  protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
    return new PostgreSqlSchemaOperations();
  }

  @Override
  protected JdbcTableOperations createJdbcTableOperations() {
    return new PostgreSqlTableOperations();
  }

  @Override
  protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
    return new PostgreSqlColumnDefaultValueConverter();
  }
}
