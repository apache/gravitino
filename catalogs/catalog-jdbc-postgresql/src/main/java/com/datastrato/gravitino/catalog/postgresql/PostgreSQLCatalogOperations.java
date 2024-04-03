/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql;

import com.datastrato.gravitino.catalog.jdbc.JdbcCatalogOperations;
import com.datastrato.gravitino.catalog.jdbc.JdbcTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import java.sql.Driver;
import java.sql.DriverManager;

public class PostgreSQLCatalogOperations extends JdbcCatalogOperations {

  public PostgreSQLCatalogOperations(
      JdbcExceptionConverter exceptionConverter,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcDatabaseOperations databaseOperation,
      JdbcTableOperations tableOperation,
      JdbcTablePropertiesMetadata jdbcTablePropertiesMetadata,
      JdbcColumnDefaultValueConverter columnDefaultValueConverter) {
    super(
        exceptionConverter,
        jdbcTypeConverter,
        databaseOperation,
        tableOperation,
        jdbcTablePropertiesMetadata,
        columnDefaultValueConverter);
  }

  @Override
  public void close() {
    super.close();
    try {
      // Deregister the PostgreSQL driver, only deregister the driver if it is loaded by
      // IsolatedClassLoader.
      Driver pgDriver = DriverManager.getDriver("jdbc:postgresql://127.0.0.1:5432/");
      LOG.info(
          "PostgreSQL driver class loader: {}",
          pgDriver.getClass().getClassLoader().getClass().getName());
      deregisterDriver(pgDriver);
    } catch (Exception e) {
      LOG.warn("Failed to deregister PostgreSQL driver", e);
    }
  }
}
