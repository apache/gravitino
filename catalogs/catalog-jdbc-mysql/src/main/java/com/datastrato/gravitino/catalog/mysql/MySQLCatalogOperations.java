/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql;

import com.datastrato.gravitino.catalog.jdbc.JdbcCatalogOperations;
import com.datastrato.gravitino.catalog.jdbc.JdbcTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLCatalogOperations extends JdbcCatalogOperations {
  private static final String CONNECTION_CLEAN_UP_THREAD =
      "com.mysql.cj.jdbc.AbandonedConnectionCleanupThread";

  private static final Logger LOG = LoggerFactory.getLogger(MySQLCatalogOperations.class);

  public MySQLCatalogOperations(
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

    // Close thread AbandonedConnectionCleanupThread
    try {
      Class.forName(CONNECTION_CLEAN_UP_THREAD).getMethod("uncheckedShutdown").invoke(null);
    } catch (Exception e) {
      // Ignore
      LOG.warn("Failed to shutdown AbandonedConnectionCleanupThread", e);
    }
  }
}
