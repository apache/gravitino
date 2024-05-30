/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris;

import com.datastrato.gravitino.catalog.doris.converter.DorisColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.doris.converter.DorisExceptionConverter;
import com.datastrato.gravitino.catalog.doris.converter.DorisTypeConverter;
import com.datastrato.gravitino.catalog.doris.operation.DorisDatabaseOperations;
import com.datastrato.gravitino.catalog.doris.operation.DorisTableOperations;
import com.datastrato.gravitino.catalog.jdbc.JdbcCatalog;
import com.datastrato.gravitino.catalog.jdbc.MySQLProtocolCompatibleCatalogOperations;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.datastrato.gravitino.connector.CatalogOperations;
import java.util.Map;

/** Implementation of a Doris catalog in Gravitino. */
public class DorisCatalog extends JdbcCatalog {

  @Override
  public String shortName() {
    return "jdbc-doris";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    JdbcTypeConverter<String> jdbcTypeConverter = createJdbcTypeConverter();
    return new MySQLProtocolCompatibleCatalogOperations(
        createExceptionConverter(),
        jdbcTypeConverter,
        createJdbcDatabaseOperations(),
        createJdbcTableOperations(),
        createJdbcColumnDefaultValueConverter());
  }

  @Override
  protected JdbcExceptionConverter createExceptionConverter() {
    return new DorisExceptionConverter();
  }

  @Override
  protected JdbcTypeConverter createJdbcTypeConverter() {
    return new DorisTypeConverter();
  }

  @Override
  protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
    return new DorisDatabaseOperations();
  }

  @Override
  protected JdbcTableOperations createJdbcTableOperations() {
    return new DorisTableOperations();
  }

  @Override
  protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
    return new DorisColumnDefaultValueConverter();
  }
}
