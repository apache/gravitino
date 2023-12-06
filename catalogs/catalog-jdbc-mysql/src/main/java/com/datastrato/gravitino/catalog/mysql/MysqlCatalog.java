/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql;

import com.datastrato.gravitino.catalog.jdbc.JdbcCatalog;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.datastrato.gravitino.catalog.mysql.converter.MysqlExceptionConverter;
import com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter;
import com.datastrato.gravitino.catalog.mysql.operation.MysqlDatabaseOperations;
import com.datastrato.gravitino.catalog.mysql.operation.MysqlTableOperations;

/** Implementation of a Mysql catalog in Gravitino. */
public class MysqlCatalog extends JdbcCatalog {

  @Override
  public String shortName() {
    return "jdbc-mysql";
  }

  @Override
  protected JdbcExceptionConverter createExceptionConverter() {
    return new MysqlExceptionConverter();
  }

  @Override
  protected JdbcTypeConverter createJdbcTypeConverter() {
    return new MysqlTypeConverter();
  }

  @Override
  protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
    return new MysqlDatabaseOperations();
  }

  @Override
  protected JdbcTableOperations createJdbcTableOperations() {
    return new MysqlTableOperations();
  }
}
