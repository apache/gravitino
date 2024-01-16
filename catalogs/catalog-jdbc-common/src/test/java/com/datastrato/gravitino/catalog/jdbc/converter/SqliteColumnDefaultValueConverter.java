/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.converter;

import com.datastrato.gravitino.rel.expressions.Expression;

public class SqliteColumnDefaultValueConverter
    extends JdbcColumnDefaultValueConverter<String, String> {
  public SqliteColumnDefaultValueConverter(SqliteTypeConverter sqliteTypeConverter) {
    super(sqliteTypeConverter);
  }

  @Override
  public Expression toGravitino(String columnType, String columnDefaultValue) {
    return null;
  }
}
