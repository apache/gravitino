/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.converter;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;

/** Type converter for MySQL. */
public class MysqlTypeConverter extends JdbcTypeConverter<String> {

  static final String TINYINT = "tinyint";
  static final String TINYINT_UNSIGNED = "tinyint unsigned";
  static final String SMALLINT = "smallint";
  static final String SMALLINT_UNSIGNED = "smallint unsigned";
  static final String INT = "int";
  static final String INT_UNSIGNED = "int unsigned";
  static final String BIGINT = "bigint";
  static final String BIGINT_UNSIGNED = "bigint unsigned";
  static final String FLOAT = "float";
  static final String DOUBLE = "double";
  static final String DECIMAL = "decimal";
  static final String CHAR = "char";
  static final String BINARY = "binary";
  static final String DATETIME = "datetime";

  @Override
  public Type toGravitinoType(JdbcTypeBean typeBean) {
    switch (typeBean.getTypeName().toLowerCase()) {
      case TINYINT:
        return Types.ByteType.get();
      case TINYINT_UNSIGNED:
        return Types.ByteType.of(false);
      case SMALLINT:
        return Types.ShortType.get();
      case SMALLINT_UNSIGNED:
        return Types.ShortType.of(false);
      case INT:
        return Types.IntegerType.get();
      case INT_UNSIGNED:
        return Types.IntegerType.of(false);
      case BIGINT:
        return Types.LongType.get();
      case BIGINT_UNSIGNED:
        return Types.LongType.of(false);
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case DATE:
        return Types.DateType.get();
      case TIME:
        return Types.TimeType.get();
      case TIMESTAMP:
        return Types.TimestampType.withoutTimeZone();
      case DECIMAL:
        return Types.DecimalType.of(
            Integer.parseInt(typeBean.getColumnSize()), Integer.parseInt(typeBean.getScale()));
      case VARCHAR:
        return Types.VarCharType.of(Integer.parseInt(typeBean.getColumnSize()));
      case CHAR:
        return Types.FixedCharType.of(Integer.parseInt(typeBean.getColumnSize()));
      case TEXT:
        return Types.StringType.get();
      case BINARY:
        return Types.BinaryType.get();
      default:
        return Types.UnparsedType.of(typeBean.getTypeName());
    }
  }

  @Override
  public String fromGravitinoType(Type type) {
    if (type instanceof Types.ByteType) {
      if (((Types.ByteType) type).unsigned()) {
        return TINYINT_UNSIGNED;
      } else {
        return TINYINT;
      }
    } else if (type instanceof Types.ShortType) {
      if (((Types.ShortType) type).unsigned()) {
        return SMALLINT_UNSIGNED;
      } else {
        return SMALLINT;
      }
    } else if (type instanceof Types.IntegerType) {
      if (((Types.IntegerType) type).unsigned()) {
        return INT_UNSIGNED;
      } else {
        return INT;
      }
    } else if (type instanceof Types.LongType) {
      if (((Types.LongType) type).unsigned()) {
        return BIGINT_UNSIGNED;
      } else {
        return BIGINT;
      }
    } else if (type instanceof Types.FloatType) {
      return type.simpleString();
    } else if (type instanceof Types.DoubleType) {
      return type.simpleString();
    } else if (type instanceof Types.StringType) {
      return TEXT;
    } else if (type instanceof Types.DateType) {
      return type.simpleString();
    } else if (type instanceof Types.TimeType) {
      return type.simpleString();
    } else if (type instanceof Types.TimestampType && !((Types.TimestampType) type).hasTimeZone()) {
      return type.simpleString();
    } else if (type instanceof Types.DecimalType) {
      return type.simpleString();
    } else if (type instanceof Types.VarCharType) {
      return type.simpleString();
    } else if (type instanceof Types.FixedCharType) {
      return type.simpleString();
    } else if (type instanceof Types.BinaryType) {
      return type.simpleString();
    }
    throw new IllegalArgumentException(
        String.format("Couldn't convert Gravitino type %s to MySQL type", type.simpleString()));
  }
}
