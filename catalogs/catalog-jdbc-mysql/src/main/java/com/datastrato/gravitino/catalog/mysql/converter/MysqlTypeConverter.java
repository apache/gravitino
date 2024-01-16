/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.converter;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import java.util.List;
import net.sf.jsqlparser.statement.create.table.ColDataType;

/** Type converter for MySQL. */
public class MysqlTypeConverter extends JdbcTypeConverter<ColDataType, String> {

  public static final String TINYINT = "tinyint";
  public static final String SMALLINT = "smallint";
  public static final String INT = "int";
  public static final String BIGINT = "bigint";
  public static final String FLOAT = "float";
  public static final String DOUBLE = "double";
  public static final String DATE = "date";
  public static final String TIME = "time";
  public static final String TIMESTAMP = "timestamp";
  public static final String DATETIME = "datetime";
  public static final String DECIMAL = "decimal";
  public static final String VARCHAR = "varchar";
  public static final String CHAR = "char";
  public static final String TEXT = "text";
  public static final String BINARY = "binary";

  @Override
  public Type toGravitinoType(ColDataType type) {
    List<String> arguments = type.getArgumentsStringList();
    switch (type.getDataType().toLowerCase()) {
      case TINYINT:
        return Types.ByteType.get();
      case SMALLINT:
        return Types.ShortType.get();
      case INT:
        return Types.IntegerType.get();
      case BIGINT:
        return Types.LongType.get();
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
            Integer.parseInt(arguments.get(0)), Integer.parseInt(arguments.get(1)));
      case VARCHAR:
        return Types.VarCharType.of(Integer.parseInt(arguments.get(0)));
      case CHAR:
        return Types.FixedCharType.of(Integer.parseInt(arguments.get(0)));
      case TEXT:
        return Types.StringType.get();
      case BINARY:
        return Types.BinaryType.get();
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }

  @Override
  public String fromGravitinoType(Type type) {
    if (type instanceof Types.ByteType) {
      return TINYINT;
    } else if (type instanceof Types.ShortType) {
      return SMALLINT;
    } else if (type instanceof Types.IntegerType) {
      return INT;
    } else if (type instanceof Types.LongType) {
      return BIGINT;
    } else if (type instanceof Types.FloatType) {
      return FLOAT;
    } else if (type instanceof Types.DoubleType) {
      return DOUBLE;
    } else if (type instanceof Types.StringType) {
      return TEXT;
    } else if (type instanceof Types.DateType) {
      return DATE;
    } else if (type instanceof Types.TimeType) {
      return TIME;
    } else if (type instanceof Types.TimestampType && !((Types.TimestampType) type).hasTimeZone()) {
      return TIMESTAMP;
    } else if (type instanceof Types.DecimalType) {
      return type.simpleString();
    } else if (type instanceof Types.VarCharType) {
      return type.simpleString();
    } else if (type instanceof Types.FixedCharType) {
      return type.simpleString();
    } else if (type instanceof Types.BinaryType) {
      return BINARY;
    }
    throw new IllegalArgumentException("Not a supported type: " + type.toString());
  }
}
