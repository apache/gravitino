/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql.converter;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import java.util.List;
import net.sf.jsqlparser.statement.create.table.ColDataType;

public class PostgreSqlTypeConverter extends JdbcTypeConverter<ColDataType, String> {

  public static final String BOOLEAN = "boolean";
  public static final String SMALLINT = "smallint";
  public static final String INTEGER = "integer";
  public static final String BIGINT = "bigint";
  public static final String REAL = "real";
  public static final String DOUBLE_PRECISION = "double precision";
  public static final String DATE = "date";
  public static final String TIME_WITHOUT_TIME_ZONE = "time without time zone";
  public static final String TIMESTAMP_WITHOUT_TIME_ZONE = "timestamp without time zone";
  public static final String TIMESTAMP_WITH_TIME_ZONE = "timestamp with time zone";
  public static final String NUMERIC = "numeric";
  public static final String CHARACTER_VARYING = "character varying";
  public static final String CHAR = "char";
  public static final String CHARACTER = "character";
  public static final String TEXT = "text";
  public static final String BYTEA = "bytea";

  @Override
  public Type toGravitinoType(ColDataType type) {
    List<String> arguments = type.getArgumentsStringList();
    // TODO #947 Complex types are not considered for support in this issue, which will bring more
    // testing needs
    switch (type.getDataType().toLowerCase()) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case SMALLINT:
        return Types.ShortType.get();
      case INTEGER:
        return Types.IntegerType.get();
      case BIGINT:
        return Types.LongType.get();
      case REAL:
        return Types.FloatType.get();
      case DOUBLE_PRECISION:
        return Types.DoubleType.get();
      case DATE:
        return Types.DateType.get();
      case TIME_WITHOUT_TIME_ZONE:
        return Types.TimeType.get();
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return Types.TimestampType.withoutTimeZone();
      case TIMESTAMP_WITH_TIME_ZONE:
        return Types.TimestampType.withTimeZone();
      case NUMERIC:
        return Types.DecimalType.of(
            Integer.parseInt(arguments.get(0)), Integer.parseInt(arguments.get(1)));
      case CHARACTER_VARYING:
        return Types.VarCharType.of(Integer.parseInt(arguments.get(0)));
      case CHAR:
      case CHARACTER:
        return Types.FixedCharType.of(Integer.parseInt(arguments.get(0)));
      case TEXT:
        return Types.StringType.get();
      case BYTEA:
        return Types.BinaryType.get();
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }

  @Override
  public String fromGravitinoType(Type type) {
    if (type instanceof Types.BooleanType) {
      return "boolean";
    } else if (type instanceof Types.ShortType) {
      return "smallint";
    } else if (type instanceof Types.IntegerType) {
      return "integer";
    } else if (type instanceof Types.LongType) {
      return "bigint";
    } else if (type instanceof Types.FloatType) {
      return "real";
    } else if (type instanceof Types.DoubleType) {
      return "double precision";
    } else if (type instanceof Types.StringType) {
      return "text";
    } else if (type instanceof Types.DateType) {
      return type.simpleString();
    } else if (type instanceof Types.TimeType) {
      return "time without time zone";
    } else if (type instanceof Types.TimestampType && !((Types.TimestampType) type).hasTimeZone()) {
      return "timestamp without time zone";
    } else if (type instanceof Types.TimestampType && ((Types.TimestampType) type).hasTimeZone()) {
      return "timestamp with time zone";
    } else if (type instanceof Types.DecimalType) {
      return "numeric("
          + ((Types.DecimalType) type).precision()
          + ","
          + ((Types.DecimalType) type).scale()
          + ")";
    } else if (type instanceof Types.VarCharType) {
      return "character varying(" + ((Types.VarCharType) type).length() + ")";
    } else if (type instanceof Types.FixedCharType) {
      return type.simpleString();
    } else if (type instanceof Types.BinaryType) {
      return "bytea";
    }
    throw new IllegalArgumentException(
        String.format("Couldn't convert PostgreSQL type %s to Gravitino type", type.toString()));
  }
}
