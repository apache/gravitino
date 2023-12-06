/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql.converter;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import java.util.List;
import net.sf.jsqlparser.statement.create.table.ColDataType;

public class PostgreSqlTypeConverter extends JdbcTypeConverter<ColDataType, String> {
  @Override
  public Type toGravitinoType(ColDataType type) {
    List<String> arguments = type.getArgumentsStringList();
    // TODO #947 Complex types are not considered for support in this issue, which will bring more
    // testing needs
    switch (type.getDataType().toLowerCase()) {
      case "boolean":
        return Types.BooleanType.get();
      case "smallint":
        return Types.ShortType.get();
      case "integer":
        return Types.IntegerType.get();
      case "bigint":
        return Types.LongType.get();
      case "real":
        return Types.FloatType.get();
      case "double precision":
        return Types.DoubleType.get();
      case "date":
        return Types.DateType.get();
      case "time without time zone":
        return Types.TimeType.get();
      case "timestamp without time zone":
        return Types.TimestampType.withoutTimeZone();
      case "timestamp with time zone":
        return Types.TimestampType.withTimeZone();
      case "numeric":
        return Types.DecimalType.of(
            Integer.parseInt(arguments.get(0)), Integer.parseInt(arguments.get(1)));
      case "character varying":
        return Types.VarCharType.of(Integer.parseInt(arguments.get(0)));
      case "char":
      case "character":
        return Types.FixedCharType.of(Integer.parseInt(arguments.get(0)));
      case "text":
        return Types.StringType.get();
      case "bytea":
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
