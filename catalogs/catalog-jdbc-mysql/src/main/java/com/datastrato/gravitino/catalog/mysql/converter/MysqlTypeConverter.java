/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
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

  @Override
  public Type toGravitinoType(ColDataType type) {
    List<String> arguments = type.getArgumentsStringList();
    switch (type.getDataType().toLowerCase()) {
      case "boolean":
        return Types.BooleanType.get();
      case "tinyint":
        return Types.ByteType.get();
      case "smallint":
        return Types.ShortType.get();
      case "int":
        return Types.IntegerType.get();
      case "bigint":
        return Types.LongType.get();
      case "float":
        return Types.FloatType.get();
      case "double":
        return Types.DoubleType.get();
      case "date":
        return Types.DateType.get();
      case "time":
        return Types.TimeType.get();
      case "timestamp":
        return Types.TimestampType.withoutTimeZone();
      case "decimal":
        return Types.DecimalType.of(
            Integer.parseInt(arguments.get(0)), Integer.parseInt(arguments.get(1)));
      case "varchar":
        return Types.VarCharType.of(Integer.parseInt(arguments.get(0)));
      case "char":
        return Types.FixedCharType.of(Integer.parseInt(arguments.get(0)));
      case "text":
        return Types.StringType.get();
      case "binary":
        return Types.BinaryType.get();
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }

  @Override
  public String fromGravitinoType(Type type) {
    if (type instanceof Types.BooleanType) {
      return type.simpleString();
    } else if (type instanceof Types.ByteType) {
      return "tinyint";
    } else if (type instanceof Types.ShortType) {
      return "smallint";
    } else if (type instanceof Types.IntegerType) {
      return "int";
    } else if (type instanceof Types.LongType) {
      return "bigint";
    } else if (type instanceof Types.FloatType) {
      return type.simpleString();
    } else if (type instanceof Types.DoubleType) {
      return type.simpleString();
    } else if (type instanceof Types.StringType) {
      return "text";
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
    throw new IllegalArgumentException("Not a supported type: " + type.toString());
  }
}
