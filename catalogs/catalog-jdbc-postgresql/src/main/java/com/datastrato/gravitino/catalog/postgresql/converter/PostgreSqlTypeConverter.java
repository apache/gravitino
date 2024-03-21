/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql.converter;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;

public class PostgreSqlTypeConverter extends JdbcTypeConverter<String> {

  static final String BOOL = "bool";
  static final String INT_2 = "int2";
  static final String INT_4 = "int4";
  static final String INT_8 = "int8";
  static final String FLOAT_4 = "float4";
  static final String FLOAT_8 = "float8";

  static final String TIMESTAMP_TZ = "timestamptz";
  static final String NUMERIC = "numeric";
  static final String BPCHAR = "bpchar";
  static final String BYTEA = "bytea";

  @Override
  public Type toGravitinoType(JdbcTypeBean typeBean) {
    // TODO #947 Complex types are not considered for support in this issue, which will bring more
    // testing needs
    switch (typeBean.getTypeName().toLowerCase()) {
      case BOOL:
        return Types.BooleanType.get();
      case INT_2:
        return Types.ShortType.get();
      case INT_4:
        return Types.IntegerType.get();
      case INT_8:
        return Types.LongType.get();
      case FLOAT_4:
        return Types.FloatType.get();
      case FLOAT_8:
        return Types.DoubleType.get();
      case DATE:
        return Types.DateType.get();
      case TIME:
        return Types.TimeType.get();
      case TIMESTAMP:
        return Types.TimestampType.withoutTimeZone();
      case TIMESTAMP_TZ:
        return Types.TimestampType.withTimeZone();
      case NUMERIC:
        return Types.DecimalType.of(
            Integer.parseInt(typeBean.getColumnSize()), Integer.parseInt(typeBean.getScale()));
      case VARCHAR:
        return Types.VarCharType.of(Integer.parseInt(typeBean.getColumnSize()));
      case BPCHAR:
        return Types.FixedCharType.of(Integer.parseInt(typeBean.getColumnSize()));
      case TEXT:
        return Types.StringType.get();
      case BYTEA:
        return Types.BinaryType.get();
      default:
        return Types.UnparsedType.of(typeBean.getTypeName());
    }
  }

  @Override
  public String fromGravitinoType(Type type) {
    if (type instanceof Types.BooleanType) {
      return BOOL;
    } else if (type instanceof Types.ShortType) {
      return INT_2;
    } else if (type instanceof Types.IntegerType) {
      return INT_4;
    } else if (type instanceof Types.LongType) {
      return INT_8;
    } else if (type instanceof Types.FloatType) {
      return FLOAT_4;
    } else if (type instanceof Types.DoubleType) {
      return FLOAT_8;
    } else if (type instanceof Types.StringType) {
      return TEXT;
    } else if (type instanceof Types.DateType) {
      return type.simpleString();
    } else if (type instanceof Types.TimeType) {
      return type.simpleString();
    } else if (type instanceof Types.TimestampType && !((Types.TimestampType) type).hasTimeZone()) {
      return TIMESTAMP;
    } else if (type instanceof Types.TimestampType && ((Types.TimestampType) type).hasTimeZone()) {
      return TIMESTAMP_TZ;
    } else if (type instanceof Types.DecimalType) {
      return NUMERIC
          + "("
          + ((Types.DecimalType) type).precision()
          + ","
          + ((Types.DecimalType) type).scale()
          + ")";
    } else if (type instanceof Types.VarCharType) {
      return VARCHAR + "(" + ((Types.VarCharType) type).length() + ")";
    } else if (type instanceof Types.FixedCharType) {
      return BPCHAR + "(" + ((Types.FixedCharType) type).length() + ")";
    } else if (type instanceof Types.BinaryType) {
      return BYTEA;
    }
    throw new IllegalArgumentException(
        String.format(
            "Couldn't convert Gravitino type %s to PostgreSQL type", type.simpleString()));
  }
}
