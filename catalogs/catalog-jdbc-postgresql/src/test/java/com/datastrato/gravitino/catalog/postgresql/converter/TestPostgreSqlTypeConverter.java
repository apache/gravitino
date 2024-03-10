/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql.converter;

import static com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter.DATE;
import static com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TEXT;
import static com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TIME;
import static com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TIMESTAMP;
import static com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter.VARCHAR;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.BOOL;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.BPCHAR;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.BYTEA;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.FLOAT_4;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.FLOAT_8;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.INT_2;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.INT_4;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.INT_8;
import static com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.NUMERIC;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit test for {@link PostgreSqlTypeConverter}. */
public class TestPostgreSqlTypeConverter {

  private static final PostgreSqlTypeConverter POSTGRE_SQL_TYPE_CONVERTER =
      new PostgreSqlTypeConverter();
  private static final String USER_DEFINED_TYPE = "user-defined";

  @Test
  public void testToGravitinoType() {
    checkJdbcTypeToGravitinoType(Types.BooleanType.get(), BOOL, null, null);
    checkJdbcTypeToGravitinoType(Types.ShortType.get(), INT_2, null, null);
    checkJdbcTypeToGravitinoType(Types.IntegerType.get(), INT_4, null, null);
    checkJdbcTypeToGravitinoType(Types.LongType.get(), INT_8, null, null);
    checkJdbcTypeToGravitinoType(Types.FloatType.get(), FLOAT_4, null, null);
    checkJdbcTypeToGravitinoType(Types.DoubleType.get(), FLOAT_8, null, null);
    checkJdbcTypeToGravitinoType(Types.DateType.get(), DATE, null, null);
    checkJdbcTypeToGravitinoType(Types.TimeType.get(), TIME, null, null);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(), TIMESTAMP, null, null);
    checkJdbcTypeToGravitinoType(Types.DecimalType.of(10, 2), NUMERIC, "10", "2");
    checkJdbcTypeToGravitinoType(Types.VarCharType.of(20), VARCHAR, "20", null);
    checkJdbcTypeToGravitinoType(Types.FixedCharType.of(20), BPCHAR, "20", null);
    checkJdbcTypeToGravitinoType(Types.StringType.get(), TEXT, null, null);
    checkJdbcTypeToGravitinoType(Types.BinaryType.get(), BYTEA, null, null);
    checkJdbcTypeToGravitinoType(
        Types.UnparsedType.of(USER_DEFINED_TYPE), USER_DEFINED_TYPE, null, null);
  }

  @Test
  public void testFromGravitinoType() {
    checkGravitinoTypeToJdbcType(BOOL, Types.BooleanType.get());
    checkGravitinoTypeToJdbcType(INT_2, Types.ShortType.get());
    checkGravitinoTypeToJdbcType(INT_4, Types.IntegerType.get());
    checkGravitinoTypeToJdbcType(INT_8, Types.LongType.get());
    checkGravitinoTypeToJdbcType(FLOAT_4, Types.FloatType.get());
    checkGravitinoTypeToJdbcType(FLOAT_8, Types.DoubleType.get());
    checkGravitinoTypeToJdbcType(DATE, Types.DateType.get());
    checkGravitinoTypeToJdbcType(TIME, Types.TimeType.get());
    checkGravitinoTypeToJdbcType(TIMESTAMP, Types.TimestampType.withoutTimeZone());
    checkGravitinoTypeToJdbcType(NUMERIC + "(10,2)", Types.DecimalType.of(10, 2));
    checkGravitinoTypeToJdbcType(VARCHAR + "(20)", Types.VarCharType.of(20));
    checkGravitinoTypeToJdbcType(BPCHAR + "(20)", Types.FixedCharType.of(20));
    checkGravitinoTypeToJdbcType(TEXT, Types.StringType.get());
    checkGravitinoTypeToJdbcType(BYTEA, Types.BinaryType.get());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            POSTGRE_SQL_TYPE_CONVERTER.fromGravitinoType(Types.UnparsedType.of(USER_DEFINED_TYPE)));
  }

  protected void checkGravitinoTypeToJdbcType(String jdbcTypeName, Type gravitinoType) {
    Assertions.assertEquals(
        jdbcTypeName, POSTGRE_SQL_TYPE_CONVERTER.fromGravitinoType(gravitinoType));
  }

  protected void checkJdbcTypeToGravitinoType(
      Type gravitinoType, String jdbcTypeName, String columnSize, String scale) {
    JdbcTypeConverter.JdbcTypeBean typeBean = createTypeBean(jdbcTypeName, columnSize, scale);
    Assertions.assertEquals(gravitinoType, POSTGRE_SQL_TYPE_CONVERTER.toGravitinoType(typeBean));
  }

  protected static JdbcTypeConverter.JdbcTypeBean createTypeBean(
      String typeName, String columnSize, String scale) {
    return new JdbcTypeConverter.JdbcTypeBean(typeName) {
      {
        setColumnSize(columnSize);
        setScale(scale);
      }
    };
  }
}
