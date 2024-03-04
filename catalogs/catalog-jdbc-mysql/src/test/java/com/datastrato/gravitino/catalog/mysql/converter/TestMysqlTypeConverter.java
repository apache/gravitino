/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.converter;

import static com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter.DATE;
import static com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TEXT;
import static com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TIME;
import static com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TIMESTAMP;
import static com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter.VARCHAR;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.BIGINT;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.BINARY;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.CHAR;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.DECIMAL;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.DOUBLE;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.FLOAT;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.INT;
import static com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter.TINYINT;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link MysqlTypeConverter} */
public class TestMysqlTypeConverter {

  private static final MysqlTypeConverter MYSQL_TYPE_CONVERTER = new MysqlTypeConverter();
  private static final String USER_DEFINED_TYPE = "user-defined";

  @Test
  public void testToGravitinoType() {
    checkJdbcTypeToGravitinoType(Types.ByteType.get(), TINYINT, null, null);
    checkJdbcTypeToGravitinoType(Types.IntegerType.get(), INT, null, null);
    checkJdbcTypeToGravitinoType(Types.LongType.get(), BIGINT, null, null);
    checkJdbcTypeToGravitinoType(Types.FloatType.get(), FLOAT, null, null);
    checkJdbcTypeToGravitinoType(Types.DoubleType.get(), DOUBLE, null, null);
    checkJdbcTypeToGravitinoType(Types.DateType.get(), DATE, null, null);
    checkJdbcTypeToGravitinoType(Types.TimeType.get(), TIME, null, null);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(), TIMESTAMP, null, null);
    checkJdbcTypeToGravitinoType(Types.DecimalType.of(10, 2), DECIMAL, "10", "2");
    checkJdbcTypeToGravitinoType(Types.VarCharType.of(20), VARCHAR, "20", null);
    checkJdbcTypeToGravitinoType(Types.FixedCharType.of(20), CHAR, "20", null);
    checkJdbcTypeToGravitinoType(Types.StringType.get(), TEXT, null, null);
    checkJdbcTypeToGravitinoType(Types.BinaryType.get(), BINARY, null, null);
    checkJdbcTypeToGravitinoType(
        Types.UnparsedType.of(USER_DEFINED_TYPE), USER_DEFINED_TYPE, null, null);
  }

  @Test
  public void testFromGravitinoType() {
    checkGravitinoTypeToJdbcType(TINYINT, Types.ByteType.get());
    checkGravitinoTypeToJdbcType(INT, Types.IntegerType.get());
    checkGravitinoTypeToJdbcType(BIGINT, Types.LongType.get());
    checkGravitinoTypeToJdbcType(FLOAT, Types.FloatType.get());
    checkGravitinoTypeToJdbcType(DOUBLE, Types.DoubleType.get());
    checkGravitinoTypeToJdbcType(DATE, Types.DateType.get());
    checkGravitinoTypeToJdbcType(TIME, Types.TimeType.get());
    checkGravitinoTypeToJdbcType(TIMESTAMP, Types.TimestampType.withoutTimeZone());
    checkGravitinoTypeToJdbcType(DECIMAL + "(10,2)", Types.DecimalType.of(10, 2));
    checkGravitinoTypeToJdbcType(VARCHAR + "(20)", Types.VarCharType.of(20));
    checkGravitinoTypeToJdbcType(CHAR + "(20)", Types.FixedCharType.of(20));
    checkGravitinoTypeToJdbcType(TEXT, Types.StringType.get());
    checkGravitinoTypeToJdbcType(BINARY, Types.BinaryType.get());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MYSQL_TYPE_CONVERTER.fromGravitinoType(Types.UnparsedType.of(USER_DEFINED_TYPE)));
  }

  protected void checkGravitinoTypeToJdbcType(String jdbcTypeName, Type gravitinoType) {
    Assertions.assertEquals(jdbcTypeName, MYSQL_TYPE_CONVERTER.fromGravitinoType(gravitinoType));
  }

  protected void checkJdbcTypeToGravitinoType(
      Type gravitinoType, String jdbcTypeName, String columnSize, String scale) {
    JdbcTypeConverter.JdbcTypeBean typeBean = createTypeBean(jdbcTypeName, columnSize, scale);
    Assertions.assertEquals(gravitinoType, MYSQL_TYPE_CONVERTER.toGravitinoType(typeBean));
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
