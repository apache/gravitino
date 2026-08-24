/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.clickhouse.converter;

import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.BFLOAT16;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.BOOL;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.DATE;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.DATE32;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.DATETIME;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.DATETIME64;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.DECIMAL;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.FIXEDSTRING;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.FLOAT32;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.FLOAT64;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.INT128;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.INT16;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.INT256;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.INT32;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.INT64;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.INT8;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.IPV4;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.IPV6;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.STRING;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.UINT128;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.UINT16;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.UINT256;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.UINT32;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.UINT64;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.UINT8;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.UUID;
import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TIME;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link ClickHouseTypeConverter} */
public class TestClickHouseTypeConverter {

  private static final ClickHouseTypeConverter CLICKHOUSE_TYPE_CONVERTER =
      new ClickHouseTypeConverter();
  private static final String USER_DEFINED_TYPE = "user-defined";

  @Test
  public void testToGravitinoType() {
    checkJdbcTypeToGravitinoType(Types.ByteType.get(), INT8, null, null);
    checkJdbcTypeToGravitinoType(Types.ByteType.unsigned(), UINT8, null, null);
    checkJdbcTypeToGravitinoType(Types.ShortType.get(), INT16, null, null);
    checkJdbcTypeToGravitinoType(Types.ShortType.unsigned(), UINT16, null, null);
    checkJdbcTypeToGravitinoType(Types.IntegerType.get(), INT32, null, null);
    checkJdbcTypeToGravitinoType(Types.IntegerType.unsigned(), UINT32, null, null);
    checkJdbcTypeToGravitinoType(Types.LongType.get(), INT64, null, null);
    checkJdbcTypeToGravitinoType(Types.LongType.unsigned(), UINT64, null, null);
    checkJdbcTypeToGravitinoType(Types.FloatType.get(), FLOAT32, null, null);
    checkJdbcTypeToGravitinoType(Types.DoubleType.get(), FLOAT64, null, null);
    checkJdbcTypeToGravitinoType(Types.DateType.get(), DATE, null, null);
    checkJdbcTypeToGravitinoType(Types.ExternalType.of(DATE32), DATE32, null, null);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(0), DATETIME, null, null);
    checkJdbcTypeToGravitinoType(Types.DecimalType.of(10, 2), DECIMAL, 10, 2);
    checkJdbcTypeToGravitinoType(Types.StringType.get(), STRING, 20, null);
    checkJdbcTypeToGravitinoType(Types.FixedCharType.of(20), FIXEDSTRING, 20, null);
    checkJdbcTypeToGravitinoType(Types.BooleanType.get(), BOOL, 20, null);
    checkJdbcTypeToGravitinoType(Types.UUIDType.get(), UUID, 20, null);
    checkJdbcTypeToGravitinoType(Types.ExternalType.of(BFLOAT16), BFLOAT16, null, null);
    checkJdbcTypeToGravitinoType(Types.ExternalType.of(INT128), INT128, null, null);
    checkJdbcTypeToGravitinoType(Types.ExternalType.of(INT256), INT256, null, null);
    checkJdbcTypeToGravitinoType(Types.ExternalType.of(UINT128), UINT128, null, null);
    checkJdbcTypeToGravitinoType(Types.ExternalType.of(UINT256), UINT256, null, null);
    checkJdbcTypeToGravitinoType(
        Types.ExternalType.of(USER_DEFINED_TYPE), USER_DEFINED_TYPE, null, null);

    JdbcTypeConverter.JdbcTypeBean dateTime64 = createTypeBean(DATETIME64, null, null);
    dateTime64.setDatetimePrecision(3);
    Assertions.assertEquals(
        Types.ExternalType.of("DateTime64"), CLICKHOUSE_TYPE_CONVERTER.toGravitino(dateTime64));

    JdbcTypeConverter.JdbcTypeBean nullableDecimal =
        createTypeBean("Nullable(" + DECIMAL + ")", 12, 2);
    nullableDecimal.setColumnSize(12);
    nullableDecimal.setScale(2);
    Assertions.assertEquals(
        Types.DecimalType.of(12, 2), CLICKHOUSE_TYPE_CONVERTER.toGravitino(nullableDecimal));

    JdbcTypeConverter.JdbcTypeBean date32 = createTypeBean(DATE32, null, null);
    Assertions.assertEquals(
        Types.ExternalType.of(DATE32), CLICKHOUSE_TYPE_CONVERTER.toGravitino(date32));

    // Enum8/Enum16: ClickHouse normalizes Enum to Enum8, so "Enum8('active'=1)" must match
    JdbcTypeConverter.JdbcTypeBean enum8 =
        createTypeBean("Enum8('active'=1,'inactive'=2)", null, null);
    Assertions.assertEquals(
        Types.ExternalType.of("Enum8('active'=1,'inactive'=2)"),
        CLICKHOUSE_TYPE_CONVERTER.toGravitino(enum8));

    JdbcTypeConverter.JdbcTypeBean enum16 = createTypeBean("Enum16('x'=1,'y'=2)", null, null);
    Assertions.assertEquals(
        Types.ExternalType.of("Enum16('x'=1,'y'=2)"),
        CLICKHOUSE_TYPE_CONVERTER.toGravitino(enum16));

    JdbcTypeConverter.JdbcTypeBean ipv4 = createTypeBean("IPv4", null, null);
    Assertions.assertEquals(
        Types.ExternalType.of("IPv4"), CLICKHOUSE_TYPE_CONVERTER.toGravitino(ipv4));

    JdbcTypeConverter.JdbcTypeBean ipv6 = createTypeBean("IPv6", null, null);
    Assertions.assertEquals(
        Types.ExternalType.of("IPv6"), CLICKHOUSE_TYPE_CONVERTER.toGravitino(ipv6));

    // DateTime64(3) should map to TimestampType.withoutTimeZone(3)
    JdbcTypeConverter.JdbcTypeBean dateTime64WithPrecision =
        createTypeBean("DateTime64(3)", null, null);
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(3),
        CLICKHOUSE_TYPE_CONVERTER.toGravitino(dateTime64WithPrecision));

    // LowCardinality(Nullable(String)) should map to StringType
    JdbcTypeConverter.JdbcTypeBean lowCardNullable =
        createTypeBean("LowCardinality(Nullable(String))", null, null);
    Assertions.assertEquals(
        Types.StringType.get(), CLICKHOUSE_TYPE_CONVERTER.toGravitino(lowCardNullable));

    // Decimal(50, 10) should map to ExternalType (not crash, since precision > 38)
    JdbcTypeConverter.JdbcTypeBean decimal50 = createTypeBean("Decimal", 50, 10);
    Assertions.assertEquals(
        Types.ExternalType.of("Decimal(50,10)"), CLICKHOUSE_TYPE_CONVERTER.toGravitino(decimal50));

    JdbcTypeConverter.JdbcTypeBean decimalTooLarge = createTypeBean("Decimal", 77, 2);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CLICKHOUSE_TYPE_CONVERTER.toGravitino(decimalTooLarge));

    JdbcTypeConverter.JdbcTypeBean decimalScaleTooHigh = createTypeBean("Decimal", 10, 20);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CLICKHOUSE_TYPE_CONVERTER.toGravitino(decimalScaleTooHigh));
  }

  @Test
  public void testFromGravitinoType() {
    checkGravitinoTypeToJdbcType(INT8, Types.ByteType.get());
    checkGravitinoTypeToJdbcType(UINT8, Types.ByteType.unsigned());
    checkGravitinoTypeToJdbcType(INT16, Types.ShortType.get());
    checkGravitinoTypeToJdbcType(UINT16, Types.ShortType.unsigned());
    checkGravitinoTypeToJdbcType(INT32, Types.IntegerType.get());
    checkGravitinoTypeToJdbcType(UINT32, Types.IntegerType.unsigned());
    checkGravitinoTypeToJdbcType(INT64, Types.LongType.get());
    checkGravitinoTypeToJdbcType(UINT64, Types.LongType.unsigned());
    checkGravitinoTypeToJdbcType(FLOAT32, Types.FloatType.get());
    checkGravitinoTypeToJdbcType(FLOAT64, Types.DoubleType.get());
    checkGravitinoTypeToJdbcType(DATE, Types.DateType.get());
    checkGravitinoTypeToJdbcType(DATETIME, Types.TimestampType.withoutTimeZone(0));
    checkGravitinoTypeToJdbcType(DECIMAL + "(10,2)", Types.DecimalType.of(10, 2));
    checkGravitinoTypeToJdbcType(STRING, Types.VarCharType.of(20));
    checkGravitinoTypeToJdbcType(FIXEDSTRING + "(20)", Types.FixedCharType.of(20));
    checkGravitinoTypeToJdbcType(STRING, Types.StringType.get());
    checkGravitinoTypeToJdbcType(BOOL, Types.BooleanType.get());
    checkGravitinoTypeToJdbcType(UUID, Types.UUIDType.get());
    checkGravitinoTypeToJdbcType(USER_DEFINED_TYPE, Types.ExternalType.of(USER_DEFINED_TYPE));
    checkGravitinoTypeToJdbcType("DateTime", Types.TimestampType.withoutTimeZone(0));
    // DateTime64(3) round-trip
    checkGravitinoTypeToJdbcType(DATETIME64 + "(3)", Types.TimestampType.withoutTimeZone(3));
    // IPv4/IPv6 round-trip
    checkGravitinoTypeToJdbcType(IPV4, Types.ExternalType.of(IPV4));
    checkGravitinoTypeToJdbcType(IPV6, Types.ExternalType.of(IPV6));
    // Wide integer round-trip (ExternalType passthrough)
    checkGravitinoTypeToJdbcType(INT128, Types.ExternalType.of(INT128));
    checkGravitinoTypeToJdbcType(INT256, Types.ExternalType.of(INT256));
    checkGravitinoTypeToJdbcType(UINT128, Types.ExternalType.of(UINT128));
    checkGravitinoTypeToJdbcType(UINT256, Types.ExternalType.of(UINT256));
    // BFloat16 round-trip (ExternalType passthrough)
    checkGravitinoTypeToJdbcType(BFLOAT16, Types.ExternalType.of(BFLOAT16));
    // Enum8 round-trip (ExternalType passthrough)
    String enum8Type = "Enum8('active'=1,'inactive'=2)";
    checkGravitinoTypeToJdbcType(enum8Type, Types.ExternalType.of(enum8Type));
    // DATE32 round-trip (ExternalType passthrough)
    checkGravitinoTypeToJdbcType(DATE32, Types.ExternalType.of(DATE32));
    checkGravitinoTypeToJdbcType(TIME, Types.TimeType.get());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CLICKHOUSE_TYPE_CONVERTER.fromGravitino(Types.UnparsedType.of(USER_DEFINED_TYPE)));
  }

  protected void checkGravitinoTypeToJdbcType(String jdbcTypeName, Type gravitinoType) {
    Assertions.assertEquals(jdbcTypeName, CLICKHOUSE_TYPE_CONVERTER.fromGravitino(gravitinoType));
  }

  protected void checkJdbcTypeToGravitinoType(
      Type gravitinoType, String jdbcTypeName, Integer columnSize, Integer scale) {
    JdbcTypeConverter.JdbcTypeBean typeBean = createTypeBean(jdbcTypeName, columnSize, scale);
    Assertions.assertEquals(gravitinoType, CLICKHOUSE_TYPE_CONVERTER.toGravitino(typeBean));
  }

  protected static JdbcTypeConverter.JdbcTypeBean createTypeBean(
      String typeName, Integer columnSize, Integer scale) {
    return new JdbcTypeConverter.JdbcTypeBean(typeName) {
      {
        setColumnSize(columnSize);
        setScale(scale);
      }
    };
  }
}
