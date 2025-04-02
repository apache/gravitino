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

import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.BOOL;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.DATE;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.DATE32;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.DATETIME;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.DATETIME64;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.DECIMAL;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.FIXEDSTRING;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.FLOAT32;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.FLOAT64;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.INT16;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.INT32;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.INT64;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.INT8;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.STRING;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.UINT16;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.UINT32;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.UINT64;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.UINT8;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter.UUID;

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
    checkJdbcTypeToGravitinoType(Types.DateType.get(), DATE32, null, null);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(), DATETIME, null, null);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(), DATETIME64, null, null);
    checkJdbcTypeToGravitinoType(Types.DecimalType.of(10, 2), DECIMAL, 10, 2);
    checkJdbcTypeToGravitinoType(Types.StringType.get(), STRING, 20, null);
    checkJdbcTypeToGravitinoType(Types.FixedCharType.of(20), FIXEDSTRING, 20, null);
    checkJdbcTypeToGravitinoType(Types.BooleanType.get(), BOOL, 20, null);
    checkJdbcTypeToGravitinoType(Types.UUIDType.get(), UUID, 20, null);
    checkJdbcTypeToGravitinoType(
        Types.ExternalType.of(USER_DEFINED_TYPE), USER_DEFINED_TYPE, null, null);
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
    checkGravitinoTypeToJdbcType(DATETIME, Types.TimestampType.withoutTimeZone());
    checkGravitinoTypeToJdbcType(DECIMAL + "(10,2)", Types.DecimalType.of(10, 2));
    checkGravitinoTypeToJdbcType(STRING, Types.VarCharType.of(20));
    checkGravitinoTypeToJdbcType(FIXEDSTRING + "(20)", Types.FixedCharType.of(20));
    checkGravitinoTypeToJdbcType(STRING, Types.StringType.get());
    checkGravitinoTypeToJdbcType(BOOL, Types.BooleanType.get());
    checkGravitinoTypeToJdbcType(UUID, Types.UUIDType.get());
    checkGravitinoTypeToJdbcType(USER_DEFINED_TYPE, Types.ExternalType.of(USER_DEFINED_TYPE));
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
