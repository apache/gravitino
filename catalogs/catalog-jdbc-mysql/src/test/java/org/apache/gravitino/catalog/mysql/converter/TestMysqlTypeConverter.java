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
package org.apache.gravitino.catalog.mysql.converter;

import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.DATE;
import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TEXT;
import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TIME;
import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TIMESTAMP;
import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.VARCHAR;
import static org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter.BIGINT;
import static org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter.BINARY;
import static org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter.CHAR;
import static org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter.DATETIME;
import static org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter.DECIMAL;
import static org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter.DECIMAL_UNSIGNED;
import static org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter.DOUBLE;
import static org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter.FLOAT;
import static org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter.INT;
import static org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter.TINYINT;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter} */
public class TestMysqlTypeConverter {

  private static final MysqlTypeConverter MYSQL_TYPE_CONVERTER = new MysqlTypeConverter();
  private static final String USER_DEFINED_TYPE = "user-defined";

  @Test
  public void testToGravitinoType() {
    checkJdbcTypeToGravitinoType(Types.ByteType.get(), TINYINT, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.IntegerType.get(), INT, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.LongType.get(), BIGINT, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.FloatType.get(), FLOAT, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.DoubleType.get(), DOUBLE, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.DateType.get(), DATE, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimeType.get(), TIME, null, null, null);
    checkJdbcTypeToGravitinoType(Types.TimeType.of(0), TIME, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimeType.of(0), TIME, 8, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimeType.of(3), TIME, 12, null, 3);
    checkJdbcTypeToGravitinoType(Types.TimeType.of(6), TIME, 15, null, 6);
    checkJdbcTypeToGravitinoType(Types.TimeType.of(9), TIME, 18, null, 9);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(), DATETIME, null, null, null);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(0), DATETIME, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(0), DATETIME, 19, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(3), DATETIME, 23, null, 3);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(6), DATETIME, 26, null, 6);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(9), DATETIME, 29, null, 9);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withTimeZone(), TIMESTAMP, null, null, null);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withTimeZone(0), TIMESTAMP, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withTimeZone(0), TIMESTAMP, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withTimeZone(3), TIMESTAMP, 23, null, 3);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withTimeZone(6), TIMESTAMP, 26, null, 6);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withTimeZone(9), TIMESTAMP, 29, null, 9);
    checkJdbcTypeToGravitinoType(Types.DecimalType.of(10, 2), DECIMAL, 10, 2, 0);
    checkJdbcTypeToGravitinoType(Types.DecimalType.of(10, 2), DECIMAL_UNSIGNED, 10, 2, 0);
    checkJdbcTypeToGravitinoType(Types.VarCharType.of(20), VARCHAR, 20, null, 0);
    checkJdbcTypeToGravitinoType(Types.FixedCharType.of(20), CHAR, 20, null, 0);
    checkJdbcTypeToGravitinoType(Types.StringType.get(), TEXT, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.BinaryType.get(), BINARY, null, null, 0);
    checkJdbcTypeToGravitinoType(
        Types.ExternalType.of(USER_DEFINED_TYPE), USER_DEFINED_TYPE, null, null, 0);
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
    checkGravitinoTypeToJdbcType(TIMESTAMP, Types.TimestampType.withTimeZone());
    checkGravitinoTypeToJdbcType(DATETIME, Types.TimestampType.withoutTimeZone());
    checkGravitinoTypeToJdbcType(DECIMAL + "(10,2)", Types.DecimalType.of(10, 2));
    checkGravitinoTypeToJdbcType(VARCHAR + "(20)", Types.VarCharType.of(20));
    checkGravitinoTypeToJdbcType(CHAR + "(20)", Types.FixedCharType.of(20));
    checkGravitinoTypeToJdbcType(TEXT, Types.StringType.get());
    checkGravitinoTypeToJdbcType(BINARY, Types.BinaryType.get());
    checkGravitinoTypeToJdbcType(USER_DEFINED_TYPE, Types.ExternalType.of(USER_DEFINED_TYPE));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MYSQL_TYPE_CONVERTER.fromGravitino(Types.UnparsedType.of(USER_DEFINED_TYPE)));
  }

  protected void checkGravitinoTypeToJdbcType(String jdbcTypeName, Type gravitinoType) {
    Assertions.assertEquals(jdbcTypeName, MYSQL_TYPE_CONVERTER.fromGravitino(gravitinoType));
  }

  protected void checkJdbcTypeToGravitinoType(
      Type gravitinoType,
      String jdbcTypeName,
      Integer columnSize,
      Integer scale,
      Integer datetimePrecision) {
    JdbcTypeConverter.JdbcTypeBean typeBean =
        createTypeBean(jdbcTypeName, columnSize, scale, datetimePrecision);
    Assertions.assertEquals(gravitinoType, MYSQL_TYPE_CONVERTER.toGravitino(typeBean));
  }

  protected static JdbcTypeConverter.JdbcTypeBean createTypeBean(
      String typeName, Integer columnSize, Integer scale, Integer datetimePrecision) {
    return new JdbcTypeConverter.JdbcTypeBean(typeName) {
      {
        setColumnSize(columnSize);
        setScale(scale);
        setDatetimePrecision(datetimePrecision);
      }
    };
  }
}
