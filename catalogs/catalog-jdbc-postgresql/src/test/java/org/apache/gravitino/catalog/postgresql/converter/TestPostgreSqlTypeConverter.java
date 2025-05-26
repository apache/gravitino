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
package org.apache.gravitino.catalog.postgresql.converter;

import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.DATE;
import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TEXT;
import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TIME;
import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TIMESTAMP;
import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.VARCHAR;
import static org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.ARRAY_TOKEN;
import static org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.BOOL;
import static org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.BPCHAR;
import static org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.BYTEA;
import static org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.FLOAT_4;
import static org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.FLOAT_8;
import static org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.INT_2;
import static org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.INT_4;
import static org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.INT_8;
import static org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.JDBC_ARRAY_PREFIX;
import static org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter.NUMERIC;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter}.
 */
public class TestPostgreSqlTypeConverter {

  private static final PostgreSqlTypeConverter POSTGRE_SQL_TYPE_CONVERTER =
      new PostgreSqlTypeConverter();
  private static final String USER_DEFINED_TYPE = "user-defined";

  @Test
  public void testToGravitinoType() {
    checkJdbcTypeToGravitinoType(Types.BooleanType.get(), BOOL, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.ShortType.get(), INT_2, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.IntegerType.get(), INT_4, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.LongType.get(), INT_8, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.FloatType.get(), FLOAT_4, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.DoubleType.get(), FLOAT_8, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.DateType.get(), DATE, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimeType.of(0), TIME, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimeType.of(0), TIME, 8, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimeType.of(3), TIME, 12, null, 3);
    checkJdbcTypeToGravitinoType(Types.TimeType.of(6), TIME, 15, null, 6);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(0), TIMESTAMP, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(0), TIMESTAMP, 19, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(3), TIMESTAMP, 23, null, 3);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(6), TIMESTAMP, 26, null, 6);
    checkJdbcTypeToGravitinoType(Types.DecimalType.of(10, 2), NUMERIC, 10, 2, 0);
    checkJdbcTypeToGravitinoType(Types.VarCharType.of(20), VARCHAR, 20, null, 0);
    checkJdbcTypeToGravitinoType(Types.FixedCharType.of(20), BPCHAR, 20, null, 0);
    checkJdbcTypeToGravitinoType(Types.StringType.get(), TEXT, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.BinaryType.get(), BYTEA, null, null, 0);
    checkJdbcTypeToGravitinoType(
        Types.ExternalType.of(USER_DEFINED_TYPE), USER_DEFINED_TYPE, null, null, 0);
  }

  @Test
  public void testArrayType() {
    Type elmentType = Types.IntegerType.get();
    Type list1 = Types.ListType.of(elmentType, false);

    checkGravitinoTypeToJdbcType(INT_4 + ARRAY_TOKEN, list1);
    checkJdbcTypeToGravitinoType(list1, JDBC_ARRAY_PREFIX + INT_4, null, null, 0);

    // not support element nullable
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            checkGravitinoTypeToJdbcType(INT_4 + ARRAY_TOKEN, Types.ListType.of(elmentType, true)));

    // not support multidimensional
    Type list2 = Types.ListType.of(list1, false);
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> checkGravitinoTypeToJdbcType(INT_4 + ARRAY_TOKEN, list2));
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
    checkGravitinoTypeToJdbcType(USER_DEFINED_TYPE, Types.ExternalType.of(USER_DEFINED_TYPE));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> POSTGRE_SQL_TYPE_CONVERTER.fromGravitino(Types.UnparsedType.of(USER_DEFINED_TYPE)));
  }

  protected void checkGravitinoTypeToJdbcType(String jdbcTypeName, Type gravitinoType) {
    Assertions.assertEquals(jdbcTypeName, POSTGRE_SQL_TYPE_CONVERTER.fromGravitino(gravitinoType));
  }

  protected void checkJdbcTypeToGravitinoType(
      Type gravitinoType,
      String jdbcTypeName,
      Integer columnSize,
      Integer scale,
      Integer datetimePrecision) {
    JdbcTypeConverter.JdbcTypeBean typeBean =
        createTypeBean(jdbcTypeName, columnSize, scale, datetimePrecision);
    Assertions.assertEquals(gravitinoType, POSTGRE_SQL_TYPE_CONVERTER.toGravitino(typeBean));
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
