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
package org.apache.gravitino.catalog.doris.converter;

import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.BIGINT;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.BOOLEAN;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.CHAR;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.DATETIME;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.DECIMAL;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.DOUBLE;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.FLOAT;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.INT;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.SMALLINT;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.STRING;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.TINYINT;
import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.DATE;
import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.TEXT;
import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.VARCHAR;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link org.apache.gravitino.catalog.doris.converter.DorisTypeConverter} */
public class TestDorisTypeConverter {

  private static final DorisTypeConverter DORIS_TYPE_CONVERTER = new DorisTypeConverter();
  private static final String USER_DEFINED_TYPE = "user-defined";

  @Test
  public void testToGravitinoType() {
    checkJdbcTypeToGravitinoType(Types.BooleanType.get(), BOOLEAN, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.ByteType.get(), TINYINT, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.ShortType.get(), SMALLINT, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.IntegerType.get(), INT, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.LongType.get(), BIGINT, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.FloatType.get(), FLOAT, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.DoubleType.get(), DOUBLE, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.DateType.get(), DATE, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(0), DATETIME, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(0), DATETIME, 19, null, 0);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(3), DATETIME, 23, null, 3);
    checkJdbcTypeToGravitinoType(Types.TimestampType.withoutTimeZone(6), DATETIME, 26, null, 6);
    checkJdbcTypeToGravitinoType(Types.DecimalType.of(10, 2), DECIMAL, 10, 2, 0);
    checkJdbcTypeToGravitinoType(Types.VarCharType.of(20), VARCHAR, 20, null, 0);
    checkJdbcTypeToGravitinoType(Types.FixedCharType.of(20), CHAR, 20, null, 0);
    checkJdbcTypeToGravitinoType(Types.StringType.get(), STRING, null, null, 0);
    checkJdbcTypeToGravitinoType(Types.StringType.get(), TEXT, null, null, 0);
    checkJdbcTypeToGravitinoType(
        Types.ExternalType.of(USER_DEFINED_TYPE), USER_DEFINED_TYPE, null, null, 0);
  }

  @Test
  public void testFromGravitinoType() {
    checkGravitinoTypeToJdbcType(BOOLEAN, Types.BooleanType.get());
    checkGravitinoTypeToJdbcType(TINYINT, Types.ByteType.get());
    checkGravitinoTypeToJdbcType(SMALLINT, Types.ShortType.get());
    checkGravitinoTypeToJdbcType(INT, Types.IntegerType.get());
    checkGravitinoTypeToJdbcType(BIGINT, Types.LongType.get());
    checkGravitinoTypeToJdbcType(FLOAT, Types.FloatType.get());
    checkGravitinoTypeToJdbcType(DOUBLE, Types.DoubleType.get());
    checkGravitinoTypeToJdbcType(DATE, Types.DateType.get());
    checkGravitinoTypeToJdbcType(DATETIME, Types.TimestampType.withoutTimeZone());
    checkGravitinoTypeToJdbcType(DECIMAL + "(10,2)", Types.DecimalType.of(10, 2));
    checkGravitinoTypeToJdbcType(VARCHAR + "(20)", Types.VarCharType.of(20));
    checkGravitinoTypeToJdbcType(CHAR + "(20)", Types.FixedCharType.of(20));
    checkGravitinoTypeToJdbcType(STRING, Types.StringType.get());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DORIS_TYPE_CONVERTER.fromGravitino(Types.UnparsedType.of(USER_DEFINED_TYPE)));
  }

  protected void checkGravitinoTypeToJdbcType(String jdbcTypeName, Type gravitinoType) {
    Assertions.assertEquals(jdbcTypeName, DORIS_TYPE_CONVERTER.fromGravitino(gravitinoType));
  }

  protected void checkJdbcTypeToGravitinoType(
      Type gravitinoType,
      String jdbcTypeName,
      Integer columnSize,
      Integer scale,
      Integer datetimePrecision) {
    JdbcTypeConverter.JdbcTypeBean typeBean =
        createTypeBean(jdbcTypeName, columnSize, scale, datetimePrecision);
    Assertions.assertEquals(gravitinoType, DORIS_TYPE_CONVERTER.toGravitino(typeBean));
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
