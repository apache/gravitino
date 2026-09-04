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

package org.apache.gravitino.trino.connector.catalog.jdbc.mysql;

import static org.apache.gravitino.trino.connector.catalog.jdbc.mysql.MySQLDataTypeTransformer.JSON_TYPE;

import io.trino.spi.TrinoException;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import java.util.Optional;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMySQLDataTypeTransformer {

  @Test
  public void testTrinoTypeToGravitinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new MySQLDataTypeTransformer();
    io.trino.spi.type.Type charTypeWithLengthOne = io.trino.spi.type.CharType.createCharType(1);
    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(charTypeWithLengthOne),
        Types.FixedCharType.of(1));

    io.trino.spi.type.Type charTypeWithLength = io.trino.spi.type.CharType.createCharType(256);
    Exception e =
        Assertions.assertThrows(
            TrinoException.class,
            () -> generalDataTypeTransformer.getGravitinoType(charTypeWithLength));
    Assertions.assertTrue(
        e.getMessage()
            .contains("MySQL does not support the datatype CHAR with the length greater than 255"));

    io.trino.spi.type.Type varcharType = io.trino.spi.type.VarcharType.createVarcharType(1);
    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharType), Types.VarCharType.of(1));

    io.trino.spi.type.Type varcharTypeWithLength =
        io.trino.spi.type.VarcharType.createVarcharType(16384);
    e =
        Assertions.assertThrows(
            TrinoException.class,
            () -> generalDataTypeTransformer.getGravitinoType(varcharTypeWithLength));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "MySQL does not support the datatype VARCHAR with the length greater than 16383"));

    io.trino.spi.type.Type varcharTypeWithLength2 =
        io.trino.spi.type.VarcharType.createUnboundedVarcharType();
    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharTypeWithLength2),
        Types.StringType.get());
  }

  @Test
  public void testGravitinoCharToTrinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new MySQLDataTypeTransformer();

    Type stringType = Types.StringType.get();
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(stringType),
        io.trino.spi.type.VarcharType.createUnboundedVarcharType());
  }

  @Test
  public void testGravitinoIntegerToTrinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new MySQLDataTypeTransformer();

    Type integerType = Types.IntegerType.get();
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(integerType),
        io.trino.spi.type.IntegerType.INTEGER);

    Type unsignedIntegerType = Types.IntegerType.unsigned();
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(unsignedIntegerType),
        io.trino.spi.type.BigintType.BIGINT);

    Type bigintType = Types.LongType.get();
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(bigintType), io.trino.spi.type.BigintType.BIGINT);

    Type unsignBigintType = Types.LongType.unsigned();
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(unsignBigintType),
        io.trino.spi.type.DecimalType.createDecimalType(20, 0));
  }

  @Test
  public void testGravitinoTimestampToTrinoType() {
    GeneralDataTypeTransformer transformer = new MySQLDataTypeTransformer();

    // MySQL DATETIME/TIMESTAMP default to a fractional seconds precision of 0.
    Assertions.assertEquals(
        TimestampType.TIMESTAMP_SECONDS,
        transformer.getTrinoType(Types.TimestampType.withoutTimeZone()));
    Assertions.assertEquals(
        TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS,
        transformer.getTrinoType(Types.TimestampType.withTimeZone()));

    // The Trino type must keep the precision of the MySQL column, otherwise reading a
    // DATETIME(3)/TIMESTAMP(6) column fails with "Expected 0s for digits beyond precision 0".
    for (int precision = 0; precision <= 6; precision++) {
      Assertions.assertEquals(
          TimestampType.createTimestampType(precision),
          transformer.getTrinoType(Types.TimestampType.withoutTimeZone(precision)));
      Assertions.assertEquals(
          TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision),
          transformer.getTrinoType(Types.TimestampType.withTimeZone(precision)));
    }

    // MySQL supports at most microseconds precision.
    Assertions.assertEquals(
        TimestampType.TIMESTAMP_MICROS,
        transformer.getTrinoType(Types.TimestampType.withoutTimeZone(9)));
    Assertions.assertEquals(
        TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS,
        transformer.getTrinoType(Types.TimestampType.withTimeZone(12)));
  }

  @Test
  public void testGravitinoTimeToTrinoType() {
    GeneralDataTypeTransformer transformer = new MySQLDataTypeTransformer();

    // MySQL TIME defaults to a fractional seconds precision of 0.
    Assertions.assertEquals(TimeType.TIME_SECONDS, transformer.getTrinoType(Types.TimeType.get()));

    for (int precision = 0; precision <= 6; precision++) {
      Assertions.assertEquals(
          TimeType.createTimeType(precision),
          transformer.getTrinoType(Types.TimeType.of(precision)));
    }

    // MySQL supports at most microseconds precision.
    Assertions.assertEquals(TimeType.TIME_MICROS, transformer.getTrinoType(Types.TimeType.of(12)));
  }

  @Test
  public void testTrinoTimestampToGravitinoType() {
    GeneralDataTypeTransformer transformer = new MySQLDataTypeTransformer();

    // Same as the native Trino MySQL connector: the precision is kept, DATETIME(p)/TIMESTAMP(p)
    for (int precision = 0; precision <= 6; precision++) {
      Assertions.assertEquals(
          Types.TimestampType.withoutTimeZone(precision),
          transformer.getGravitinoType(TimestampType.createTimestampType(precision)));
      Assertions.assertEquals(
          Types.TimestampType.withTimeZone(precision),
          transformer.getGravitinoType(
              TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision)));
    }

    // MySQL supports at most microseconds precision.
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(6),
        transformer.getGravitinoType(TimestampType.TIMESTAMP_NANOS));
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(6),
        transformer.getGravitinoType(TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS));

    // Round trip keeps the precision, so a table created through Trino reads back with the same
    // types.
    Assertions.assertEquals(
        TimestampType.TIMESTAMP_MICROS,
        transformer.getTrinoType(transformer.getGravitinoType(TimestampType.TIMESTAMP_MICROS)));
    Assertions.assertEquals(
        TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS,
        transformer.getTrinoType(
            transformer.getGravitinoType(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS)));
  }

  @Test
  public void testTrinoTimeToGravitinoType() {
    GeneralDataTypeTransformer transformer = new MySQLDataTypeTransformer();

    for (int precision = 0; precision <= 6; precision++) {
      Assertions.assertEquals(
          Types.TimeType.of(precision),
          transformer.getGravitinoType(TimeType.createTimeType(precision)));
    }

    // MySQL supports at most microseconds precision.
    Assertions.assertEquals(
        Types.TimeType.of(6), transformer.getGravitinoType(TimeType.TIME_NANOS));

    Assertions.assertEquals(
        TimeType.TIME_MICROS,
        transformer.getTrinoType(transformer.getGravitinoType(TimeType.TIME_MICROS)));
  }

  @Test
  public void testSupportedType() {
    GeneralDataTypeTransformer transformer = new MySQLDataTypeTransformer();

    // Up to microseconds the requested type is used as is.
    Assertions.assertEquals(
        Optional.empty(), transformer.getSupportedType(TimestampType.TIMESTAMP_MICROS));
    Assertions.assertEquals(
        Optional.empty(),
        transformer.getSupportedType(TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS));
    Assertions.assertEquals(Optional.empty(), transformer.getSupportedType(TimeType.TIME_MILLIS));
    Assertions.assertEquals(
        Optional.empty(), transformer.getSupportedType(VarcharType.createVarcharType(10)));

    // Above microseconds the column is created with precision 6 and the values are coerced.
    Assertions.assertEquals(
        Optional.of(TimestampType.TIMESTAMP_MICROS),
        transformer.getSupportedType(TimestampType.TIMESTAMP_NANOS));
    Assertions.assertEquals(
        Optional.of(TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS),
        transformer.getSupportedType(TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS));
    Assertions.assertEquals(
        Optional.of(TimeType.TIME_MICROS), transformer.getSupportedType(TimeType.TIME_NANOS));
  }

  @Test
  public void testGravitinoExternalTypeToTrinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new MySQLDataTypeTransformer();

    Type mediumintType = Types.ExternalType.of("mediumint");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(mediumintType), IntegerType.INTEGER);

    Type mediumintUnsignedType = Types.ExternalType.of("mediumint unsigned");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(mediumintUnsignedType), IntegerType.INTEGER);

    Type floatUnsignedType = Types.ExternalType.of("float unsigned");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(floatUnsignedType), RealType.REAL);

    Type doubleUnsignedType = Types.ExternalType.of("double unsigned");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(doubleUnsignedType), DoubleType.DOUBLE);

    Type tinytextType = Types.ExternalType.of("tinytext");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(tinytextType), VarcharType.VARCHAR);

    Type mediumtextType = Types.ExternalType.of("mediumtext");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(mediumtextType), VarcharType.VARCHAR);

    Type longtextType = Types.ExternalType.of("longtext");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(longtextType), VarcharType.VARCHAR);

    Type yearType = Types.ExternalType.of("year");
    Assertions.assertEquals(generalDataTypeTransformer.getTrinoType(yearType), DateType.DATE);

    Type enumType = Types.ExternalType.of("enum");
    Assertions.assertEquals(generalDataTypeTransformer.getTrinoType(enumType), VarcharType.VARCHAR);

    Type setType = Types.ExternalType.of("set");
    Assertions.assertEquals(generalDataTypeTransformer.getTrinoType(setType), VarcharType.VARCHAR);

    Type jsonType = Types.ExternalType.of("json");
    Assertions.assertEquals(generalDataTypeTransformer.getTrinoType(jsonType), JSON_TYPE);

    Type varbinaryType = Types.ExternalType.of("varbinary");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(varbinaryType), VarbinaryType.VARBINARY);

    Type tinyblobType = Types.ExternalType.of("tinyblob");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(tinyblobType), VarbinaryType.VARBINARY);

    Type mediumblobType = Types.ExternalType.of("mediumblob");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(mediumblobType), VarbinaryType.VARBINARY);

    Type longblobType = Types.ExternalType.of("longblob");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(longblobType), VarbinaryType.VARBINARY);

    Type geometryType = Types.ExternalType.of("geometry");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(geometryType), VarbinaryType.VARBINARY);

    Type unknownType = Types.ExternalType.of("unknown");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(unknownType), VarcharType.VARCHAR);
  }
}
