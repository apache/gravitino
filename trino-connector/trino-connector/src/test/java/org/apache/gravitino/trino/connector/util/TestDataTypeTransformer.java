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
package org.apache.gravitino.trino.connector.util;

import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE;
import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_TRINO_DATATYPE;

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.HyperLogLogType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;
import java.util.Optional;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

public class TestDataTypeTransformer {

  GeneralDataTypeTransformer dataTypeTransformer = new GeneralDataTypeTransformer();

  @Test
  public void testGetGravitinoType() {
    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(BooleanType.BOOLEAN), Types.BooleanType.get());

    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(TinyintType.TINYINT), Types.ByteType.get());
    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(SmallintType.SMALLINT), Types.ShortType.get());
    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(IntegerType.INTEGER), Types.IntegerType.get());
    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(BigintType.BIGINT), Types.LongType.get());

    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(RealType.REAL), Types.FloatType.get());
    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(DoubleType.DOUBLE), Types.DoubleType.get());
    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(DecimalType.createDecimalType(10, 2)),
        Types.DecimalType.of(10, 2));

    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(CharType.createCharType(10)),
        Types.FixedCharType.of(10));
    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(VarcharType.createVarcharType(10)),
        Types.VarCharType.of(10));
    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(VarcharType.createVarcharType(VarcharType.MAX_LENGTH)),
        Types.VarCharType.of(Integer.MAX_VALUE - 1));

    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(DateType.DATE), Types.DateType.get());
    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(TimeType.TIME_MILLIS), Types.TimeType.of(3));
    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(TimestampType.TIMESTAMP_MILLIS),
        Types.TimestampType.withoutTimeZone(3));
    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS),
        Types.TimestampType.withTimeZone(3));

    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(new ArrayType(IntegerType.INTEGER)),
        Types.ListType.nullable(Types.IntegerType.get()));
    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(
            new MapType(SmallintType.SMALLINT, IntegerType.INTEGER, new TypeOperators())),
        Types.MapType.of(Types.ShortType.get(), Types.IntegerType.get(), true));

    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(
            RowType.from(
                ImmutableList.of(
                    new Field(Optional.of("a"), IntegerType.INTEGER),
                    new Field(Optional.of("b"), VarcharType.createVarcharType(10))))),
        Types.StructType.of(
            Types.StructType.Field.nullableField("a", Types.IntegerType.get()),
            Types.StructType.Field.nullableField("b", Types.VarCharType.of(10))));

    Assertions.assertEquals(
        dataTypeTransformer.getGravitinoType(UuidType.UUID), Types.UUIDType.get());

    try {
      dataTypeTransformer.getGravitinoType(HyperLogLogType.HYPER_LOG_LOG);
    } catch (TrinoException e) {
      if (!GRAVITINO_UNSUPPORTED_TRINO_DATATYPE.toErrorCode().equals(e.getErrorCode())) {
        throw e;
      }
    }
  }

  @Test
  public void testGetTrinoType() {
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.BooleanType.get()), BooleanType.BOOLEAN);

    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.ByteType.get()), TinyintType.TINYINT);
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.ShortType.get()), SmallintType.SMALLINT);
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.IntegerType.get()), IntegerType.INTEGER);
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.LongType.get()), BigintType.BIGINT);

    Assertions.assertEquals(dataTypeTransformer.getTrinoType(Types.FloatType.get()), RealType.REAL);
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.DoubleType.get()), DoubleType.DOUBLE);
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.DecimalType.of(10, 2)),
        DecimalType.createDecimalType(10, 2));

    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.ByteType.unsigned()), SmallintType.SMALLINT);
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.ShortType.unsigned()), IntegerType.INTEGER);
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.IntegerType.unsigned()), BigintType.BIGINT);
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.LongType.unsigned()),
        DecimalType.createDecimalType(20, 0));

    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.FixedCharType.of(10)), CharType.createCharType(10));
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.VarCharType.of(10)),
        VarcharType.createVarcharType(10));
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.VarCharType.of(Integer.MAX_VALUE - 1)),
        VarcharType.createVarcharType(VarcharType.MAX_LENGTH));

    Assertions.assertEquals(dataTypeTransformer.getTrinoType(Types.DateType.get()), DateType.DATE);
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.TimeType.get()), TimeType.TIME_MILLIS);
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.TimestampType.withoutTimeZone()),
        TimestampType.TIMESTAMP_MILLIS);
    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.TimestampType.withTimeZone()),
        TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS);

    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(Types.ListType.nullable(Types.IntegerType.get())),
        new ArrayType(IntegerType.INTEGER));

    Assertions.assertEquals(
        Types.MapType.of(Types.ShortType.get(), Types.IntegerType.get(), true),
        dataTypeTransformer.getGravitinoType(
            new MapType(SmallintType.SMALLINT, IntegerType.INTEGER, new TypeOperators())));

    Assertions.assertEquals(
        dataTypeTransformer.getTrinoType(
            Types.StructType.of(
                Types.StructType.Field.nullableField("a", Types.IntegerType.get()),
                Types.StructType.Field.nullableField("b", Types.VarCharType.of(10)))),
        RowType.from(
            ImmutableList.of(
                new Field(Optional.of("a"), IntegerType.INTEGER),
                new Field(Optional.of("b"), VarcharType.createVarcharType(10)))));

    Assertions.assertEquals(dataTypeTransformer.getTrinoType(Types.UUIDType.get()), UuidType.UUID);

    try {
      dataTypeTransformer.getTrinoType(Types.BinaryType.get());
    } catch (TrinoException e) {
      if (!GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE.toErrorCode().equals(e.getErrorCode())) {
        throw e;
      }
    }
  }
}
