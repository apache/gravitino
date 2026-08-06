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

package org.apache.gravitino.catalog.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

class TestFlussDataTypeConverter {

  private final FlussDataTypeConverter converter = FlussDataTypeConverter.CONVERTER;

  @Test
  void testFromGravitinoPrimitiveTypes() {
    assertEquals(DataTypes.BOOLEAN(), converter.fromGravitino(Types.BooleanType.get()));
    assertEquals(DataTypes.TINYINT(), converter.fromGravitino(Types.ByteType.get()));
    assertEquals(DataTypes.SMALLINT(), converter.fromGravitino(Types.ShortType.get()));
    assertEquals(DataTypes.INT(), converter.fromGravitino(Types.IntegerType.get()));
    assertEquals(DataTypes.BIGINT(), converter.fromGravitino(Types.LongType.get()));
    assertEquals(DataTypes.FLOAT(), converter.fromGravitino(Types.FloatType.get()));
    assertEquals(DataTypes.DOUBLE(), converter.fromGravitino(Types.DoubleType.get()));
    assertEquals(DataTypes.DECIMAL(12, 2), converter.fromGravitino(Types.DecimalType.of(12, 2)));
    assertEquals(DataTypes.DATE(), converter.fromGravitino(Types.DateType.get()));
    assertEquals(DataTypes.TIME(), converter.fromGravitino(Types.TimeType.get()));
    assertEquals(DataTypes.TIME(3), converter.fromGravitino(Types.TimeType.of(3)));
    assertEquals(
        DataTypes.TIMESTAMP(), converter.fromGravitino(Types.TimestampType.withoutTimeZone()));
    assertEquals(
        DataTypes.TIMESTAMP(6), converter.fromGravitino(Types.TimestampType.withoutTimeZone(6)));
    assertEquals(
        DataTypes.TIMESTAMP_LTZ(), converter.fromGravitino(Types.TimestampType.withTimeZone()));
    assertEquals(
        DataTypes.TIMESTAMP_LTZ(3), converter.fromGravitino(Types.TimestampType.withTimeZone(3)));
    assertEquals(DataTypes.STRING(), converter.fromGravitino(Types.StringType.get()));
    assertEquals(DataTypes.STRING(), converter.fromGravitino(Types.VarCharType.of(32)));
    assertEquals(DataTypes.CHAR(4), converter.fromGravitino(Types.FixedCharType.of(4)));
    assertEquals(DataTypes.BINARY(8), converter.fromGravitino(Types.FixedType.of(8)));
    assertEquals(DataTypes.BYTES(), converter.fromGravitino(Types.BinaryType.get()));

    DataType notNull = converter.fromGravitino(Types.IntegerType.get(), false);
    assertFalse(notNull.isNullable());
  }

  @Test
  void testFromGravitinoComplexTypes() {
    DataType array = converter.fromGravitino(Types.ListType.of(Types.IntegerType.get(), false));
    assertEquals(DataTypes.ARRAY(DataTypes.INT().copy(false)), array);

    DataType map =
        converter.fromGravitino(
            Types.MapType.of(Types.StringType.get(), Types.LongType.get(), false));
    assertInstanceOf(MapType.class, map);
    assertEquals(DataTypes.STRING().copy(false), ((MapType) map).getKeyType());
    assertEquals(DataTypes.BIGINT().copy(false), ((MapType) map).getValueType());

    DataType row =
        converter.fromGravitino(
            Types.StructType.of(
                Types.StructType.Field.of("id", Types.LongType.get(), false, "primary identifier"),
                Types.StructType.Field.of("name", Types.StringType.get(), true, "name")));
    RowType rowType = assertInstanceOf(RowType.class, row);
    assertEquals(2, rowType.getFields().size());
    DataField id = rowType.getFields().get(0);
    assertEquals("id", id.getName());
    assertEquals(DataTypes.BIGINT().copy(false), id.getType());
    assertEquals("primary identifier", id.getDescription().orElse(null));
  }

  @Test
  void testToGravitinoPrimitiveTypes() {
    assertEquals(Types.FixedCharType.of(4), converter.toGravitino(DataTypes.CHAR(4)));
    assertEquals(Types.StringType.get(), converter.toGravitino(DataTypes.STRING()));
    assertEquals(Types.BooleanType.get(), converter.toGravitino(DataTypes.BOOLEAN()));
    assertEquals(Types.FixedType.of(8), converter.toGravitino(DataTypes.BINARY(8)));
    assertEquals(Types.BinaryType.get(), converter.toGravitino(DataTypes.BYTES()));
    assertEquals(Types.DecimalType.of(10, 3), converter.toGravitino(DataTypes.DECIMAL(10, 3)));
    assertEquals(Types.ByteType.get(), converter.toGravitino(DataTypes.TINYINT()));
    assertEquals(Types.ShortType.get(), converter.toGravitino(DataTypes.SMALLINT()));
    assertEquals(Types.IntegerType.get(), converter.toGravitino(DataTypes.INT()));
    assertEquals(Types.LongType.get(), converter.toGravitino(DataTypes.BIGINT()));
    assertEquals(Types.FloatType.get(), converter.toGravitino(DataTypes.FLOAT()));
    assertEquals(Types.DoubleType.get(), converter.toGravitino(DataTypes.DOUBLE()));
    assertEquals(Types.DateType.get(), converter.toGravitino(DataTypes.DATE()));
    assertEquals(Types.TimeType.of(3), converter.toGravitino(DataTypes.TIME(3)));
    assertEquals(
        Types.TimestampType.withoutTimeZone(6), converter.toGravitino(DataTypes.TIMESTAMP(6)));
    assertEquals(
        Types.TimestampType.withTimeZone(3), converter.toGravitino(DataTypes.TIMESTAMP_LTZ(3)));
  }

  @Test
  void testToGravitinoComplexTypes() {
    Type list = converter.toGravitino(DataTypes.ARRAY(DataTypes.INT().copy(false)));
    Types.ListType listType = assertInstanceOf(Types.ListType.class, list);
    assertEquals(Types.IntegerType.get(), listType.elementType());
    assertFalse(listType.elementNullable());

    Type map =
        converter.toGravitino(DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT().copy(false)));
    Types.MapType mapType = assertInstanceOf(Types.MapType.class, map);
    assertEquals(Types.StringType.get(), mapType.keyType());
    assertEquals(Types.LongType.get(), mapType.valueType());
    assertFalse(mapType.valueNullable());

    Type struct =
        converter.toGravitino(
            DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT().copy(false), "primary identifier"),
                DataTypes.FIELD("name", DataTypes.STRING(), "name")));
    Types.StructType structType = assertInstanceOf(Types.StructType.class, struct);
    assertEquals(2, structType.fields().length);
    assertEquals("id", structType.fields()[0].name());
    assertEquals(Types.LongType.get(), structType.fields()[0].type());
    assertFalse(structType.fields()[0].nullable());
    assertEquals("primary identifier", structType.fields()[0].comment());
  }

  @Test
  void testRejectsUnsupportedGravitinoTypes() {
    assertThrows(
        IllegalArgumentException.class, () -> converter.fromGravitino(Types.ByteType.unsigned()));
    assertThrows(
        IllegalArgumentException.class, () -> converter.fromGravitino(Types.ShortType.unsigned()));
    assertThrows(
        IllegalArgumentException.class,
        () -> converter.fromGravitino(Types.IntegerType.unsigned()));
    assertThrows(
        IllegalArgumentException.class, () -> converter.fromGravitino(Types.LongType.unsigned()));
    assertThrows(
        IllegalArgumentException.class, () -> converter.fromGravitino(Types.UUIDType.get()));
    assertThrows(
        IllegalArgumentException.class, () -> converter.fromGravitino(Types.NullType.get()));
  }

  @Test
  void testFromGravitinoMapKeysAreAlwaysNotNull() {
    MapType map =
        assertInstanceOf(
            MapType.class,
            converter.fromGravitino(
                Types.MapType.of(Types.StringType.get(), Types.IntegerType.get(), true)));

    assertFalse(map.getKeyType().isNullable());
    assertTrue(map.getValueType().isNullable());
  }
}
