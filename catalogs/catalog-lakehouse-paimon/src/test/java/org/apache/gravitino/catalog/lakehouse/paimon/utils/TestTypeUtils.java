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
package org.apache.gravitino.catalog.lakehouse.paimon.utils;

import static org.apache.gravitino.catalog.lakehouse.paimon.utils.TypeUtils.fromPaimonType;
import static org.apache.gravitino.catalog.lakehouse.paimon.utils.TypeUtils.toPaimonType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.function.Consumer;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Type.Name;
import org.apache.gravitino.rel.types.Types;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Test;

/** Tests for {@link org.apache.gravitino.catalog.lakehouse.paimon.utils.TypeUtils}. */
public class TestTypeUtils {

  @Test
  void testFromAndToPaimonType() {
    // Test supported data types.
    RowType rowType =
        RowType.builder()
            .fields(
                DataTypes.BOOLEAN(),
                DataTypes.TINYINT(),
                DataTypes.SMALLINT(),
                DataTypes.INT(),
                DataTypes.BIGINT(),
                DataTypes.FLOAT(),
                DataTypes.DOUBLE(),
                DataTypes.DECIMAL(8, 3),
                DataTypes.DATE(),
                DataTypes.TIME(),
                DataTypes.TIME(0),
                DataTypes.TIME(3),
                DataTypes.TIME(6),
                DataTypes.TIME(9),
                DataTypes.TIMESTAMP(),
                DataTypes.TIMESTAMP(0),
                DataTypes.TIMESTAMP(3),
                DataTypes.TIMESTAMP(6),
                DataTypes.TIMESTAMP(9),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(0),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9),
                DataTypes.STRING(),
                DataTypes.VARCHAR(10),
                DataTypes.CHAR(10),
                DataTypes.BINARY(BinaryType.MAX_LENGTH),
                DataTypes.VARBINARY(VarBinaryType.MAX_LENGTH),
                DataTypes.ARRAY(DataTypes.INT()),
                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
            .build();

    Type gravitinoDataType = toGravitinoDataType(rowType);

    DataType paimonDataType = toPaimonType(gravitinoDataType);

    assertEquals(rowType, paimonDataType);
  }

  @Test
  void testUnparsedType() {
    Arrays.asList(
            DataTypes.CHAR(CharType.MAX_LENGTH), DataTypes.VARBINARY(VarBinaryType.MAX_LENGTH))
        .forEach(this::toGravitinoDataType);
  }

  @Test
  void testUnsupportedType() {
    // Test UnsupportedOperationException with IntervalYearType, IntervalDayType, FixedCharType,
    // UUIDType, FixedType, UnionType, NullType for toPaimonType.
    Arrays.asList(
            Types.IntervalYearType.get(),
            Types.IntervalDayType.get(),
            Types.UUIDType.get(),
            Types.UnionType.of(Types.IntegerType.get()),
            Types.NullType.get(),
            Types.UnparsedType.of("unparsed"))
        .forEach(this::checkUnsupportedType);
  }

  @Test
  void testToPaimonTypeNullability() {
    assertEquals(
        DataTypes.ARRAY(DataTypes.INT().notNull()),
        toPaimonType(Types.ListType.of(Types.IntegerType.get(), false)));
    assertEquals(
        DataTypes.ARRAY(DataTypes.INT().nullable()),
        toPaimonType(Types.ListType.of(Types.IntegerType.get(), true)));
    assertEquals(
        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT().notNull()),
        toPaimonType(Types.MapType.of(Types.StringType.get(), Types.IntegerType.get(), false)));
    assertEquals(
        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT().nullable()),
        toPaimonType(Types.MapType.of(Types.StringType.get(), Types.IntegerType.get(), true)));
  }

  private Type toGravitinoDataType(DataType dataType) {
    switch (dataType.getTypeRoot()) {
      case BOOLEAN:
        return checkDataType(dataType, Name.BOOLEAN);
      case TINYINT:
        return checkDataType(dataType, Name.BYTE);
      case SMALLINT:
        return checkDataType(dataType, Name.SHORT);
      case INTEGER:
        return checkDataType(dataType, Name.INTEGER);
      case BIGINT:
        return checkDataType(dataType, Name.LONG);
      case FLOAT:
        return checkDataType(dataType, Name.FLOAT);
      case DOUBLE:
        return checkDataType(dataType, Name.DOUBLE);
      case DECIMAL:
        return checkDataType(
            dataType,
            Name.DECIMAL,
            type -> {
              assertEquals(
                  ((DecimalType) dataType).getPrecision(), ((Types.DecimalType) type).precision());
              assertEquals(((DecimalType) dataType).getScale(), ((Types.DecimalType) type).scale());
            });
      case DATE:
        return checkDataType(dataType, Name.DATE);
      case TIME_WITHOUT_TIME_ZONE:
        return checkDataType(dataType, Name.TIME);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return checkDataType(
            dataType,
            Name.TIMESTAMP,
            type -> assertFalse(((Types.TimestampType) type).hasTimeZone()));
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return checkDataType(
            dataType,
            Name.TIMESTAMP,
            type -> assertTrue(((Types.TimestampType) type).hasTimeZone()));
      case VARCHAR:
        if (((VarCharType) dataType).getLength() == Integer.MAX_VALUE) {
          return checkDataType(dataType, Name.STRING);
        } else {
          return checkDataType(
              dataType,
              Name.VARCHAR,
              type ->
                  assertEquals(
                      ((VarCharType) dataType).getLength(), ((Types.VarCharType) type).length()));
        }
      case CHAR:
        return checkDataType(dataType, Name.FIXEDCHAR);
      case BINARY:
        return checkDataType(dataType, Name.FIXED);
      case VARBINARY:
        return checkDataType(dataType, Name.BINARY);
      case ARRAY:
        return checkDataType(
            dataType,
            Name.LIST,
            type -> assertTrue(((Types.ListType) type).elementType() instanceof Types.IntegerType));
      case MULTISET:
        return checkDataType(
            dataType,
            Name.MAP,
            type -> {
              assertEquals(
                  fromPaimonType(((MultisetType) dataType).getElementType()),
                  ((Types.MapType) type).keyType());
              assertTrue(((Types.MapType) type).valueType() instanceof Types.IntegerType);
            });
      case MAP:
        return checkDataType(
            dataType,
            Name.MAP,
            type -> {
              assertEquals(
                  fromPaimonType(((MapType) dataType).getKeyType()),
                  ((Types.MapType) type).keyType());
              assertEquals(
                  fromPaimonType(((MapType) dataType).getValueType()),
                  ((Types.MapType) type).valueType());
            });
      case ROW:
        return checkDataType(
            dataType,
            Name.STRUCT,
            type -> ((RowType) dataType).getFieldTypes().forEach(this::toGravitinoDataType));
      default:
        return checkDataType(dataType, Name.UNPARSED);
    }
  }

  private Type checkDataType(DataType dataType, Name expected) {
    return checkDataType(dataType, expected, type -> {});
  }

  private Type checkDataType(DataType dataType, Name expected, Consumer<Type> consumer) {
    Type actual = fromPaimonType(dataType);
    assertEquals(expected, actual.name());
    consumer.accept(actual);
    return actual;
  }

  private void checkUnsupportedType(Type type) {
    UnsupportedOperationException exception =
        assertThrowsExactly(UnsupportedOperationException.class, () -> toPaimonType(type));
    assertEquals(
        String.format("Paimon does not support Gravitino %s data type.", type.simpleString()),
        exception.getMessage());
  }
}
