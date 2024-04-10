/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.paimon.utils;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.TypeUtils.fromPaimonType;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.TypeUtils.toPaimonType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Type.Name;
import com.datastrato.gravitino.rel.types.Types;
import java.util.Arrays;
import java.util.function.Consumer;
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

/** Tests for {@link TypeUtils}. */
public class TestTypeUtils {

  @Test
  void testFromToPaimonType() {
    // Test supported data types.
    RowType rowType =
        RowType.builder()
            .fields(
                DataTypes.VARCHAR(10),
                DataTypes.STRING(),
                DataTypes.BOOLEAN(),
                DataTypes.BINARY(BinaryType.MAX_LENGTH),
                DataTypes.DECIMAL(8, 3),
                DataTypes.TINYINT(),
                DataTypes.SMALLINT(),
                DataTypes.INT(),
                DataTypes.BIGINT(),
                DataTypes.FLOAT(),
                DataTypes.DOUBLE(),
                DataTypes.DATE(),
                DataTypes.TIME(),
                DataTypes.TIMESTAMP(),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
            .build();
    assertEquals(rowType, toPaimonType(assertDataType(rowType)));
    // Test UnparsedType with CharType, VarBinaryType for fromPaimonType.
    Arrays.asList(
            DataTypes.CHAR(CharType.MAX_LENGTH), DataTypes.VARBINARY(VarBinaryType.MAX_LENGTH))
        .forEach(this::assertDataType);
    // Test UnsupportedOperationException with IntervalYearType, IntervalDayType, FixedCharType,
    // UUIDType, FixedType, UnionType, NullType for toPaimonType.
    Arrays.asList(
            Types.IntervalYearType.get(),
            Types.IntervalDayType.get(),
            Types.FixedCharType.of(10),
            Types.UUIDType.get(),
            Types.FixedType.of(20),
            Types.UnionType.of(Types.IntegerType.get()),
            Types.NullType.get(),
            Types.UnparsedType.of("unparsed"))
        .forEach(this::assertUnsupportedType);
  }

  private Type assertDataType(DataType dataType) {
    switch (dataType.getTypeRoot()) {
      case VARCHAR:
        return assertDataType(
            dataType,
            Name.VARCHAR,
            type ->
                assertEquals(
                    ((VarCharType) dataType).getLength(), ((Types.VarCharType) type).length()));
      case BOOLEAN:
        return assertDataType(dataType, Name.BOOLEAN);
      case BINARY:
        return assertDataType(dataType, Name.BINARY);
      case DECIMAL:
        return assertDataType(
            dataType,
            Name.DECIMAL,
            type -> {
              assertEquals(
                  ((DecimalType) dataType).getPrecision(), ((Types.DecimalType) type).precision());
              assertEquals(((DecimalType) dataType).getScale(), ((Types.DecimalType) type).scale());
            });
      case TINYINT:
        return assertDataType(dataType, Name.BYTE);
      case SMALLINT:
        return assertDataType(dataType, Name.SHORT);
      case INTEGER:
        return assertDataType(dataType, Name.INTEGER);
      case BIGINT:
        return assertDataType(dataType, Name.LONG);
      case FLOAT:
        return assertDataType(dataType, Name.FLOAT);
      case DOUBLE:
        return assertDataType(dataType, Name.DOUBLE);
      case DATE:
        return assertDataType(dataType, Name.DATE);
      case TIME_WITHOUT_TIME_ZONE:
        return assertDataType(dataType, Name.TIME);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return assertDataType(
            dataType,
            Name.TIMESTAMP,
            type -> assertFalse(((Types.TimestampType) type).hasTimeZone()));
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return assertDataType(
            dataType,
            Name.TIMESTAMP,
            type -> assertTrue(((Types.TimestampType) type).hasTimeZone()));
      case ARRAY:
        return assertDataType(
            dataType,
            Name.LIST,
            type -> assertTrue(((Types.ListType) type).elementType() instanceof Types.IntegerType));
      case MULTISET:
        return assertDataType(
            dataType,
            Name.MAP,
            type -> {
              assertEquals(
                  fromPaimonType(((MultisetType) dataType).getElementType()),
                  ((Types.MapType) type).keyType());
              assertTrue(((Types.MapType) type).valueType() instanceof Types.IntegerType);
            });
      case MAP:
        return assertDataType(
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
        return assertDataType(
            dataType,
            Name.STRUCT,
            type -> ((RowType) dataType).getFieldTypes().forEach(this::assertDataType));
      default:
        return assertDataType(dataType, Name.UNPARSED);
    }
  }

  private Type assertDataType(DataType dataType, Name expected) {
    return assertDataType(dataType, expected, type -> {});
  }

  private Type assertDataType(DataType dataType, Name expected, Consumer<Type> consumer) {
    Type actual = fromPaimonType(dataType);
    assertEquals(expected, actual.name());
    consumer.accept(actual);
    return actual;
  }

  private void assertUnsupportedType(Type type) {
    UnsupportedOperationException exception =
        assertThrowsExactly(UnsupportedOperationException.class, () -> toPaimonType(type));
    assertEquals(
        String.format("Paimon does not support Gravitino %s data type.", type.simpleString()),
        exception.getMessage());
  }
}
