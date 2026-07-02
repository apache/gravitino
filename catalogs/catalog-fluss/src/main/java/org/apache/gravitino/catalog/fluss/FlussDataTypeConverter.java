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

import java.util.Arrays;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeVisitor;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;
import org.apache.gravitino.connector.DataTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

final class FlussDataTypeConverter implements DataTypeConverter<DataType, DataType> {

  static final FlussDataTypeConverter CONVERTER = new FlussDataTypeConverter();

  private FlussDataTypeConverter() {}

  @Override
  public DataType fromGravitino(Type type) {
    return fromGravitino(type, true);
  }

  DataType fromGravitino(Type type, boolean nullable) {
    return GravitinoToFlussTypeVisitor.visit(type, nullable);
  }

  @Override
  public Type toGravitino(DataType dataType) {
    return dataType.accept(FlussToGravitinoTypeVisitor.INSTANCE);
  }

  private static DataField toFlussField(Types.StructType.Field field) {
    return DataTypes.FIELD(
        field.name(), CONVERTER.fromGravitino(field.type(), field.nullable()), field.comment());
  }

  private static Types.StructType.Field toGravitinoField(DataField field) {
    DataType dataType = field.getType();
    return Types.StructType.Field.of(
        field.getName(),
        CONVERTER.toGravitino(dataType),
        dataType.isNullable(),
        field.getDescription().orElse(null));
  }

  private static class GravitinoToFlussTypeVisitor {

    private static final GravitinoToFlussTypeVisitor INSTANCE = new GravitinoToFlussTypeVisitor();

    private static DataType visit(Type type, boolean nullable) {
      return INSTANCE.visit(type).copy(nullable);
    }

    private DataType visit(Type type) {
      return switch (type.name()) {
        case BOOLEAN -> DataTypes.BOOLEAN();
        case BYTE -> {
          checkSigned(type);
          yield DataTypes.TINYINT();
        }
        case SHORT -> {
          checkSigned(type);
          yield DataTypes.SMALLINT();
        }
        case INTEGER -> {
          checkSigned(type);
          yield DataTypes.INT();
        }
        case LONG -> {
          checkSigned(type);
          yield DataTypes.BIGINT();
        }
        case FLOAT -> DataTypes.FLOAT();
        case DOUBLE -> DataTypes.DOUBLE();
        case DECIMAL -> visitDecimal((Types.DecimalType) type);
        case DATE -> DataTypes.DATE();
        case TIME -> visitTime((Types.TimeType) type);
        case TIMESTAMP -> visitTimestamp((Types.TimestampType) type);
        case STRING, VARCHAR -> DataTypes.STRING();
        case FIXEDCHAR -> visitFixedChar((Types.FixedCharType) type);
        case FIXED -> visitFixed((Types.FixedType) type);
        case BINARY -> DataTypes.BYTES();
        case LIST -> visitList((Types.ListType) type);
        case MAP -> visitMap((Types.MapType) type);
        case STRUCT -> visitStruct((Types.StructType) type);
        default -> throw new IllegalArgumentException(
            "Unsupported Fluss type conversion for " + type);
      };
    }

    private static void checkSigned(Type type) {
      if (type instanceof Type.IntegralType && !((Type.IntegralType) type).signed()) {
        throw new IllegalArgumentException("Fluss does not support unsigned integer type " + type);
      }
    }

    private DataType visitDecimal(Types.DecimalType decimalType) {
      return DataTypes.DECIMAL(decimalType.precision(), decimalType.scale());
    }

    private DataType visitTime(Types.TimeType timeType) {
      return timeType.hasPrecisionSet() ? DataTypes.TIME(timeType.precision()) : DataTypes.TIME();
    }

    private DataType visitTimestamp(Types.TimestampType timestampType) {
      if (timestampType.hasPrecisionSet()) {
        return timestampType.hasTimeZone()
            ? DataTypes.TIMESTAMP_LTZ(timestampType.precision())
            : DataTypes.TIMESTAMP(timestampType.precision());
      }
      return timestampType.hasTimeZone() ? DataTypes.TIMESTAMP_LTZ() : DataTypes.TIMESTAMP();
    }

    private DataType visitFixedChar(Types.FixedCharType fixedCharType) {
      return DataTypes.CHAR(fixedCharType.length());
    }

    private DataType visitFixed(Types.FixedType fixedType) {
      return DataTypes.BINARY(fixedType.length());
    }

    private DataType visitList(Types.ListType listType) {
      return DataTypes.ARRAY(visit(listType.elementType(), listType.elementNullable()));
    }

    private DataType visitMap(Types.MapType mapType) {
      return DataTypes.MAP(
          visit(mapType.keyType(), false), visit(mapType.valueType(), mapType.valueNullable()));
    }

    private DataType visitStruct(Types.StructType structType) {
      return DataTypes.ROW(
          Arrays.stream(structType.fields())
              .map(FlussDataTypeConverter::toFlussField)
              .toArray(DataField[]::new));
    }
  }

  private static class FlussToGravitinoTypeVisitor implements DataTypeVisitor<Type> {

    private static final FlussToGravitinoTypeVisitor INSTANCE = new FlussToGravitinoTypeVisitor();

    @Override
    public Type visit(CharType charType) {
      return Types.FixedCharType.of(charType.getLength());
    }

    @Override
    public Type visit(StringType stringType) {
      return Types.StringType.get();
    }

    @Override
    public Type visit(BooleanType booleanType) {
      return Types.BooleanType.get();
    }

    @Override
    public Type visit(BinaryType binaryType) {
      return Types.FixedType.of(binaryType.getLength());
    }

    @Override
    public Type visit(BytesType bytesType) {
      return Types.BinaryType.get();
    }

    @Override
    public Type visit(DecimalType decimalType) {
      return Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public Type visit(TinyIntType tinyIntType) {
      return Types.ByteType.get();
    }

    @Override
    public Type visit(SmallIntType smallIntType) {
      return Types.ShortType.get();
    }

    @Override
    public Type visit(IntType intType) {
      return Types.IntegerType.get();
    }

    @Override
    public Type visit(BigIntType bigIntType) {
      return Types.LongType.get();
    }

    @Override
    public Type visit(FloatType floatType) {
      return Types.FloatType.get();
    }

    @Override
    public Type visit(DoubleType doubleType) {
      return Types.DoubleType.get();
    }

    @Override
    public Type visit(DateType dateType) {
      return Types.DateType.get();
    }

    @Override
    public Type visit(TimeType timeType) {
      return Types.TimeType.of(timeType.getPrecision());
    }

    @Override
    public Type visit(TimestampType timestampType) {
      return Types.TimestampType.withoutTimeZone(timestampType.getPrecision());
    }

    @Override
    public Type visit(LocalZonedTimestampType localZonedTimestampType) {
      return Types.TimestampType.withTimeZone(localZonedTimestampType.getPrecision());
    }

    @Override
    public Type visit(ArrayType arrayType) {
      DataType elementType = arrayType.getElementType();
      return Types.ListType.of(elementType.accept(this), elementType.isNullable());
    }

    @Override
    public Type visit(MapType mapType) {
      DataType valueType = mapType.getValueType();
      return Types.MapType.of(
          mapType.getKeyType().accept(this), valueType.accept(this), valueType.isNullable());
    }

    @Override
    public Type visit(RowType rowType) {
      return Types.StructType.of(
          rowType.getFields().stream()
              .map(FlussDataTypeConverter::toGravitinoField)
              .toArray(Types.StructType.Field[]::new));
    }
  }
}
