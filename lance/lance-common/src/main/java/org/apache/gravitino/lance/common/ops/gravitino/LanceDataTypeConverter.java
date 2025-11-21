/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.lance.common.ops.gravitino;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.gravitino.connector.DataTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.rel.types.Types.FixedType;

public class LanceDataTypeConverter implements DataTypeConverter<ArrowType, Field> {

  public static final LanceDataTypeConverter CONVERTER = new LanceDataTypeConverter();
  private static final ObjectMapper mapper = new ObjectMapper();

  public Field toArrowField(String name, Type type, boolean nullable) {
    switch (type.name()) {
      case LIST:
        Types.ListType listType = (Types.ListType) type;
        FieldType listField = new FieldType(nullable, ArrowType.List.INSTANCE, null);
        return new Field(
            name,
            listField,
            Lists.newArrayList(
                toArrowField("element", listType.elementType(), listType.elementNullable())));

      case STRUCT:
        Types.StructType structType = (Types.StructType) type;
        FieldType structField = new FieldType(nullable, ArrowType.Struct.INSTANCE, null);
        return new Field(
            name,
            structField,
            Arrays.stream(structType.fields())
                .map(field -> toArrowField(field.name(), field.type(), field.nullable()))
                .toList());

      case MAP:
        Types.MapType mapType = (Types.MapType) type;
        FieldType mapField = new FieldType(nullable, new ArrowType.Map(false), null);
        return new Field(
            name,
            mapField,
            Lists.newArrayList(
                toArrowField(
                    MapVector.DATA_VECTOR_NAME,
                    Types.StructType.of(
                        Types.StructType.Field.of(
                            // Note: Arrow MapVector requires key field to be non-nullable
                            MapVector.KEY_NAME,
                            mapType.keyType(),
                            false /*nullable*/,
                            null /*comment*/),
                        Types.StructType.Field.of(
                            MapVector.VALUE_NAME,
                            mapType.valueType(),
                            mapType.valueNullable(),
                            null)),
                    false /*nullable*/)));

      case UNION:
        Types.UnionType unionType = (Types.UnionType) type;
        List<Field> types =
            Arrays.stream(unionType.types())
                .map(
                    t ->
                        toArrowField(
                            t.simpleString(), t, true /*nullable*/) // union members are nullable
                    )
                .toList();
        int[] typeIds =
            types.stream()
                .mapToInt(
                    f ->
                        org.apache.arrow.vector.types.Types.getMinorTypeForArrowType(f.getType())
                            .ordinal())
                .toArray();
        FieldType unionField =
            new FieldType(nullable, new ArrowType.Union(UnionMode.Sparse, typeIds), null);
        return new Field(name, unionField, types);

      case EXTERNAL:
        Types.ExternalType externalType = (Types.ExternalType) type;
        Field field;
        try {
          field = mapper.readValue(externalType.catalogString(), Field.class);
        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to parse external type catalog string: " + externalType.catalogString(), e);
        }
        Preconditions.checkArgument(
            name.equals(field.getName()),
            "expected field name %s but got %s",
            name,
            field.getName());
        Preconditions.checkArgument(
            nullable == field.isNullable(),
            "expected field nullable %s but got %s",
            nullable,
            field.isNullable());
        return field;

      default:
        // non-complex type
        FieldType fieldType = new FieldType(nullable, fromGravitino(type), null);
        return new Field(name, fieldType, null);
    }
  }

  @Override
  public ArrowType fromGravitino(Type type) {
    switch (type.name()) {
      case BOOLEAN:
        return Bool.INSTANCE;
      case BYTE:
        return new Int(8, ((Types.ByteType) type).signed());
      case SHORT:
        return new Int(8 * 2, ((Types.ShortType) type).signed());
      case INTEGER:
        return new Int(8 * 4, ((Types.IntegerType) type).signed());
      case LONG:
        return new Int(8 * 8, ((Types.LongType) type).signed());
      case FLOAT:
        return new FloatingPoint(FloatingPointPrecision.SINGLE);
      case DOUBLE:
        return new FloatingPoint(FloatingPointPrecision.DOUBLE);
      case STRING:
        return ArrowType.Utf8.INSTANCE;
      case BINARY:
        return ArrowType.Binary.INSTANCE;
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) type;
        return new ArrowType.Decimal(decimalType.precision(), decimalType.scale(), 8 * 16);
      case DATE:
        return new ArrowType.Date(DateUnit.DAY);
      case TIMESTAMP:
        Types.TimestampType timestampType = (Types.TimestampType) type;
        TimeUnit timeUnit = TimeUnit.MICROSECOND;
        if (timestampType.hasPrecisionSet()) {
          timeUnit =
              switch (timestampType.precision()) {
                case 0 -> TimeUnit.SECOND;
                case 3 -> TimeUnit.MILLISECOND;
                case 6 -> TimeUnit.MICROSECOND;
                case 9 -> TimeUnit.NANOSECOND;
                default -> throw new UnsupportedOperationException(
                    "Expected precision to be one of 0, 3, 6, 9 but got: "
                        + timestampType.precision());
              };
        }
        if (timestampType.hasTimeZone()) {
          // todo: need timeZoneId for timestamp with time zone
          return new ArrowType.Timestamp(timeUnit, "UTC");
        }
        return new ArrowType.Timestamp(timeUnit, null);
      case TIME:
        return new ArrowType.Time(TimeUnit.NANOSECOND, 8 * 8);
      case NULL:
        return ArrowType.Null.INSTANCE;
      case INTERVAL_YEAR:
        return new ArrowType.Interval(IntervalUnit.YEAR_MONTH);
      case INTERVAL_DAY:
        return new ArrowType.Duration(TimeUnit.MICROSECOND);
      case FIXED:
        FixedType fixedType = (FixedType) type;
        return new ArrowType.FixedSizeBinary(fixedType.length());
      default:
        throw new UnsupportedOperationException("Unsupported Gravitino type: " + type.name());
    }
  }

  @Override
  public Type toGravitino(Field arrowField) {
    FieldType fieldType = arrowField.getFieldType();
    switch (fieldType.getType().getTypeID()) {
      case Map:
        Field structField = arrowField.getChildren().get(0);
        Type keyType = toGravitino(structField.getChildren().get(0));
        Type valueType = toGravitino(structField.getChildren().get(1));
        boolean valueNullable = structField.getChildren().get(1).isNullable();
        return Types.MapType.of(keyType, valueType, valueNullable);

      case List:
        Type elementType = toGravitino(arrowField.getChildren().get(0));
        boolean containsNull = arrowField.getChildren().get(0).isNullable();
        return Types.ListType.of(elementType, containsNull);

      case Struct:
        Types.StructType.Field[] fields =
            arrowField.getChildren().stream()
                .map(
                    child ->
                        Types.StructType.Field.of(
                            child.getName(),
                            toGravitino(child),
                            child.isNullable(),
                            null /*comment*/))
                .toArray(Types.StructType.Field[]::new);
        return Types.StructType.of(fields);

      case Union:
        List<Type> types = arrowField.getChildren().stream().map(this::toGravitino).toList();
        return Types.UnionType.of(types.toArray(new Type[0]));

      case Bool:
        return Types.BooleanType.get();

      case Int:
        Int intType = (Int) fieldType.getType();
        switch (intType.getBitWidth()) {
          case 8:
            return intType.getIsSigned() ? Types.ByteType.get() : Types.ByteType.unsigned();
          case 8 * 2:
            return intType.getIsSigned() ? Types.ShortType.get() : Types.ShortType.unsigned();
          case 8 * 4:
            return intType.getIsSigned() ? Types.IntegerType.get() : Types.IntegerType.unsigned();
          case 8 * 8:
            return intType.getIsSigned() ? Types.LongType.get() : Types.LongType.unsigned();
        }
        break;

      case FloatingPoint:
        FloatingPoint floatingPoint = (FloatingPoint) fieldType.getType();
        switch (floatingPoint.getPrecision()) {
          case SINGLE:
            return Types.FloatType.get();
          case DOUBLE:
            return Types.DoubleType.get();
          default:
            // fallthrough
        }
        break;

      case Utf8:
        return Types.StringType.get();
      case Binary:
        return Types.BinaryType.get();
      case Decimal:
        ArrowType.Decimal decimalType = (ArrowType.Decimal) fieldType.getType();
        return Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale());
      case Date:
        if (((ArrowType.Date) fieldType.getType()).getUnit() == DateUnit.DAY) {
          return Types.DateType.get();
        }
        break;
      case Timestamp:
        ArrowType.Timestamp timestampType = (ArrowType.Timestamp) fieldType.getType();
        int precision =
            switch (timestampType.getUnit()) {
              case SECOND -> 0;
              case MILLISECOND -> 3;
              case MICROSECOND -> 6;
              case NANOSECOND -> 9;
            };
        boolean hasTimeZone = timestampType.getTimezone() != null;
        return hasTimeZone
            ? Types.TimestampType.withTimeZone(precision)
            : Types.TimestampType.withoutTimeZone(precision);
      case Time:
        ArrowType.Time timeType = (ArrowType.Time) fieldType.getType();
        if (timeType.getUnit() == TimeUnit.NANOSECOND && timeType.getBitWidth() == 8 * 8) {
          return Types.TimeType.get();
        }
        break;
      case Null:
        return Types.NullType.get();
      case Interval:
        IntervalUnit intervalUnit = ((ArrowType.Interval) fieldType.getType()).getUnit();
        if (intervalUnit == IntervalUnit.YEAR_MONTH) {
          return Types.IntervalYearType.get();
        }
        break;
      case Duration:
        TimeUnit timeUnit = ((ArrowType.Duration) fieldType.getType()).getUnit();
        if (timeUnit == TimeUnit.MICROSECOND) {
          return Types.IntervalDayType.get();
        }
        break;
      case FixedSizeBinary:
        ArrowType.FixedSizeBinary fixedSizeBinary = (ArrowType.FixedSizeBinary) fieldType.getType();
        return Types.FixedType.of(fixedSizeBinary.getByteWidth());
      default:
        // fallthrough
    }

    String typeString;
    try {
      typeString = mapper.writeValueAsString(arrowField);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize Arrow field to string.", e);
    }
    return Types.ExternalType.of(typeString);
  }
}
