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
package org.apache.gravitino.catalog.lakehouse.iceberg.converter;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;

public class ConvertUtil {

  /**
   * Convert an Apache Iceberg Table to the corresponding schema information in the Iceberg.
   *
   * @param gravitinoTable Gravitino table of Iceberg.
   * @return Iceberg schema.
   */
  public static Schema toIcebergSchema(IcebergTable gravitinoTable) {
    org.apache.gravitino.rel.types.Types.StructType gravitinoStructType =
        toGravitinoStructType(gravitinoTable);
    Type converted =
        ToIcebergTypeVisitor.visit(gravitinoStructType, new ToIcebergType(gravitinoStructType));
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  /**
   * Convert an array of Gravitino columns to an Iceberg Schema.
   *
   * @param columns Gravitino columns.
   * @return Iceberg schema.
   */
  public static Schema toIcebergSchema(Column[] columns) {
    Preconditions.checkArgument(columns != null, "columns must not be null");
    org.apache.gravitino.rel.types.Types.StructType.Field[] fields =
        Arrays.stream(columns)
            .map(
                col ->
                    org.apache.gravitino.rel.types.Types.StructType.Field.of(
                        col.name(), col.dataType(), col.nullable(), col.comment()))
            .toArray(org.apache.gravitino.rel.types.Types.StructType.Field[]::new);
    org.apache.gravitino.rel.types.Types.StructType structType =
        org.apache.gravitino.rel.types.Types.StructType.of(fields);
    Type converted = ToIcebergTypeVisitor.visit(structType, new ToIcebergType(structType));
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  /**
   * Convert the nested field of Iceberg to the Iceberg column.
   *
   * <p>For format-version 3 fields, Iceberg {@code write-default} maps to {@link
   * Column#defaultValue()} and {@code initial-default} maps to {@link
   * Column#initialDefaultValue()}.
   *
   * @param nestedField Iceberg nested field.
   * @return Gravitino Iceberg column
   */
  public static IcebergColumn fromNestedField(Types.NestedField nestedField) {
    return IcebergColumn.builder()
        .withName(nestedField.name())
        .withNullable(nestedField.isOptional())
        .withComment(nestedField.doc())
        .withType(IcebergDataTypeConverter.CONVERTER.toGravitino(nestedField.type()))
        .withDefaultValue(
            fromIcebergLiteral(
                nestedField.writeDefaultLiteral(),
                nestedField.type(),
                Column.DEFAULT_VALUE_NOT_SET))
        .withInitialDefaultValue(
            fromIcebergLiteral(
                nestedField.initialDefaultLiteral(),
                nestedField.type(),
                Column.INITIAL_DEFAULT_VALUE_NOT_SET))
        .build();
  }

  private static Expression fromIcebergLiteral(
      Literal<?> icebergDefault, Type type, Expression unset) {
    if (icebergDefault == null) {
      return unset;
    }
    return toGravitinoLiteral(icebergDefault, type, unset);
  }

  private static Expression toGravitinoLiteral(
      Literal<?> icebergDefault, Type type, Expression unset) {
    Object value = icebergDefault.value();
    if (value == null) {
      return Literals.NULL;
    }

    switch (type.typeId()) {
      case BOOLEAN:
        return Literals.booleanLiteral((Boolean) value);
      case INTEGER:
        return Literals.integerLiteral((Integer) value);
      case LONG:
        return Literals.longLiteral((Long) value);
      case FLOAT:
        return Literals.floatLiteral((Float) value);
      case DOUBLE:
        return Literals.doubleLiteral((Double) value);
      case STRING:
        return Literals.stringLiteral(value.toString());
      case DATE:
        return Literals.dateLiteral(DateTimeUtil.dateFromDays((Integer) value));
      case TIME:
        return Literals.timeLiteral(DateTimeUtil.timeFromMicros((Long) value));
      case TIMESTAMP:
        {
          Types.TimestampType timestampType = (Types.TimestampType) type;
          if (timestampType.shouldAdjustToUTC()) {
            // Iceberg timestamptz is a UTC instant (micros). Expose it as UTC LocalDateTime while
            // keeping TimestampType.withTimeZone so the type still says "with time zone".
            return Literals.of(
                DateTimeUtil.timestamptzFromMicros((Long) value).toLocalDateTime(),
                org.apache.gravitino.rel.types.Types.TimestampType.withTimeZone(
                    FromIcebergType.PRECISION_MICROSECOND));
          }
          return Literals.of(
              DateTimeUtil.timestampFromMicros((Long) value),
              org.apache.gravitino.rel.types.Types.TimestampType.withoutTimeZone(
                  FromIcebergType.PRECISION_MICROSECOND));
        }
      case TIMESTAMP_NANO:
        {
          Types.TimestampNanoType timestampNanoType = (Types.TimestampNanoType) type;
          if (timestampNanoType.shouldAdjustToUTC()) {
            // Same UTC-instant → LocalDateTime convention as TIMESTAMP with zone above.
            return Literals.of(
                DateTimeUtil.timestamptzFromNanos((Long) value).toLocalDateTime(),
                org.apache.gravitino.rel.types.Types.TimestampType.withTimeZone(
                    FromIcebergType.PRECISION_NANOSECOND));
          }
          return Literals.of(
              DateTimeUtil.timestampFromNanos((Long) value),
              org.apache.gravitino.rel.types.Types.TimestampType.withoutTimeZone(
                  FromIcebergType.PRECISION_NANOSECOND));
        }
      case DECIMAL:
        {
          Types.DecimalType decimalType = (Types.DecimalType) type;
          return Literals.decimalLiteral(
              Decimal.of((BigDecimal) value, decimalType.precision(), decimalType.scale()));
        }
      case UUID:
        return Literals.of(
            ((UUID) value).toString(), org.apache.gravitino.rel.types.Types.UUIDType.get());
      case FIXED:
        return Literals.of(
            toByteArray(value),
            org.apache.gravitino.rel.types.Types.FixedType.of(((Types.FixedType) type).length()));
      case BINARY:
        return Literals.of(
            toByteArray(value), org.apache.gravitino.rel.types.Types.BinaryType.get());
      default:
        // Nested types cannot carry non-null Iceberg defaults; other V3 types are left unset
        // rather than failing table load.
        return unset;
    }
  }

  private static byte[] toByteArray(Object value) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    return ByteBuffers.toByteArray((ByteBuffer) value);
  }

  /**
   * Convert the Gravitino Iceberg table to the Gravitino StructType
   *
   * @param icebergTable Gravitino Iceberg table
   * @return Gravitino StructType
   */
  private static org.apache.gravitino.rel.types.Types.StructType toGravitinoStructType(
      IcebergTable icebergTable) {
    org.apache.gravitino.rel.types.Types.StructType.Field[] fields =
        Arrays.stream(icebergTable.columns())
            .map(
                column ->
                    org.apache.gravitino.rel.types.Types.StructType.Field.of(
                        column.name(), column.dataType(), column.nullable(), column.comment()))
            .toArray(org.apache.gravitino.rel.types.Types.StructType.Field[]::new);
    return org.apache.gravitino.rel.types.Types.StructType.of(fields);
  }

  private ConvertUtil() {}
}
