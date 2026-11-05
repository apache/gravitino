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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    return toIcebergSchema(gravitinoTable.columns());
  }

  /**
   * Convert Gravitino columns to an Iceberg Schema, including format-version 3 column defaults.
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
    List<Types.NestedField> icebergFields = converted.asNestedType().asStructType().fields();
    List<Types.NestedField> withDefaults = new ArrayList<>(icebergFields.size());
    for (int i = 0; i < icebergFields.size(); i++) {
      Types.NestedField field = icebergFields.get(i);
      Literal<?> write = toIcebergLiteral(columns[i].defaultValue(), field.type());
      Literal<?> initial = toIcebergLiteral(columns[i].initialDefaultValue(), field.type());
      if (write == null && initial == null) {
        withDefaults.add(field);
      } else {
        // Seed both when only one side is set (Iceberg addColumn behavior for new fields).
        withDefaults.add(
            Types.NestedField.from(field)
                .withWriteDefault(write != null ? write : initial)
                .withInitialDefault(initial != null ? initial : write)
                .build());
      }
    }
    return new Schema(withDefaults);
  }

  /**
   * Converts a Gravitino default expression to an Iceberg {@link Literal}, or {@code null} when
   * unset / NULL.
   *
   * @param expression the Gravitino expression
   * @param icebergType the target Iceberg field type
   * @return the Iceberg literal, or null when unset
   */
  public static Literal<?> toIcebergLiteral(Expression expression, Type icebergType) {
    if (expression == null
        || expression.equals(Column.DEFAULT_VALUE_NOT_SET)
        || expression.equals(Column.INITIAL_DEFAULT_VALUE_NOT_SET)
        || Literals.NULL.equals(expression)) {
      return null;
    }
    Preconditions.checkArgument(
        expression instanceof org.apache.gravitino.rel.expressions.literals.Literal,
        "Iceberg column defaults must be literals, got: %s",
        expression);
    Object value = ((org.apache.gravitino.rel.expressions.literals.Literal<?>) expression).value();
    Literal<?> typed = toIcebergLiteralValue(value, icebergType).to(icebergType);
    Preconditions.checkArgument(
        typed != null, "Cannot cast column default %s to Iceberg type %s", value, icebergType);
    return typed;
  }

  /**
   * Convert the nested field of Iceberg to the Iceberg column.
   *
   * <p>Iceberg {@code write-default} maps to {@link Column#defaultValue()} and {@code
   * initial-default} maps to {@link Column#initialDefaultValue()}.
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
            toGravitinoLiteral(
                nestedField.writeDefaultLiteral(),
                nestedField.type(),
                Column.DEFAULT_VALUE_NOT_SET))
        .withInitialDefaultValue(
            toGravitinoLiteral(
                nestedField.initialDefaultLiteral(),
                nestedField.type(),
                Column.INITIAL_DEFAULT_VALUE_NOT_SET))
        .build();
  }

  private static Literal<?> toIcebergLiteralValue(Object value, Type icebergType) {
    switch (icebergType.typeId()) {
      case BOOLEAN:
        return Literal.of((Boolean) value);
      case INTEGER:
        return Literal.of(((Number) value).intValue());
      case LONG:
        return Literal.of(((Number) value).longValue());
      case FLOAT:
        return Literal.of(((Number) value).floatValue());
      case DOUBLE:
        return Literal.of(((Number) value).doubleValue());
      case STRING:
        return Literal.of(String.valueOf(value));
      case DATE:
        return Literal.of(DateTimeUtil.daysFromDate((LocalDate) value));
      case TIME:
        return Literal.of(DateTimeUtil.microsFromTime((LocalTime) value));
      case TIMESTAMP:
        {
          Types.TimestampType timestampType = (Types.TimestampType) icebergType;
          long micros = DateTimeUtil.microsFromTimestamp((LocalDateTime) value);
          return timestampType.shouldAdjustToUTC()
              ? Literal.of(micros).to(timestampType)
              : Literal.of(micros);
        }
      case TIMESTAMP_NANO:
        {
          Types.TimestampNanoType timestampNanoType = (Types.TimestampNanoType) icebergType;
          return Literal.of(DateTimeUtil.nanosFromTimestamp((LocalDateTime) value))
              .to(timestampNanoType);
        }
      case DECIMAL:
        return Literal.of(
            value instanceof Decimal ? ((Decimal) value).value() : (BigDecimal) value);
      case UUID:
        return Literal.of(UUID.fromString(String.valueOf(value)));
      case FIXED:
      case BINARY:
        return Literal.of(toByteArray(value));
      default:
        throw new IllegalArgumentException(
            "Unsupported Iceberg column default type: " + icebergType);
    }
  }

  private static Expression toGravitinoLiteral(
      Literal<?> icebergDefault, Type type, Expression unset) {
    if (icebergDefault == null) {
      return unset;
    }
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
        // Nested types cannot carry non-null Iceberg defaults; leave unset rather than fail load.
        return unset;
    }
  }

  private static byte[] toByteArray(Object value) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    return ByteBuffers.toByteArray((ByteBuffer) value);
  }

  private ConvertUtil() {}
}
