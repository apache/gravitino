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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.TimestampNanoType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;

/**
 * Converts Iceberg format-version 3 column defaults into Gravitino {@link Expression} values.
 *
 * <p>Iceberg {@code write-default} maps to {@link Column#defaultValue()} and {@code
 * initial-default} maps to {@link Column#initialDefaultValue()}.
 */
final class FromIcebergColumnDefault {

  private FromIcebergColumnDefault() {}

  /**
   * Returns the Gravitino write-default expression for {@code field}.
   *
   * @param field the Iceberg nested field
   * @return the mapped default expression
   */
  static Expression writeDefault(NestedField field) {
    return toGravitino(field.writeDefaultLiteral(), field.type(), Column.DEFAULT_VALUE_NOT_SET);
  }

  /**
   * Returns the Gravitino initial-default expression for {@code field}.
   *
   * @param field the Iceberg nested field
   * @return the mapped initial default expression
   */
  static Expression initialDefault(NestedField field) {
    return toGravitino(
        field.initialDefaultLiteral(), field.type(), Column.INITIAL_DEFAULT_VALUE_NOT_SET);
  }

  private static Expression toGravitino(Literal<?> icebergDefault, Type type, Expression unset) {
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
          TimestampType timestampType = (TimestampType) type;
          if (timestampType.shouldAdjustToUTC()) {
            return Literals.of(
                DateTimeUtil.timestamptzFromMicros((Long) value).toLocalDateTime(),
                Types.TimestampType.withTimeZone(FromIcebergType.PRECISION_MICROSECOND));
          }
          return Literals.of(
              DateTimeUtil.timestampFromMicros((Long) value),
              Types.TimestampType.withoutTimeZone(FromIcebergType.PRECISION_MICROSECOND));
        }
      case TIMESTAMP_NANO:
        {
          TimestampNanoType timestampNanoType = (TimestampNanoType) type;
          if (timestampNanoType.shouldAdjustToUTC()) {
            return Literals.of(
                DateTimeUtil.timestamptzFromNanos((Long) value).toLocalDateTime(),
                Types.TimestampType.withTimeZone(FromIcebergType.PRECISION_NANOSECOND));
          }
          return Literals.of(
              DateTimeUtil.timestampFromNanos((Long) value),
              Types.TimestampType.withoutTimeZone(FromIcebergType.PRECISION_NANOSECOND));
        }
      case DECIMAL:
        {
          DecimalType decimalType = (DecimalType) type;
          return Literals.decimalLiteral(
              Decimal.of((BigDecimal) value, decimalType.precision(), decimalType.scale()));
        }
      case UUID:
        return Literals.of(((UUID) value).toString(), Types.UUIDType.get());
      case FIXED:
        return Literals.of(toByteArray(value), Types.FixedType.of(((FixedType) type).length()));
      case BINARY:
        return Literals.of(toByteArray(value), Types.BinaryType.get());
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
}
