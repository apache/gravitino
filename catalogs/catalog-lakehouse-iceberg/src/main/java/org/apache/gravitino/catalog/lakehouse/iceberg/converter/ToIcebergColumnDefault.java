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
import java.util.UUID;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;

/**
 * Converts Gravitino column default {@link Expression} values into Iceberg {@link Literal} values
 * for format-version 3 write-default / initial-default.
 */
public final class ToIcebergColumnDefault {

  private ToIcebergColumnDefault() {}

  /**
   * Converts a Gravitino default expression to an Iceberg {@link Literal}, or {@code null} when
   * unset / NULL.
   *
   * @param expression the Gravitino expression
   * @param icebergType the target Iceberg field type
   * @return the Iceberg literal, or null when unset
   */
  public static Literal<?> fromGravitino(Expression expression, Type icebergType) {
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
    Literal<?> icebergLiteral;
    switch (icebergType.typeId()) {
      case BOOLEAN:
        icebergLiteral = Literal.of((Boolean) value);
        break;
      case INTEGER:
        icebergLiteral = Literal.of(((Number) value).intValue());
        break;
      case LONG:
        icebergLiteral = Literal.of(((Number) value).longValue());
        break;
      case FLOAT:
        icebergLiteral = Literal.of(((Number) value).floatValue());
        break;
      case DOUBLE:
        icebergLiteral = Literal.of(((Number) value).doubleValue());
        break;
      case STRING:
        icebergLiteral = Literal.of(String.valueOf(value));
        break;
      case DATE:
        icebergLiteral = Literal.of(DateTimeUtil.daysFromDate((LocalDate) value));
        break;
      case TIME:
        icebergLiteral = Literal.of(DateTimeUtil.microsFromTime((LocalTime) value));
        break;
      case TIMESTAMP:
        {
          Types.TimestampType timestampType = (Types.TimestampType) icebergType;
          long micros = DateTimeUtil.microsFromTimestamp((LocalDateTime) value);
          icebergLiteral =
              timestampType.shouldAdjustToUTC()
                  ? Literal.of(micros).to(timestampType)
                  : Literal.of(micros);
          break;
        }
      case TIMESTAMP_NANO:
        {
          Types.TimestampNanoType timestampNanoType = (Types.TimestampNanoType) icebergType;
          icebergLiteral =
              Literal.of(DateTimeUtil.nanosFromTimestamp((LocalDateTime) value))
                  .to(timestampNanoType);
          break;
        }
      case DECIMAL:
        icebergLiteral =
            Literal.of(value instanceof Decimal ? ((Decimal) value).value() : (BigDecimal) value);
        break;
      case UUID:
        icebergLiteral = Literal.of(UUID.fromString(String.valueOf(value)));
        break;
      case FIXED:
      case BINARY:
        icebergLiteral = Literal.of(toByteArray(value));
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported Iceberg column default type: " + icebergType);
    }
    Literal<?> typed = icebergLiteral.to(icebergType);
    Preconditions.checkArgument(
        typed != null, "Cannot cast column default %s to Iceberg type %s", value, icebergType);
    return typed;
  }

  private static byte[] toByteArray(Object value) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    return ByteBuffers.toByteArray((ByteBuffer) value);
  }
}
