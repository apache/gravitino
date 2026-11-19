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
package org.apache.gravitino.iceberg.service.restriction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import javax.annotation.Nullable;
import org.apache.gravitino.policy.sql.Literal;
import org.apache.gravitino.policy.sql.LiteralType;
import org.apache.gravitino.policy.sql.SqlType;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Exact logical-type and literal binding shared by SQL predicates and masks. */
final class SqlRestrictionLiterals {
  private SqlRestrictionLiterals() {}

  static Type type(SqlType declared) {
    switch (declared.name()) {
      case "BOOLEAN":
        return Types.BooleanType.get();
      case "INTEGER":
        return Types.IntegerType.get();
      case "LONG":
        return Types.LongType.get();
      case "FLOAT":
        return Types.FloatType.get();
      case "DOUBLE":
        return Types.DoubleType.get();
      case "STRING":
        return Types.StringType.get();
      case "BINARY":
        return Types.BinaryType.get();
      case "DATE":
        return Types.DateType.get();
      case "DECIMAL":
        return Types.DecimalType.of(declared.precision(), declared.scale());
      case "TIME":
        temporalPrecision(declared);
        return Types.TimeType.get();
      case "TIMESTAMP":
        temporalPrecision(declared);
        return declared.withTimeZone()
            ? Types.TimestampType.withZone()
            : Types.TimestampType.withoutZone();
      default:
        throw new IllegalArgumentException("Unsupported declared SQL type: " + declared);
    }
  }

  @Nullable
  static Object value(Literal source, Type expected) {
    if (source.type() != null && !type(source.type()).equals(expected)) {
      throw new IllegalArgumentException("Literal or context type does not match " + expected);
    }
    if (source.literalType() == LiteralType.NULL) {
      return null;
    }
    try {
      switch (expected.typeId()) {
        case BOOLEAN:
          kind(source, LiteralType.BOOLEAN);
          if (!source.value().equalsIgnoreCase("TRUE")
              && !source.value().equalsIgnoreCase("FALSE")) {
            throw new IllegalArgumentException("Invalid Boolean literal");
          }
          return Boolean.parseBoolean(source.value());
        case STRING:
          kind(source, LiteralType.STRING);
          return source.value();
        case INTEGER:
          kind(source, LiteralType.NUMBER);
          return new BigDecimal(source.value()).intValueExact();
        case LONG:
          kind(source, LiteralType.NUMBER);
          return new BigDecimal(source.value()).longValueExact();
        case FLOAT:
          kind(source, LiteralType.NUMBER);
          BigDecimal floatDecimal = new BigDecimal(source.value());
          float floatValue = Float.parseFloat(source.value());
          if (!Float.isFinite(floatValue)
              || new BigDecimal((double) floatValue).compareTo(floatDecimal) != 0) {
            throw new IllegalArgumentException("Inexact FLOAT literal");
          }
          return floatValue;
        case DOUBLE:
          kind(source, LiteralType.NUMBER);
          BigDecimal doubleDecimal = new BigDecimal(source.value());
          double doubleValue = Double.parseDouble(source.value());
          if (!Double.isFinite(doubleValue)
              || new BigDecimal(doubleValue).compareTo(doubleDecimal) != 0) {
            throw new IllegalArgumentException("Inexact DOUBLE literal");
          }
          return doubleValue;
        case DECIMAL:
          kind(source, LiteralType.NUMBER);
          Types.DecimalType decimalType = (Types.DecimalType) expected;
          BigDecimal decimal =
              new BigDecimal(source.value())
                  .setScale(decimalType.scale(), RoundingMode.UNNECESSARY);
          if (decimal.precision() > decimalType.precision()) {
            throw new IllegalArgumentException("Decimal precision overflow");
          }
          return decimal;
        case DATE:
          kind(source, LiteralType.TEMPORAL);
          return Math.toIntExact(LocalDate.parse(source.value()).toEpochDay());
        case TIME:
          kind(source, LiteralType.TEMPORAL);
          LocalTime time = LocalTime.parse(source.value());
          if (!time.equals(time.truncatedTo(ChronoUnit.MICROS))) {
            throw new IllegalArgumentException("Unsupported temporal precision");
          }
          return time.toNanoOfDay() / 1000;
        case TIMESTAMP:
          kind(source, LiteralType.TEMPORAL);
          String text = source.value().replace(' ', 'T');
          boolean zoned = ((Types.TimestampType) expected).shouldAdjustToUTC();
          LocalDateTime timestamp =
              zoned
                  ? OffsetDateTime.parse(text)
                      .withOffsetSameInstant(ZoneOffset.UTC)
                      .toLocalDateTime()
                  : LocalDateTime.parse(text);
          if (!timestamp.equals(timestamp.truncatedTo(ChronoUnit.MICROS))) {
            throw new IllegalArgumentException("Unsupported temporal precision");
          }
          return ChronoUnit.MICROS.between(LocalDateTime.of(1970, 1, 1, 0, 0), timestamp);
        default:
          throw new IllegalArgumentException("Unsupported literal target: " + expected);
      }
    } catch (ArithmeticException | DateTimeException exception) {
      throw new IllegalArgumentException(
          "Literal cannot be represented exactly as " + expected, exception);
    }
  }

  static String canonicalValue(Object value) {
    if (value instanceof Float) {
      return Integer.toUnsignedString(Float.floatToRawIntBits((Float) value));
    }
    if (value instanceof Double) {
      return Long.toUnsignedString(Double.doubleToRawLongBits((Double) value));
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toPlainString();
    }
    return value.toString();
  }

  @SuppressWarnings("unchecked")
  static int compare(Object left, Object right) {
    if (left instanceof CharSequence) {
      return Comparators.charSequences().compare((CharSequence) left, (CharSequence) right);
    }
    if (left instanceof BigDecimal) {
      return ((BigDecimal) left).compareTo((BigDecimal) right);
    }
    if (left instanceof Float && (Float) left == 0F && (Float) right == 0F) {
      return 0;
    }
    if (left instanceof Double && (Double) left == 0D && (Double) right == 0D) {
      return 0;
    }
    return ((Comparable<Object>) left).compareTo(right);
  }

  private static void temporalPrecision(SqlType type) {
    if (type.precision() != 0 && type.precision() != 6) {
      throw new IllegalArgumentException("Only microsecond temporal precision is supported");
    }
  }

  private static void kind(Literal source, LiteralType expected) {
    if (source.literalType() != expected) {
      throw new IllegalArgumentException("Literal kind does not match target type");
    }
  }
}
