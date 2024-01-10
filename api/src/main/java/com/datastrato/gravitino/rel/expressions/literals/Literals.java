/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.literals;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;

/** The helper class to create literals to pass into Gravitino. */
public class Literals {

  /**
   * Creates a literal with the given value and data type.
   *
   * @param value the literal value
   * @param dataType the data type of the literal
   * @return a new {@link com.datastrato.gravitino.rel.expressions.Literal} instance
   * @param <T> the JVM type of value held by the literal
   */
  public static <T> LiteralImpl<T> of(T value, Type dataType) {
    return new LiteralImpl<>(value, dataType);
  }

  /**
   * Creates a boolean type literal with the given value.
   *
   * @param value the boolean literal value
   * @return a new {@link Literal} instance
   */
  public static LiteralImpl<Boolean> booleanLiteral(Boolean value) {
    return of(value, Types.BooleanType.get());
  }

  /**
   * Creates a byte type literal with the given value.
   *
   * @param value the byte literal value
   * @return a new {@link Literal} instance
   */
  public static LiteralImpl<Byte> byteLiteral(Byte value) {
    return of(value, Types.ByteType.get());
  }

  /**
   * Creates a short type literal with the given value.
   *
   * @param value the short literal value
   * @return a new {@link Literal} instance
   */
  public static LiteralImpl<Short> shortLiteral(Short value) {
    return of(value, Types.ShortType.get());
  }

  /**
   * Creates an integer type literal with the given value.
   *
   * @param value the integer literal value
   * @return a new {@link Literal} instance
   */
  public static LiteralImpl<Integer> integer(Integer value) {
    return of(value, Types.IntegerType.get());
  }

  /**
   * Creates a long type literal with the given value.
   *
   * @param value the long literal value
   * @return a new {@link Literal} instance
   */
  public static LiteralImpl<Long> longLiteral(Long value) {
    return of(value, Types.LongType.get());
  }

  /**
   * Creates a float type literal with the given value.
   *
   * @param value the float literal value
   * @return a new {@link Literal} instance
   */
  public static LiteralImpl<Float> floatLiteral(Float value) {
    return of(value, Types.FloatType.get());
  }

  /**
   * Creates a double type literal with the given value.
   *
   * @param value the double literal value
   * @return a new {@link Literal} instance
   */
  public static LiteralImpl<Double> doubleLiteral(Double value) {
    return of(value, Types.DoubleType.get());
  }

  /**
   * Creates a date type literal with the given value.
   *
   * @param value the date literal value
   * @return a new {@link Literal} instance
   */
  public static LiteralImpl<LocalDate> date(LocalDate value) {
    return of(value, Types.DateType.get());
  }

  /**
   * Creates a time type literal with the given value.
   *
   * @param value the time literal value
   * @return a new {@link Literal} instance
   */
  public static LiteralImpl<LocalTime> time(LocalTime value) {
    return of(value, Types.TimeType.get());
  }

  /**
   * Creates a timestamp type literal with the given value.
   *
   * @param value the timestamp literal value
   * @return a new {@link Literal} instance
   */
  public static LiteralImpl<LocalDateTime> timestamp(LocalDateTime value) {
    return of(value, Types.TimestampType.withoutTimeZone());
  }

  /**
   * Creates a string type literal with the given value.
   *
   * @param value the string literal value
   * @return a new {@link Literal} instance
   */
  public static LiteralImpl<String> string(String value) {
    return of(value, Types.StringType.get());
  }

  /**
   * Creates a literal with the given type value.
   *
   * @param <T> The JVM type of value held by the literal.
   */
  public static final class LiteralImpl<T> implements Literal<T> {
    private final T value;
    private final Type dataType;

    private LiteralImpl(T value, Type dataType) {
      this.value = value;
      this.dataType = dataType;
    }

    @Override
    public T value() {
      return value;
    }

    @Override
    public Type dataType() {
      return dataType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LiteralImpl<?> literal = (LiteralImpl<?>) o;
      return Objects.equals(value, literal.value) && Objects.equals(dataType, literal.dataType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, dataType);
    }
  }
}
