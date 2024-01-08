/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.literals;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
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
   * Creates an integer type literal with the given value.
   *
   * @param value the integer literal value
   * @return a new {@link Literal} instance
   */
  public static LiteralImpl<Integer> integer(Integer value) {
    return of(value, Types.IntegerType.get());
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
