/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.expressions;

import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import lombok.EqualsAndHashCode;

/** Represents a Literal Data Transfer Object (DTO) that implements the Literal interface. */
@EqualsAndHashCode
public class LiteralDTO implements Literal<String>, FunctionArg {

  /** A instance of LiteralDTO with a value of "NULL" and a data type of Types.NullType.get(). */
  public static final LiteralDTO NULL = new LiteralDTO("NULL", Types.NullType.get());

  private final String value;
  private final Type dataType;

  private LiteralDTO(String value, Type dataType) {
    this.value = value;
    this.dataType = dataType;
  }

  /** @return The value of the literal. */
  @Override
  public String value() {
    return value;
  }

  /** @return The data type of the literal. */
  @Override
  public Type dataType() {
    return dataType;
  }

  /** @return The type of the argument. */
  @Override
  public ArgType argType() {
    return ArgType.LITERAL;
  }

  @Override
  public String toString() {
    return "LiteralDTO{" + "value='" + value + '\'' + ", dataType=" + dataType + '}';
  }

  /** Builder for LiteralDTO. */
  public static class Builder {
    private String value;
    private Type dataType;

    /**
     * Set the value of the literal.
     *
     * @param value The value of the literal.
     * @return The builder.
     */
    public Builder withValue(String value) {
      this.value = value;
      return this;
    }

    /**
     * Set the data type of the literal.
     *
     * @param dataType The data type of the literal.
     * @return The builder.
     */
    public Builder withDataType(Type dataType) {
      this.dataType = dataType;
      return this;
    }

    /**
     * Builds a LiteralDTO instance.
     *
     * @return The LiteralDTO instance.
     */
    public LiteralDTO build() {
      return new LiteralDTO(value, dataType);
    }
  }
}
