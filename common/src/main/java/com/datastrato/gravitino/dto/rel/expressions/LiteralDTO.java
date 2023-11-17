/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.expressions;

import com.datastrato.gravitino.rel.expressions.Literal;
import io.substrait.type.Type;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class LiteralDTO implements Literal<String>, FunctionArg {

  private final String value;
  private final Type dataType;

  private LiteralDTO(String value, Type dataType) {
    this.value = value;
    this.dataType = dataType;
  }

  @Override
  public String value() {
    return value;
  }

  @Override
  public Type dataType() {
    return dataType;
  }

  @Override
  public ArgType argType() {
    return ArgType.LITERAL;
  }

  public static class Builder {
    private String value;
    private Type dataType;

    public Builder withValue(String value) {
      this.value = value;
      return this;
    }

    public Builder withDataType(Type dataType) {
      this.dataType = dataType;
      return this;
    }

    public LiteralDTO build() {
      return new LiteralDTO(value, dataType);
    }
  }
}
