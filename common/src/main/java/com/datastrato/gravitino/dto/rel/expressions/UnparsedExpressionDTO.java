/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.expressions;

import static com.datastrato.gravitino.dto.rel.expressions.FunctionArg.ArgType.UNPARSED;

import com.datastrato.gravitino.rel.expressions.UnparsedExpression;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class UnparsedExpressionDTO implements UnparsedExpression, FunctionArg {

  private final String unparsedExpression;

  private UnparsedExpressionDTO(String unparsedExpression) {
    this.unparsedExpression = unparsedExpression;
  }

  @Override
  public String unparsedExpression() {
    return unparsedExpression;
  }

  @Override
  public ArgType argType() {
    return UNPARSED;
  }

  @Override
  public String toString() {
    return "UnparsedExpressionDTO{" + "unparsedExpression='" + unparsedExpression + '\'' + '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String unparsedExpression;

    public Builder withUnparsedExpression(String unparsedExpression) {
      this.unparsedExpression = unparsedExpression;
      return this;
    }

    public UnparsedExpressionDTO build() {
      return new UnparsedExpressionDTO(unparsedExpression);
    }
  }
}
