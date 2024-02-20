/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.expressions;

import static com.datastrato.gravitino.dto.rel.expressions.FunctionArg.ArgType.UNPARSED;

import com.datastrato.gravitino.rel.expressions.UnparsedExpression;
import lombok.EqualsAndHashCode;

/** Data transfer object representing an unparsed expression. */
@EqualsAndHashCode
public class UnparsedExpressionDTO implements UnparsedExpression, FunctionArg {

  private final String unparsedExpression;

  private UnparsedExpressionDTO(String unparsedExpression) {
    this.unparsedExpression = unparsedExpression;
  }

  /** @return The value of the unparsed expression. */
  @Override
  public String unparsedExpression() {
    return unparsedExpression;
  }

  /** @return The type of the function argument. */
  @Override
  public ArgType argType() {
    return UNPARSED;
  }

  @Override
  public String toString() {
    return "UnparsedExpressionDTO{" + "unparsedExpression='" + unparsedExpression + '\'' + '}';
  }

  /** @return A builder instance for {@link UnparsedExpressionDTO}. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link UnparsedExpressionDTO}. */
  public static class Builder {
    private String unparsedExpression;

    /**
     * Set the unparsed expression.
     *
     * @param unparsedExpression The unparsed expression.
     * @return The builder.
     */
    public Builder withUnparsedExpression(String unparsedExpression) {
      this.unparsedExpression = unparsedExpression;
      return this;
    }

    /**
     * Build the unparsed expression.
     *
     * @return The unparsed expression.
     */
    public UnparsedExpressionDTO build() {
      return new UnparsedExpressionDTO(unparsedExpression);
    }
  }
}
