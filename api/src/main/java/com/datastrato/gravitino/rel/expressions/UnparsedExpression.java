/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions;

import com.datastrato.gravitino.annotation.Evolving;
import java.util.Objects;

/**
 * Represents an expression that is not parsed yet. The parsed expression is represented by {@link
 * FunctionExpression}, {@link com.datastrato.gravitino.rel.expressions.literals.Literal} or {@link
 * NamedReference}.
 */
@Evolving
public interface UnparsedExpression extends Expression {

  /** @return The unparsed expression as a string. */
  String unparsedExpression();

  @Override
  default Expression[] children() {
    return Expression.EMPTY_EXPRESSION;
  }

  /**
   * Creates a new {@link UnparsedExpression} with the given unparsed expression.
   *
   * @param unparsedExpression The unparsed expression
   * @return The created {@link UnparsedExpression}
   */
  static UnparsedExpression of(String unparsedExpression) {
    return new UnparsedExpressionImpl(unparsedExpression);
  }

  /** An {@link UnparsedExpression} implementation */
  final class UnparsedExpressionImpl implements UnparsedExpression {
    private final String unparsedExpression;

    private UnparsedExpressionImpl(String unparsedExpression) {
      this.unparsedExpression = unparsedExpression;
    }

    /** @return The unparsed expression as a string. */
    @Override
    public String unparsedExpression() {
      return unparsedExpression;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UnparsedExpressionImpl that = (UnparsedExpressionImpl) o;
      return Objects.equals(unparsedExpression, that.unparsedExpression);
    }

    @Override
    public int hashCode() {
      return Objects.hash(unparsedExpression);
    }
  }
}
