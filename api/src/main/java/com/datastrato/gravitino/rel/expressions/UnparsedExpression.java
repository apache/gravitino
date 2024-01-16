/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions;

/**
 * Represents an expression that is not parsed yet. The parsed expression is represented by {@link
 * FunctionExpression}, {@link Literal} or {@link NamedReference}.
 */
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

    @Override
    public String unparsedExpression() {
      return unparsedExpression;
    }
  }
}
