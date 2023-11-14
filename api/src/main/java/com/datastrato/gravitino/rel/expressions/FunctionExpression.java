/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions;

public interface FunctionExpression extends Expression {

  static FuncExpressionImpl of(String functionName, Expression... arguments) {
    return new FuncExpressionImpl(functionName, arguments);
  }

  /** Returns the transform function name. */
  String functionName();

  /** Returns the arguments passed to the transform function. */
  Expression[] arguments();

  @Override
  default Expression[] children() {
    return arguments();
  }

  final class FuncExpressionImpl implements FunctionExpression {
    private final String functionName;
    private final Expression[] arguments;

    private FuncExpressionImpl(String functionName, Expression[] arguments) {
      this.functionName = functionName;
      this.arguments = arguments;
    }

    @Override
    public String functionName() {
      return functionName;
    }

    @Override
    public Expression[] arguments() {
      return arguments;
    }
  }
}
