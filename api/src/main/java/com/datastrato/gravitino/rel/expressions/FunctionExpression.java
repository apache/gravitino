/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions;

public interface FunctionExpression extends Expression {

  static FuncExpressionValue of(String functionName, Expression... arguments) {
    return new FuncExpressionValue(functionName, arguments);
  }

  /** Returns the transform function name. */
  String functionName();

  /** Returns the arguments passed to the transform function. */
  Expression[] arguments();

  @Override
  default Expression[] children() {
    return arguments();
  }

  final class FuncExpressionValue implements FunctionExpression {
    private final String functionName;
    private final Expression[] arguments;

    private FuncExpressionValue(String functionName, Expression[] arguments) {
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
