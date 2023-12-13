/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.expressions;

import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class FuncExpressionDTO implements FunctionExpression, FunctionArg {
  private final String functionName;
  private final FunctionArg[] functionArgs;

  private FuncExpressionDTO(String functionName, FunctionArg[] functionArgs) {
    this.functionName = functionName;
    this.functionArgs = functionArgs;
  }

  public FunctionArg[] args() {
    return functionArgs;
  }

  @Override
  public String functionName() {
    return functionName;
  }

  @Override
  public Expression[] arguments() {
    return functionArgs;
  }

  @Override
  public ArgType argType() {
    return ArgType.FUNCTION;
  }

  public static class Builder {
    private String functionName;
    private FunctionArg[] functionArgs;

    public Builder withFunctionName(String functionName) {
      this.functionName = functionName;
      return this;
    }

    public Builder withFunctionArgs(FunctionArg... functionArgs) {
      this.functionArgs = functionArgs;
      return this;
    }

    public FuncExpressionDTO build() {
      return new FuncExpressionDTO(functionName, functionArgs);
    }
  }
}
