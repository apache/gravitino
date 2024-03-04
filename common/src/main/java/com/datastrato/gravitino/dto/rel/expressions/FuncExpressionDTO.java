/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.expressions;

import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import lombok.EqualsAndHashCode;

/** Data transfer object representing a function expression. */
@EqualsAndHashCode
public class FuncExpressionDTO implements FunctionExpression, FunctionArg {
  private final String functionName;
  private final FunctionArg[] functionArgs;

  private FuncExpressionDTO(String functionName, FunctionArg[] functionArgs) {
    this.functionName = functionName;
    this.functionArgs = functionArgs;
  }

  /** @return The function arguments. */
  public FunctionArg[] args() {
    return functionArgs;
  }

  /** @return The function name. */
  @Override
  public String functionName() {
    return functionName;
  }

  /** @return The function arguments. */
  @Override
  public Expression[] arguments() {
    return functionArgs;
  }

  /** @return The type of the function argument. */
  @Override
  public ArgType argType() {
    return ArgType.FUNCTION;
  }

  /** Builder for {@link FuncExpressionDTO}. */
  public static class Builder {
    private String functionName;
    private FunctionArg[] functionArgs;

    /**
     * Set the function name for the function expression.
     *
     * @param functionName The function name.
     * @return The builder.
     */
    public Builder withFunctionName(String functionName) {
      this.functionName = functionName;
      return this;
    }

    /**
     * Set the function arguments for the function expression.
     *
     * @param functionArgs The function arguments.
     * @return The builder.
     */
    public Builder withFunctionArgs(FunctionArg... functionArgs) {
      this.functionArgs = functionArgs;
      return this;
    }

    /**
     * Builds the function expression.
     *
     * @return The function expression.
     */
    public FuncExpressionDTO build() {
      return new FuncExpressionDTO(functionName, functionArgs);
    }
  }
}
