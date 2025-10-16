/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.dto.rel.expressions;

import lombok.EqualsAndHashCode;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;

/** Data transfer object representing a function expression. */
@EqualsAndHashCode
public class FuncExpressionDTO implements FunctionExpression, FunctionArg {
  private final String functionName;
  private final FunctionArg[] functionArgs;

  private FuncExpressionDTO(String functionName, FunctionArg[] functionArgs) {
    this.functionName = functionName;
    this.functionArgs = functionArgs;
  }

  /**
   * @return The function arguments.
   */
  public FunctionArg[] args() {
    return functionArgs;
  }

  /**
   * @return The function name.
   */
  @Override
  public String functionName() {
    return functionName;
  }

  /**
   * @return The function arguments.
   */
  @Override
  public Expression[] arguments() {
    return functionArgs;
  }

  /**
   * @return The type of the function argument.
   */
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

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Builds the function expression.
     *
     * @return The function expression.
     */
    public FuncExpressionDTO build() {
      return new FuncExpressionDTO(functionName, functionArgs);
    }
  }

  /**
   * @return the builder for creating a new instance of FuncExpressionDTO.
   */
  public static Builder builder() {
    return new Builder();
  }
}
