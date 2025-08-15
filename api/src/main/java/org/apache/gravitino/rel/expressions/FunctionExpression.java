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
package org.apache.gravitino.rel.expressions;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.gravitino.annotation.Evolving;

/**
 * The interface of a function expression. A function expression is an expression that takes a
 * function name and a list of arguments.
 */
@Evolving
public interface FunctionExpression extends Expression {

  /**
   * Creates a new {@link FunctionExpression} with the given function name and arguments.
   *
   * @param functionName The name of the function
   * @param arguments The arguments to the function
   * @return The created {@link FunctionExpression}
   */
  static FuncExpressionImpl of(String functionName, Expression... arguments) {
    return new FuncExpressionImpl(functionName, arguments);
  }

  /**
   * Creates a new {@link FunctionExpression} with the given function name and no arguments.
   *
   * @param functionName The name of the function
   * @return The created {@link FunctionExpression}
   */
  static FuncExpressionImpl of(String functionName) {
    return of(functionName, EMPTY_EXPRESSION);
  }

  /** @return The transform function name. */
  String functionName();

  /** @return The arguments passed to the transform function. */
  Expression[] arguments();

  @Override
  default Expression[] children() {
    return arguments();
  }

  /** A {@link FunctionExpression} implementation */
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

    /** @return The string representation of the function expression. */
    @Override
    public String toString() {
      if (arguments.length == 0) {
        return functionName + "()";
      }
      return functionName
          + Arrays.stream(arguments)
              .map(Expression::toString)
              .collect(Collectors.joining(", ", "(", ")"));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FuncExpressionImpl that = (FuncExpressionImpl) o;
      return Objects.equals(functionName, that.functionName)
          && Arrays.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(functionName);
      result = 31 * result + Arrays.hashCode(arguments);
      return result;
    }
  }
}
