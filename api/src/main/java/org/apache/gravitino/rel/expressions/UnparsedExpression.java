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

import java.util.Objects;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.rel.expressions.literals.Literal;

/**
 * Represents an expression that is not parsed yet. The parsed expression is represented by {@link
 * FunctionExpression}, {@link Literal} or {@link NamedReference}.
 */
@Evolving
public interface UnparsedExpression extends Expression {

  /**
   * @return The unparsed expression as a string.
   */
  String unparsedExpression();

  @Override
  default Expression[] children() {
    return EMPTY_EXPRESSION;
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

    /**
     * @return The unparsed expression as a string.
     */
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

    @Override
    public String toString() {
      return "UnparsedExpressionImpl{" + "unparsedExpression='" + unparsedExpression + '\'' + '}';
    }
  }
}
