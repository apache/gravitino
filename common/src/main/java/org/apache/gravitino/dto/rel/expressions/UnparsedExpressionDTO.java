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
import org.apache.gravitino.rel.expressions.UnparsedExpression;

/** Data transfer object representing an unparsed expression. */
@EqualsAndHashCode
public class UnparsedExpressionDTO implements UnparsedExpression, FunctionArg {

  private final String unparsedExpression;

  private UnparsedExpressionDTO(String unparsedExpression) {
    this.unparsedExpression = unparsedExpression;
  }

  /**
   * @return The value of the unparsed expression.
   */
  @Override
  public String unparsedExpression() {
    return unparsedExpression;
  }

  /**
   * @return The type of the function argument.
   */
  @Override
  public ArgType argType() {
    return ArgType.UNPARSED;
  }

  @Override
  public String toString() {
    return "UnparsedExpressionDTO{" + "unparsedExpression='" + unparsedExpression + '\'' + '}';
  }

  /**
   * @return A builder instance for {@link UnparsedExpressionDTO}.
   */
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
