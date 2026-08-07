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
package org.apache.gravitino.rel.metric;

import java.util.List;
import java.util.Objects;
import org.apache.gravitino.annotation.Unstable;

/** A semantic-model expression with one or more dialect-specific forms. */
@Unstable
public final class Expression {

  private final List<DialectExpression> dialects;

  private Expression(List<DialectExpression> dialects) {
    this.dialects = dialects;
  }

  /**
   * Creates a builder for an expression.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the dialect-specific expressions.
   *
   * @return An immutable, non-empty list of expressions.
   */
  public List<DialectExpression> dialects() {
    return dialects;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Expression)) {
      return false;
    }
    Expression that = (Expression) other;
    return Objects.equals(dialects, that.dialects);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dialects);
  }

  @Override
  public String toString() {
    return "Expression{" + "dialects=" + dialects + '}';
  }

  /** Builder for {@link Expression}. */
  public static final class Builder {
    private List<DialectExpression> dialects;

    private Builder() {}

    /**
     * Sets the dialect-specific expressions.
     *
     * @param dialects The expressions.
     * @return This builder.
     */
    public Builder withDialects(List<DialectExpression> dialects) {
      this.dialects = dialects;
      return this;
    }

    /**
     * Builds the expression.
     *
     * @return The expression.
     */
    public Expression build() {
      return new Expression(SemanticModelUtils.requireNonEmptyList(dialects, "dialects"));
    }
  }
}
