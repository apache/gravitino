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

import java.util.Objects;
import org.apache.gravitino.annotation.Unstable;

/** An expression written in a particular dialect. */
@Unstable
public final class DialectExpression {

  private final String dialect;
  private final String expression;

  private DialectExpression(String dialect, String expression) {
    this.dialect = dialect;
    this.expression = expression;
  }

  /**
   * Creates a builder for a dialect expression.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the expression dialect.
   *
   * @return The dialect.
   */
  public String dialect() {
    return dialect;
  }

  /**
   * Returns the expression text.
   *
   * @return The expression text.
   */
  public String expression() {
    return expression;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof DialectExpression)) {
      return false;
    }
    DialectExpression that = (DialectExpression) other;
    return Objects.equals(dialect, that.dialect) && Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dialect, expression);
  }

  @Override
  public String toString() {
    return "DialectExpression{" + "dialect=" + dialect + ", expression='" + expression + '\'' + '}';
  }

  /** Builder for {@link DialectExpression}. */
  public static final class Builder {
    private String dialect;
    private String expression;

    private Builder() {}

    /**
     * Sets the expression dialect.
     *
     * @param dialect The dialect.
     * @return This builder.
     */
    public Builder withDialect(String dialect) {
      this.dialect = dialect;
      return this;
    }

    /**
     * Sets the expression text.
     *
     * @param expression The expression text.
     * @return This builder.
     */
    public Builder withExpression(String expression) {
      this.expression = expression;
      return this;
    }

    /**
     * Builds the dialect expression.
     *
     * @return The dialect expression.
     */
    public DialectExpression build() {
      return new DialectExpression(
          SemanticModelUtils.requireNonBlank(dialect, "dialect"),
          SemanticModelUtils.requireNonBlank(expression, "expression"));
    }
  }
}
