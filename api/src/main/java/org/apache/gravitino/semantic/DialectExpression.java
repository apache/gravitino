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
package org.apache.gravitino.semantic;

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/** An expression written in a specific dialect. */
@Evolving
public final class DialectExpression {

  private final String dialect;
  private final String expression;

  private DialectExpression(Builder builder) {
    this.dialect = builder.dialect;
    this.expression = builder.expression;
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
   * Returns the dialect in which the expression is written.
   *
   * @return The expression dialect identifier.
   */
  public String dialect() {
    return dialect;
  }

  /**
   * Returns the dialect-specific expression text.
   *
   * @return The non-empty expression text.
   */
  public String expression() {
    return expression;
  }

  /**
   * Compares this dialect expression with another object.
   *
   * @param other The object to compare.
   * @return {@code true} if the object has the same dialect and expression text.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof DialectExpression)) {
      return false;
    }
    DialectExpression that = (DialectExpression) other;
    return dialect.equals(that.dialect) && expression.equals(that.expression);
  }

  /**
   * Returns the hash code of this dialect expression.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(dialect, expression);
  }

  /**
   * Returns a string representation of this dialect expression.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "DialectExpression{" + "dialect=" + dialect + ", expression='" + expression + '\'' + '}';
  }

  /** A builder for {@link DialectExpression}. */
  public static final class Builder {

    private String dialect;
    private String expression;

    private Builder() {}

    /**
     * Sets the expression dialect.
     *
     * @param dialect The non-empty expression dialect identifier.
     * @return This builder.
     */
    public Builder withDialect(String dialect) {
      this.dialect = dialect;
      return this;
    }

    /**
     * Sets the dialect-specific expression text.
     *
     * @param expression The non-empty expression text.
     * @return This builder.
     */
    public Builder withExpression(String expression) {
      this.expression = expression;
      return this;
    }

    /**
     * Builds a dialect expression.
     *
     * @return The immutable dialect expression.
     * @throws IllegalArgumentException If the dialect or expression is null or empty.
     */
    public DialectExpression build() {
      Preconditions.checkArgument(
          dialect != null && !dialect.isEmpty(), "dialect must not be null or empty");
      Preconditions.checkArgument(
          expression != null && !expression.isEmpty(), "expression must not be null or empty");
      return new DialectExpression(this);
    }
  }
}
