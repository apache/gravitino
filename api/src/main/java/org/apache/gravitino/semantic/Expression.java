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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/** A Semantic Model expression with one representation per dialect identifier. */
@Evolving
public final class Expression {

  private final DialectExpression[] dialects;

  private Expression(Builder builder) {
    this.dialects = Arrays.copyOf(builder.dialects, builder.dialects.length);
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
   * Returns the ordered dialect-specific representations of this expression.
   *
   * @return A defensive copy of the non-empty dialect expression array.
   */
  public DialectExpression[] dialects() {
    return Arrays.copyOf(dialects, dialects.length);
  }

  /**
   * Compares this expression with another object.
   *
   * @param other The object to compare.
   * @return {@code true} if the object has the same ordered dialect expressions.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Expression)) {
      return false;
    }
    Expression that = (Expression) other;
    return Arrays.equals(dialects, that.dialects);
  }

  /**
   * Returns the hash code of this expression.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Arrays.hashCode(dialects);
  }

  /**
   * Returns a string representation of this expression.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "Expression{" + "dialects=" + Arrays.toString(dialects) + '}';
  }

  /** A builder for {@link Expression}. */
  public static final class Builder {

    private DialectExpression[] dialects;

    private Builder() {}

    /**
     * Sets the ordered dialect-specific representations of the expression.
     *
     * @param dialects The non-empty dialect expression array. Each dialect may occur at most once.
     * @return This builder.
     */
    public Builder withDialects(DialectExpression[] dialects) {
      this.dialects = dialects;
      return this;
    }

    /**
     * Builds an expression.
     *
     * @return The immutable expression.
     * @throws IllegalArgumentException If the array is null or empty, contains null, or contains a
     *     duplicate dialect.
     */
    public Expression build() {
      Preconditions.checkArgument(
          dialects != null && dialects.length > 0, "dialects must not be null or empty");

      Set<String> seenDialects = new HashSet<>();
      for (DialectExpression dialectExpression : dialects) {
        Preconditions.checkArgument(dialectExpression != null, "dialects must not contain null");
        Preconditions.checkArgument(
            seenDialects.add(dialectExpression.dialect()),
            "dialects must not contain duplicate dialect: %s",
            dialectExpression.dialect());
      }
      return new Expression(this);
    }
  }
}
