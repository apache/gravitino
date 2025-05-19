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
package org.apache.gravitino.rel.expressions.distributions;

import java.util.Arrays;
import java.util.Objects;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;

/** Helper methods to create distributions to pass into Apache Gravitino. */
public class Distributions {

  /**
   * AUTO indicates that the number of buckets is automatically determined by the system (without
   * the need for manual specification).
   */
  public static final int AUTO = -1;

  /** NONE is used to indicate that there is no distribution. */
  public static final Distribution NONE =
      new DistributionImpl(Strategy.NONE, 0, Expression.EMPTY_EXPRESSION);

  /** List bucketing strategy hash, TODO: #1505 Separate the bucket number from the Distribution. */
  public static final Distribution HASH =
      new DistributionImpl(Strategy.HASH, 0, Expression.EMPTY_EXPRESSION);

  /**
   * List bucketing strategy range, TODO: #1505 Separate the bucket number from the Distribution.
   */
  public static final Distribution RANGE =
      new DistributionImpl(Strategy.RANGE, 0, Expression.EMPTY_EXPRESSION);

  /**
   * Create a distribution by evenly distributing the data across the number of buckets.
   *
   * @param number The number of buckets
   * @param expressions The expressions to distribute by
   * @return The created even distribution
   */
  public static Distribution even(int number, Expression... expressions) {
    return new DistributionImpl(Strategy.EVEN, number, expressions);
  }

  /**
   * Create a distribution by hashing the data across the number of buckets.
   *
   * @param number The number of buckets
   * @param expressions The expressions to distribute by
   * @return The created hash distribution
   */
  public static Distribution hash(int number, Expression... expressions) {
    return new DistributionImpl(Strategy.HASH, number, expressions);
  }

  /**
   * Create a distribution by the given strategy.
   *
   * @param strategy The strategy to use
   * @param number The number of buckets
   * @param expressions The expressions to distribute by
   * @return The created distribution
   */
  public static Distribution of(Strategy strategy, int number, Expression... expressions) {
    return new DistributionImpl(strategy, number, expressions);
  }

  /**
   * Create a distribution by the given strategy, the number of buckets is not used.
   *
   * @param strategy The strategy to use
   * @param expressions The expressions to distribute by
   * @return The created distribution
   */
  public static Distribution auto(Strategy strategy, Expression... expressions) {
    return new DistributionImpl(strategy, AUTO, expressions);
  }

  /**
   * Create a distribution on columns. Like distribute by (a) or (a, b), for complex like
   * distributing by (func(a), b) or (func(a), func(b)), please use {@link DistributionImpl.Builder}
   * to create.
   *
   * <pre>
   *   NOTE: a, b, c are column names.
   *
   *   SQL syntax: distribute by hash(a, b) buckets 5
   *   fields(Strategy.HASH, 5, new String[]{"a"}, new String[]{"b"});
   *
   *   SQL syntax: distribute by hash(a, b, c) buckets 10
   *   fields(Strategy.HASH, 10, new String[]{"a"}, new String[]{"b"}, new String[]{"c"});
   *
   *   SQL syntax: distribute by EVEN(a) buckets 128
   *   fields(Strategy.EVEN, 128, new String[]{"a"});
   * </pre>
   *
   * @param strategy The strategy to use.
   * @param number The number of buckets.
   * @param fieldNames The field names to distribute by.
   * @return The created distribution.
   */
  public static Distribution fields(Strategy strategy, int number, String[]... fieldNames) {
    Expression[] expressions =
        Arrays.stream(fieldNames).map(NamedReference::field).toArray(Expression[]::new);
    return of(strategy, number, expressions);
  }

  /**
   * Create a distribution on columns. Like distribute by (a) or (a, b), for complex like
   * distributing by (func(a), b) or (func(a), func(b)), please use {@link DistributionImpl.Builder}
   */
  public static final class DistributionImpl implements Distribution {
    private final Strategy strategy;
    private final int number;
    private final Expression[] expressions;

    private DistributionImpl(Strategy strategy, int number, Expression[] expressions) {
      this.strategy = strategy;
      this.number = number;
      this.expressions = expressions;
    }

    /**
     * Get the strategy of the distribution.
     *
     * @return The strategy of the distribution.
     */
    @Override
    public Strategy strategy() {
      return strategy;
    }

    /**
     * Get the number of buckets of the distribution.
     *
     * @return The number of buckets of the distribution.
     */
    @Override
    public int number() {
      return number;
    }

    /**
     * Get the expressions of the distribution.
     *
     * @return The expressions of the distribution.
     */
    @Override
    public Expression[] expressions() {
      return expressions;
    }

    @Override
    public String toString() {
      return "DistributionImpl{"
          + "strategy="
          + strategy
          + ", number="
          + number
          + ", expressions="
          + Arrays.toString(expressions)
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DistributionImpl that = (DistributionImpl) o;
      return number == that.number
          && strategy == that.strategy
          && Arrays.equals(expressions, that.expressions);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(strategy, number);
      result = 31 * result + Arrays.hashCode(expressions);
      return result;
    }

    /** Builder to create a distribution. */
    public static class Builder {
      private Strategy strategy;
      private int number;
      private Expression[] expressions;

      /**
       * Set the strategy of the distribution.
       *
       * @param strategy The strategy of the distribution.
       * @return The builder.
       */
      public Builder withStrategy(Strategy strategy) {
        this.strategy = strategy;
        return this;
      }

      /**
       * Set the number of buckets of the distribution.
       *
       * @param number The number of buckets of the distribution.
       * @return The builder.
       */
      public Builder withNumber(int number) {
        this.number = number;
        return this;
      }

      /**
       * Set the expressions of the distribution.
       *
       * @param expressions The expressions of the distribution.
       * @return The builder.
       */
      public Builder withExpressions(Expression[] expressions) {
        this.expressions = expressions;
        return this;
      }

      /**
       * Build the distribution.
       *
       * @return The created distribution.
       */
      public Distribution build() {
        return new DistributionImpl(strategy, number, expressions);
      }
    }
  }

  private Distributions() {}
}
