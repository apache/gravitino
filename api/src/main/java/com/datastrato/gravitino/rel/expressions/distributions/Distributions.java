/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.distributions;

import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import java.util.Arrays;

/** Helper methods to create distributions to pass into Gravitino. */
public class Distributions {

  // NONE is used to indicate that there is no distribution.
  public static final Distribution NONE =
      new DistributionImpl(Strategy.HASH, 0, Expression.EMPTY_EXPRESSION);

  public static Distribution ofEVEN(int number, Expression... expressions) {
    return new DistributionImpl(Strategy.EVEN, number, expressions);
  }

  public static Distribution ofHash(int number, Expression... expressions) {
    return new DistributionImpl(Strategy.HASH, number, expressions);
  }

  public static Distribution of(Strategy strategy, int number, Expression... expressions) {
    return new DistributionImpl(strategy, number, expressions);
  }

  /**
   * Create a distribution on columns. Like distribute by (a) or (a, b), for complex like
   * distributing by (func(a), b) or (func(a), func(b)), please use {@link Builder} to create.
   *
   * <pre>
   *   NOTE: a, b, c are column names.
   *
   *   SQL syntax: distribute by hash(a, b) buckets 2
   *   nameReferenceDistribution(Strategy.HASH, 2, new String[]{"a"}, new String[]{"b"});
   *
   *   SQL syntax: distribute by hash(a, b, c) buckets 3
   *   nameReferenceDistribution(Strategy.HASH, 3, new String[]{"a"}, new String[]{"b"}, new String[]{"c"});
   *
   *   SQL syntax: distribute by EVEN(a) buckets 1
   *   nameReferenceDistribution(Strategy.EVEN, 1, new String[]{"a"});
   * </pre>
   */
  public static Distribution ofFields(Strategy strategy, int number, String[] fieldNames) {
    Expression[] expressions =
        Arrays.stream(fieldNames).map(NamedReference::field).toArray(Expression[]::new);
    return of(strategy, number, expressions);
  }

  public static final class DistributionImpl implements Distribution {
    private final Strategy strategy;
    private final int number;
    private final Expression[] expressions;

    private DistributionImpl(Strategy strategy, int number, Expression[] expressions) {
      this.strategy = strategy;
      this.number = number;
      this.expressions = expressions;
    }

    @Override
    public Strategy strategy() {
      return strategy;
    }

    @Override
    public int number() {
      return number;
    }

    @Override
    public Expression[] expressions() {
      return expressions;
    }

    public static class Builder {
      private Strategy strategy;
      private int number;
      private Expression[] expressions;

      public Builder withStrategy(Strategy strategy) {
        this.strategy = strategy;
        return this;
      }

      public Builder withNumber(int number) {
        this.number = number;
        return this;
      }

      public Builder withExpressions(Expression[] expressions) {
        this.expressions = expressions;
        return this;
      }

      public Distribution build() {
        return new DistributionImpl(strategy, number, expressions);
      }
    }
  }
}
