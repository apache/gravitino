/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.sorts;

import com.datastrato.gravitino.rel.expressions.Expression;

/** Helper methods to create SortOrders to pass into Gravitino. */
public class SortOrders {

  public static SortImpl of(Expression expression) {
    return of(expression, SortDirection.ASCENDING);
  }

  public static SortImpl of(Expression expression, SortDirection direction) {
    return of(expression, direction, direction.defaultNullOrdering());
  }

  public static SortImpl of(
      Expression expression, SortDirection direction, NullOrdering nullOrdering) {
    return new SortImpl(expression, direction, nullOrdering);
  }

  public static final class SortImpl implements SortOrder {
    private final Expression expression;
    private final SortDirection direction;
    private final NullOrdering nullOrdering;

    private SortImpl(Expression expression, SortDirection direction, NullOrdering nullOrdering) {
      this.expression = expression;
      this.direction = direction;
      this.nullOrdering = nullOrdering;
    }

    @Override
    public Expression expression() {
      return expression;
    }

    @Override
    public SortDirection direction() {
      return direction;
    }

    @Override
    public NullOrdering nullOrdering() {
      return nullOrdering;
    }
  }
}
