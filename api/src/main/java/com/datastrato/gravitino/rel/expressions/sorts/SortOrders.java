/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.sorts;

import com.datastrato.gravitino.rel.expressions.Expression;

/** Helper methods to create SortOrders to pass into Gravitino. */
public class SortOrders {

  /**
   * Create a sort order by the given expression with the ascending sort direction and nulls first
   * ordering.
   *
   * @param expression The expression to sort by
   * @return The created sort order
   */
  public static SortImpl ascending(Expression expression) {
    return of(expression, SortDirection.ASCENDING);
  }

  /**
   * Create a sort order by the given expression with the descending sort direction and nulls last
   * ordering.
   *
   * @param expression The expression to sort by
   * @return The created sort order
   */
  public static SortImpl descending(Expression expression) {
    return of(expression, SortDirection.DESCENDING);
  }

  /**
   * Create a sort order by the given expression with the given sort direction and default null
   * ordering.
   *
   * @param expression The expression to sort by
   * @param direction The sort direction
   * @return The created sort order
   */
  public static SortImpl of(Expression expression, SortDirection direction) {
    return of(expression, direction, direction.defaultNullOrdering());
  }

  /**
   * Create a sort order by the given expression with the given sort direction and null ordering.
   *
   * @param expression The expression to sort by
   * @param direction The sort direction
   * @param nullOrdering The null ordering
   * @return The created sort order
   */
  public static SortImpl of(
      Expression expression, SortDirection direction, NullOrdering nullOrdering) {
    return new SortImpl(expression, direction, nullOrdering);
  }

  /**
   * Create a sort order by the given expression with the given sort direction and null ordering.
   */
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

  private SortOrders() {}
}
