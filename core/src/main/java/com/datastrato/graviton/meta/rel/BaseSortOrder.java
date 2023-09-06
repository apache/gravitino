/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.meta.rel;

import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.Transform;

public class BaseSortOrder implements SortOrder {

  protected Transform transform;

  protected Direction direction;

  protected NullOrder nullOrder;

  @Override
  public Transform transform() {
    return transform;
  }

  @Override
  public Direction direction() {
    return direction;
  }

  @Override
  public NullOrder nullOrder() {
    return nullOrder;
  }

  interface Builder<SELF extends BaseSortOrder.Builder<SELF, T>, T extends BaseSortOrder> {

    SELF withTransform(Transform transform);

    SELF withDirection(Direction direction);

    SELF withNullOrder(NullOrder nullOrder);

    T build();
  }

  // Need to learn this constructor syntax
  public abstract static class BaseSortOrderBuilder<
          SELF extends BaseSortOrder.Builder<SELF, T>, T extends BaseSortOrder>
      implements BaseSortOrder.Builder<SELF, T> {
    protected Transform transform;
    protected Direction direction;
    protected NullOrder nullOrder;

    public SELF withTransform(Transform transform) {
      this.transform = transform;
      return (SELF) this;
    }

    public SELF withDirection(Direction direction) {
      this.direction = direction;
      return (SELF) this;
    }

    public SELF withNullOrder(NullOrder nullOrder) {
      this.nullOrder = nullOrder;
      return (SELF) this;
    }
  }

  public static class SortOrderBuilder
      extends BaseSortOrder.BaseSortOrderBuilder<SortOrderBuilder, BaseSortOrder> {
    public BaseSortOrder build() {
      BaseSortOrder baseSortOrder = new BaseSortOrder();
      baseSortOrder.nullOrder = nullOrder;
      baseSortOrder.direction = direction;
      baseSortOrder.transform = transform;

      return baseSortOrder;
    }
  }
}
