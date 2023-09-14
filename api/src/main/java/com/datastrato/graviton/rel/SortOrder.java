/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.transforms.Transform;

public class SortOrder {
  /**
   * Sort transform. This is the transform that is used to sort the data. Say we have a table with 3
   * columns: a, b, c. We want to sort on a. Then the sort transform is a. Generally, the sort
   * transform is a field transform.
   */
  private final Transform transform;

  /** Sort direction. Default is ASC. */
  private final Direction direction;

  /** Sort null order. Default is FIRST. */
  private final NullOrder nullOrder;

  private SortOrder(Transform transform, Direction direction, NullOrder nullOrder) {
    this.transform = transform;
    this.direction = direction;
    this.nullOrder = nullOrder;
  }

  public Transform getTransform() {
    return transform;
  }

  public Direction getDirection() {
    return direction;
  }

  public NullOrder getNullOrder() {
    return nullOrder;
  }

  public enum Direction {
    ASC,
    DESC;

    public static Direction fromString(String value) {
      return Direction.valueOf(value.toUpperCase());
    }
  }

  public enum NullOrder {
    FIRST,
    LAST;

    public static NullOrder fromString(String value) {
      return NullOrder.valueOf(value.toUpperCase());
    }
  }

  public static SortOrderBuilder builder() {
    return new SortOrderBuilder();
  }

  public static class SortOrderBuilder {
    private Direction direction;
    private NullOrder nullOrder;
    private Transform transform;

    public SortOrderBuilder withDirection(Direction direction) {
      this.direction = direction;
      return this;
    }

    public SortOrderBuilder withNullOrder(NullOrder nullOrder) {
      this.nullOrder = nullOrder;
      return this;
    }

    public SortOrderBuilder withTransform(Transform transform) {
      this.transform = transform;
      return this;
    }

    public SortOrder build() {
      if (direction == null) {
        direction = Direction.ASC;
      }
      if (nullOrder == null) {
        nullOrder = NullOrder.FIRST;
      }
      return new SortOrder(transform, direction, nullOrder);
    }
  }
}
