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
  private final NullOrdering nullOrder;

  private SortOrder(Transform transform, Direction direction, NullOrdering nullOrder) {
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

  public NullOrdering getNullOrder() {
    return nullOrder;
  }

  public enum Direction {
    ASC,
    DESC;

    public static Direction fromString(String value) {
      return Direction.valueOf(value.toUpperCase());
    }
  }

  public enum NullOrdering {
    FIRST,
    LAST;

    public static NullOrdering fromString(String value) {
      return NullOrdering.valueOf(value.toUpperCase());
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Direction direction;
    private NullOrdering nullOrder;
    private Transform transform;

    public Builder withDirection(Direction direction) {
      this.direction = direction;
      return this;
    }

    public Builder withNullOrder(NullOrdering nullOrder) {
      this.nullOrder = nullOrder;
      return this;
    }

    public Builder withTransform(Transform transform) {
      this.transform = transform;
      return this;
    }

    public SortOrder build() {
      if (direction == null) {
        direction = Direction.ASC;
      }
      if (nullOrder == null) {
        nullOrder = NullOrdering.FIRST;
      }
      return new SortOrder(transform, direction, nullOrder);
    }
  }
}
