/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.transforms.Transform;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class SortOrder {
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
}
