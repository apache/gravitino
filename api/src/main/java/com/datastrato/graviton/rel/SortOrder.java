/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.rel.transforms.Transform;

// This interface may not need it
public interface SortOrder {

  /**
   * Sort transform. This is the transform that is used to sort the data. Say we have a table with 3
   * columns: a, b, c. We want to sort on a. Then the sort transform is a. Generally, the sort
   * transform is a field transform.
   */
  Transform transform();

  /** Sort direction. Default is ASC. */
  Direction direction();

  /** Sort null order. Default is FIRST. */
  NullOrder nullOrder();

  enum Direction {
    ASC,
    DESC
  }

  enum NullOrder {
    FIRST,
    LAST
  }
}
