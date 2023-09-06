/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

// This interface may not need it
public interface SortOrder {
  Transform transform();

  Direction direction();

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
