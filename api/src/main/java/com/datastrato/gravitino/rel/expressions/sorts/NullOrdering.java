/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.sorts;

public enum NullOrdering {
  NULLS_FIRST,
  NULLS_LAST;

  @Override
  public String toString() {
    switch (this) {
      case NULLS_FIRST:
        return "nulls_first";
      case NULLS_LAST:
        return "nulls_last";
      default:
        throw new IllegalArgumentException("Unexpected null order: " + this);
    }
  }
}
