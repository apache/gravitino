/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions.sorts;

import static com.datastrato.gravitino.rel.expressions.sorts.NullOrdering.NULLS_FIRST;
import static com.datastrato.gravitino.rel.expressions.sorts.NullOrdering.NULLS_LAST;

public enum SortDirection {
  ASCENDING(NULLS_FIRST),
  DESCENDING(NULLS_LAST);

  private final NullOrdering defaultNullOrdering;

  SortDirection(NullOrdering defaultNullOrdering) {
    this.defaultNullOrdering = defaultNullOrdering;
  }

  /** Returns the default null ordering to use if no null ordering is specified explicitly. */
  public NullOrdering defaultNullOrdering() {
    return defaultNullOrdering;
  }

  @Override
  public String toString() {
    switch (this) {
      case ASCENDING:
        return "asc";
      case DESCENDING:
        return "desc";
      default:
        throw new IllegalArgumentException("Unexpected sort direction: " + this);
    }
  }

  public static SortDirection fromString(String str) {
    switch (str.toLowerCase()) {
      case "asc":
        return ASCENDING;
      case "desc":
        return DESCENDING;
      default:
        throw new IllegalArgumentException("Unexpected sort direction: " + str);
    }
  }
}
