/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Referred from Apache Spark's connector/catalog implementation
// sql/catalyst/src/main/java/org/apache/spark/sql/connector/expressions/SortDirection.java
package com.datastrato.gravitino.rel.expressions.sorts;

import static com.datastrato.gravitino.rel.expressions.sorts.NullOrdering.NULLS_FIRST;
import static com.datastrato.gravitino.rel.expressions.sorts.NullOrdering.NULLS_LAST;

import com.datastrato.gravitino.annotation.Evolving;

/**
 * A sort direction used in sorting expressions.
 *
 * <p>Each direction has a default null ordering that is implied if no null ordering is specified
 * explicitly.
 */
@Evolving
public enum SortDirection {

  /**
   * Ascending sort direction. Nulls appear first. For ascending order, this means nulls appear at
   * the beginning.
   */
  ASCENDING(NULLS_FIRST),

  /**
   * Descending sort direction. Nulls appear last. For ascending order, this means nulls appear at
   * the end.
   */
  DESCENDING(NULLS_LAST);

  private final NullOrdering defaultNullOrdering;

  SortDirection(NullOrdering defaultNullOrdering) {
    this.defaultNullOrdering = defaultNullOrdering;
  }

  /**
   * Returns the default null ordering to use if no null ordering is specified explicitly.
   *
   * @return The default null ordering.
   */
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

  /**
   * Returns the SortDirection from the string representation.
   *
   * @param str The string representation of the sort direction.
   * @return The SortDirection.
   */
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
