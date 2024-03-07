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
// sql/catalyst/src/main/java/org/apache/spark/sql/connector/expressions/NullOrdering.java
package com.datastrato.gravitino.rel.expressions.sorts;

import com.datastrato.gravitino.annotation.Evolving;

/** A null order used in sorting expressions. */
@Evolving
public enum NullOrdering {

  /**
   * Nulls appear before non-nulls. For ascending order, this means nulls appear at the beginning,
   */
  NULLS_FIRST,

  /** Nulls appear after non-nulls. For ascending order, this means nulls appear at the end. */
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
