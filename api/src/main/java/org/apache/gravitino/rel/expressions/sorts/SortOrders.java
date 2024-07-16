/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.rel.expressions.sorts;

import java.util.Objects;
import org.apache.gravitino.rel.expressions.Expression;

/** Helper methods to create SortOrders to pass into Apache Gravitino. */
public class SortOrders {

  /** NONE is used to indicate that there is no sort order. */
  public static final SortOrder[] NONE = new SortOrder[0];

  /**
   * Create a sort order by the given expression with the ascending sort direction and nulls first
   * ordering.
   *
   * @param expression The expression to sort by
   * @return The created sort order
   */
  public static SortImpl ascending(Expression expression) {
    return of(expression, SortDirection.ASCENDING);
  }

  /**
   * Create a sort order by the given expression with the descending sort direction and nulls last
   * ordering.
   *
   * @param expression The expression to sort by
   * @return The created sort order
   */
  public static SortImpl descending(Expression expression) {
    return of(expression, SortDirection.DESCENDING);
  }

  /**
   * Create a sort order by the given expression with the given sort direction and default null
   * ordering.
   *
   * @param expression The expression to sort by
   * @param direction The sort direction
   * @return The created sort order
   */
  public static SortImpl of(Expression expression, SortDirection direction) {
    return of(expression, direction, direction.defaultNullOrdering());
  }

  /**
   * Create a sort order by the given expression with the given sort direction and null ordering.
   *
   * @param expression The expression to sort by
   * @param direction The sort direction
   * @param nullOrdering The null ordering
   * @return The created sort order
   */
  public static SortImpl of(
      Expression expression, SortDirection direction, NullOrdering nullOrdering) {
    return new SortImpl(expression, direction, nullOrdering);
  }

  /**
   * Create a sort order by the given expression with the given sort direction and null ordering.
   */
  public static final class SortImpl implements SortOrder {
    private final Expression expression;
    private final SortDirection direction;
    private final NullOrdering nullOrdering;

    private SortImpl(Expression expression, SortDirection direction, NullOrdering nullOrdering) {
      this.expression = expression;
      this.direction = direction;
      this.nullOrdering = nullOrdering;
    }

    @Override
    public Expression expression() {
      return expression;
    }

    @Override
    public SortDirection direction() {
      return direction;
    }

    @Override
    public NullOrdering nullOrdering() {
      return nullOrdering;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SortImpl sort = (SortImpl) o;
      return Objects.equals(expression, sort.expression)
          && direction == sort.direction
          && nullOrdering == sort.nullOrdering;
    }

    @Override
    public int hashCode() {
      return Objects.hash(expression, direction, nullOrdering);
    }

    @Override
    public String toString() {
      return "SortImpl{"
          + "expression="
          + expression
          + ", direction="
          + direction
          + ", nullOrdering="
          + nullOrdering
          + '}';
    }
  }

  private SortOrders() {}
}
