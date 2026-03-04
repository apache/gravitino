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

package org.apache.gravitino.dto.rel;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import org.apache.gravitino.dto.rel.expressions.FunctionArg;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;

/** Data Transfer Object for SortOrder. */
@EqualsAndHashCode
@JsonSerialize(using = JsonUtils.SortOrderSerializer.class)
@JsonDeserialize(using = JsonUtils.SortOrderDeserializer.class)
public class SortOrderDTO implements SortOrder {

  /** An empty array of SortOrderDTO. */
  public static final SortOrderDTO[] EMPTY_SORT = new SortOrderDTO[0];

  private final FunctionArg sortTerm;

  private final SortDirection direction;

  private final NullOrdering nullOrdering;

  private SortOrderDTO(FunctionArg sortTerm, SortDirection direction, NullOrdering nullOrdering) {
    this.sortTerm = sortTerm;
    this.direction = direction;
    this.nullOrdering = nullOrdering;
  }

  /**
   * @return The sort term.
   */
  public FunctionArg sortTerm() {
    return sortTerm;
  }

  /**
   * @return The sort expression.
   */
  @Override
  public Expression expression() {
    return sortTerm;
  }

  /**
   * @return The sort direction.
   */
  @Override
  public SortDirection direction() {
    return direction;
  }

  /**
   * @return The null ordering.
   */
  @Override
  public NullOrdering nullOrdering() {
    return nullOrdering;
  }

  /**
   * Validates the sort order.
   *
   * @param columns The column DTOs to validate against.
   * @throws IllegalArgumentException If the sort order is invalid, this exception is thrown.
   */
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    sortTerm.validate(columns);
  }

  /** Builder for SortOrderDTO. */
  public static class Builder {
    private FunctionArg sortTerm;
    private SortDirection direction;
    private NullOrdering nullOrdering;

    /** Default constructor. */
    private Builder() {}

    /**
     * Set the sort term.
     *
     * @param sortTerm The sort term to set.
     * @return The builder.
     */
    public Builder withSortTerm(FunctionArg sortTerm) {
      this.sortTerm = sortTerm;
      return this;
    }

    /**
     * Set the sort direction.
     *
     * @param direction The sort direction to set.
     * @return The builder.
     */
    public Builder withDirection(SortDirection direction) {
      this.direction = direction;
      return this;
    }

    /**
     * Set the null ordering.
     *
     * @param nullOrdering The null ordering to set.
     * @return The builder.
     */
    public Builder withNullOrder(NullOrdering nullOrdering) {
      this.nullOrdering = nullOrdering;
      return this;
    }

    /**
     * Builds a SortOrderDTO based on the provided builder parameters.
     *
     * @return A new SortOrderDTO instance.
     */
    public SortOrderDTO build() {
      // Default direction is ASC
      this.direction = direction == null ? SortDirection.ASCENDING : direction;

      // Default is by direction
      this.nullOrdering = nullOrdering == null ? direction.defaultNullOrdering() : nullOrdering;

      Preconditions.checkArgument(this.sortTerm != null, "expression cannot be null");
      return new SortOrderDTO(sortTerm, direction, nullOrdering);
    }
  }

  /**
   * @return the builder for creating a new instance of SortOrderDTO.
   */
  public static Builder builder() {
    return new Builder();
  }
}
