/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.dto.rel;

import com.datastrato.gravitino.dto.rel.expressions.FunctionArg;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.sorts.NullOrdering;
import com.datastrato.gravitino.rel.expressions.sorts.SortDirection;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;

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

  /** @return The sort term. */
  public FunctionArg sortTerm() {
    return sortTerm;
  }

  /** @return The sort expression. */
  @Override
  public Expression expression() {
    return sortTerm;
  }

  /** @return The sort direction. */
  @Override
  public SortDirection direction() {
    return direction;
  }

  /** @return The null ordering. */
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
    public Builder() {}

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

      Preconditions.checkNotNull(this.sortTerm, "expression cannot be null");
      return new SortOrderDTO(sortTerm, direction, nullOrdering);
    }
  }
}
