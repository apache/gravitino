/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
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

@EqualsAndHashCode
@JsonSerialize(using = JsonUtils.SortOrderSerializer.class)
@JsonDeserialize(using = JsonUtils.SortOrderDeserializer.class)
public class SortOrderDTO implements SortOrder {
  public static final SortOrderDTO[] EMPTY_SORT = new SortOrderDTO[0];

  private final FunctionArg sortTerm;

  private final SortDirection direction;

  private final NullOrdering nullOrdering;

  private SortOrderDTO(FunctionArg sortTerm, SortDirection direction, NullOrdering nullOrdering) {
    this.sortTerm = sortTerm;
    this.direction = direction;
    this.nullOrdering = nullOrdering;
  }

  public FunctionArg sortTerm() {
    return sortTerm;
  }

  @Override
  public Expression expression() {
    return sortTerm;
  }

  @Override
  public SortDirection direction() {
    return direction;
  }

  @Override
  public NullOrdering nullOrdering() {
    return nullOrdering;
  }

  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    sortTerm.validate(columns);
  }

  public static class Builder {
    private FunctionArg sortTerm;
    private SortDirection direction;
    private NullOrdering nullOrdering;

    public Builder() {}

    public Builder withSortTerm(FunctionArg sortTerm) {
      this.sortTerm = sortTerm;
      return this;
    }

    public Builder withDirection(SortDirection direction) {
      this.direction = direction;
      return this;
    }

    public Builder withNullOrder(NullOrdering nullOrdering) {
      this.nullOrdering = nullOrdering;
      return this;
    }

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
