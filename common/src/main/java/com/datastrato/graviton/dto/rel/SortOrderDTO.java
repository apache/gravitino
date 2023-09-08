/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.dto.rel;

import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
@JsonPropertyOrder({"expression", "direction", "nullOrder"})
@Getter
public class SortOrderDTO {
  public enum Direction {
    ASC,
    DESC;

    @JsonCreator
    public static Direction fromString(String value) {
      return Direction.valueOf(value.toUpperCase());
    }

    @JsonValue
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  }

  public enum NullOrder {
    FIRST,
    LAST;

    @JsonCreator
    public static NullOrder fromString(String value) {
      return NullOrder.valueOf(value.toUpperCase());
    }

    @JsonValue
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  }

  @JsonProperty("expression")
  private final Expression expression;

  @JsonProperty("direction")
  private final Direction direction;

  @JsonProperty("nullOrder")
  private final NullOrder nullOrder;

  @JsonCreator
  private SortOrderDTO(
      @JsonProperty("expression") Expression expression,
      @JsonProperty("direction") Direction direction,
      @JsonProperty("nullOrder") NullOrder nullOrder) {
    this.expression = expression;
    this.direction = direction;
    this.nullOrder = nullOrder;
  }

  public static class Builder {
    private Expression expression;
    private Direction direction;
    private NullOrder nullOrder;

    public Builder() {}

    public Builder withExpression(Expression expression) {
      this.expression = expression;
      return this;
    }

    public Builder withDirection(Direction direction) {
      this.direction = direction;
      return this;
    }

    public Builder withNullOrder(NullOrder nullOrder) {
      this.nullOrder = nullOrder;
      return this;
    }

    public SortOrderDTO build() {
      // Default direciton is ASC
      this.direction = direction == null ? Direction.ASC : direction;

      // Default is nulls first
      this.nullOrder = nullOrder == null ? NullOrder.FIRST : nullOrder;

      Preconditions.checkNotNull(this.expression, "expression cannot be null");
      return new SortOrderDTO(expression, direction, nullOrder);
    }
  }
}
