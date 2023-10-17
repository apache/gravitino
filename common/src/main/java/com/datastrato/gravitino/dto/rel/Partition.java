/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "strategy",
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = SimplePartitionDTO.class,
      names = {"identity", "year", "month", "day", "hour"}),
  @JsonSubTypes.Type(value = ListPartitionDTO.class, name = "list"),
  @JsonSubTypes.Type(value = RangePartitionDTO.class, name = "range"),
  @JsonSubTypes.Type(value = ExpressionPartitionDTO.class, name = "expression"),
})
public interface Partition {

  /** @return The strategy of partitioning */
  @JsonProperty("strategy")
  Strategy strategy();

  /**
   * Validate field referenced exists in table columns
   *
   * @param columns The columns of the table.
   * @throws IllegalArgumentException if validation fails.
   */
  void validate(ColumnDTO[] columns) throws IllegalArgumentException;

  enum Strategy {
    IDENTITY,
    YEAR,
    MONTH,
    DAY,
    HOUR,
    LIST,
    RANGE,
    EXPRESSION;

    @JsonCreator
    private static Strategy fromString(String strategy) {
      return valueOf(strategy.toUpperCase());
    }

    @JsonValue
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
}
