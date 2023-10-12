/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.rel;

import static com.datastrato.graviton.dto.rel.PartitionUtils.validateFieldExistence;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Represent some common and simple partitioning which only references single field, such as year,
 * month, day, hour, etc. Mainly for the convenience of usage.
 */
@EqualsAndHashCode(callSuper = false)
public class SimplePartitionDTO implements Partition {

  private final Strategy strategy;

  @Getter
  @JsonProperty("fieldName")
  private final String[] fieldName;

  @JsonCreator
  private SimplePartitionDTO(
      @JsonProperty(value = "strategy", required = true) String strategy,
      @JsonProperty("fieldName") String[] fieldName) {
    Preconditions.checkArgument(
        fieldName != null && fieldName.length != 0, "fieldName cannot be null or empty");

    this.strategy = Strategy.valueOf(strategy.toUpperCase());
    this.fieldName = fieldName;
  }

  @Override
  public Strategy strategy() {
    return strategy;
  }

  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    validateFieldExistence(columns, fieldName);
  }

  public static class Builder {

    private Strategy strategy;
    private String[] fieldName;

    public Builder() {}

    public Builder withFieldName(String[] fieldName) {
      this.fieldName = fieldName;
      return this;
    }

    public Builder withStrategy(Strategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public SimplePartitionDTO build() {
      Preconditions.checkArgument(strategy != null, "strategy cannot be null");
      return new SimplePartitionDTO(strategy.name(), fieldName);
    }
  }

  public static SimplePartitionDTO identity(String[] fieldName) {
    return new SimplePartitionDTO.Builder()
        .withStrategy(Partition.Strategy.IDENTITY)
        .withFieldName(fieldName)
        .build();
  }

  public static SimplePartitionDTO year(String[] fieldName) {
    return new SimplePartitionDTO.Builder()
        .withStrategy(Partition.Strategy.YEAR)
        .withFieldName(fieldName)
        .build();
  }

  public static SimplePartitionDTO month(String[] fieldName) {
    return new SimplePartitionDTO.Builder()
        .withStrategy(Partition.Strategy.MONTH)
        .withFieldName(fieldName)
        .build();
  }

  public static SimplePartitionDTO day(String[] fieldName) {
    return new SimplePartitionDTO.Builder()
        .withStrategy(Partition.Strategy.DAY)
        .withFieldName(fieldName)
        .build();
  }

  public static SimplePartitionDTO hour(String[] fieldName) {
    return new SimplePartitionDTO.Builder()
        .withStrategy(Partition.Strategy.HOUR)
        .withFieldName(fieldName)
        .build();
  }
}
