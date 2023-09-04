/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.rel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
public class SimplePartitionDTO implements Partition {

  private final Strategy strategy;

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
}
