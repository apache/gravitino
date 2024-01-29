/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import java.util.Map;
import java.util.Objects;

public class RangePartitionDTO implements PartitionDTO {

  private final String name;
  private final Map<String, String> properties;
  private final LiteralDTO upper;
  private final LiteralDTO lower;

  public static Builder builder() {
    return new Builder();
  }

  private RangePartitionDTO() {
    this(null, null, null, null);
  }

  private RangePartitionDTO(
      String name, Map<String, String> properties, LiteralDTO upper, LiteralDTO lower) {
    this.name = name;
    this.properties = properties;
    this.upper = upper;
    this.lower = lower;
  }

  @Override
  public String name() {
    return name;
  }

  public LiteralDTO upper() {
    return upper;
  }

  public LiteralDTO lower() {
    return lower;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RangePartitionDTO that = (RangePartitionDTO) o;
    return Objects.equals(name, that.name)
        && Objects.equals(properties, that.properties)
        && Objects.equals(upper, that.upper)
        && Objects.equals(lower, that.lower);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, properties, upper, lower);
  }

  @Override
  public Type type() {
    return Type.RANGE;
  }

  public static class Builder {
    private String name;
    private Map<String, String> properties;
    private LiteralDTO upper;
    private LiteralDTO lower;

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    public Builder withUpper(LiteralDTO upper) {
      this.upper = upper;
      return this;
    }

    public Builder withLower(LiteralDTO lower) {
      this.lower = lower;
      return this;
    }

    public RangePartitionDTO build() {
      return new RangePartitionDTO(name, properties, upper, lower);
    }
  }
}
