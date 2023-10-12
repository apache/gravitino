/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.rel;

import static com.datastrato.graviton.dto.rel.PartitionUtils.validateFieldExistence;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.time.LocalDateTime;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.logging.log4j.util.Strings;

@EqualsAndHashCode(callSuper = false)
public class RangePartitionDTO implements Partition {

  @Getter
  @JsonProperty("fieldName")
  private final String[] fieldName;

  @JsonProperty("ranges")
  private final Range[] ranges;

  @JsonCreator
  private RangePartitionDTO(
      @JsonProperty("strategy") String strategy,
      @JsonProperty("fieldName") String[] fieldName,
      @JsonProperty("ranges") Range[] ranges) {
    Preconditions.checkArgument(
        fieldName != null && fieldName.length != 0, "fieldName cannot be null or empty");

    this.fieldName = fieldName;
    this.ranges = ranges;
  }

  @Override
  public Strategy strategy() {
    return Strategy.RANGE;
  }

  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    validateFieldExistence(columns, fieldName);
  }

  @EqualsAndHashCode
  private static class Range {

    @JsonProperty("name")
    private final String name;

    @JsonProperty("lower")
    private final String lower;

    @JsonProperty("upper")
    private final String upper;

    @JsonCreator
    private Range(
        @JsonProperty("name") String name,
        @JsonProperty("lower") String lower,
        @JsonProperty("upper") String upper) {
      Preconditions.checkArgument(Strings.isNotBlank(name), "Range name cannot be null or empty");
      Preconditions.checkArgument(
          validateDatetimeLiteral(lower),
          "Range boundary(lower) only supports ISO date-time format literal");
      Preconditions.checkArgument(
          validateDatetimeLiteral(upper),
          "Range boundary(upper) only supports ISO date-time format literal");

      this.name = name;
      this.lower = lower;
      this.upper = upper;
    }

    private boolean validateDatetimeLiteral(String literal) {
      if (literal == null) {
        return true;
      }
      try {
        LocalDateTime.parse(literal);
      } catch (Exception e) {
        return false;
      }
      return true;
    }
  }

  public static class Builder {

    private String[] fieldName;

    private final List<Range> ranges = Lists.newArrayList();

    public Builder() {}

    public Builder withFieldName(String[] fieldName) {
      this.fieldName = fieldName;
      return this;
    }

    public Builder withRange(String partitionName, String lower, String upper) {
      ranges.add(new Range(partitionName, lower, upper));
      return this;
    }

    public RangePartitionDTO build() {
      return new RangePartitionDTO(Strategy.RANGE.name(), fieldName, ranges.toArray(new Range[0]));
    }
  }
}
