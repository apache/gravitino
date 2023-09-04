/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.rel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import lombok.EqualsAndHashCode;
import org.apache.logging.log4j.util.Strings;

@EqualsAndHashCode(callSuper = false)
public class ListPartitionDTO implements Partition {
  @JsonProperty("fieldNames")
  private final String[][] fieldNames;

  @JsonProperty("assignments")
  private final Assignment[] assignments;

  @JsonCreator
  private ListPartitionDTO(
      @JsonProperty("strategy") String strategy,
      @JsonProperty("fieldNames") String[][] fieldNames,
      @JsonProperty("assignments") Assignment[] assignments) {
    Preconditions.checkArgument(
        fieldNames != null && fieldNames.length != 0, "fieldNames cannot be null or empty");

    if (assignments != null && assignments.length != 0) {
      Preconditions.checkArgument(
          Arrays.stream(assignments)
              .allMatch(
                  assignment ->
                      Arrays.stream(assignment.values)
                          .allMatch(v -> v.length == fieldNames.length)),
          "Assignment values length must be equal to field number");
    }

    this.fieldNames = fieldNames;
    this.assignments = assignments;
  }

  @Override
  public Strategy strategy() {
    return Strategy.LIST;
  }

  @EqualsAndHashCode
  public static class Assignment {

    @JsonProperty("name")
    private final String name;

    @JsonProperty("values")
    private final String[][] values;

    @JsonCreator
    private Assignment(
        @JsonProperty("name") String name, @JsonProperty("values") String[][] values) {
      Preconditions.checkArgument(
          !Strings.isBlank(name), "Assignment name cannot be null or empty");
      Preconditions.checkArgument(
          values != null && values.length != 0, "values cannot be null or empty");
      this.name = name;
      this.values = values;
    }
  }

  public static class Builder {

    private String[][] fieldNames;

    private final List<Assignment> assignments = Lists.newArrayList();

    public Builder() {}

    public Builder withFieldNames(String[][] fieldNames) {
      this.fieldNames = fieldNames;
      return this;
    }

    public Builder withAssignment(String partitionName, String[][] values) {
      assignments.add(new Assignment(partitionName, values));
      return this;
    }

    public ListPartitionDTO build() {
      return new ListPartitionDTO(
          Strategy.LIST.name(), fieldNames, assignments.toArray(new Assignment[0]));
    }
  }
}
