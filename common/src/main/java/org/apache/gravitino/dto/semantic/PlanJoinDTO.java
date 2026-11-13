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
package org.apache.gravitino.dto.semantic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.semantic.PlanJoin;

/** Represents a join between two datasets of a Semantic Model query plan. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "relationship", "fromDataset", "fromColumns", "toDataset", "toColumns"})
public class PlanJoinDTO {

  @JsonProperty("type")
  private final String type;

  @Nullable
  @JsonProperty("relationship")
  private final String relationship;

  @JsonProperty("fromDataset")
  private final String fromDataset;

  @JsonProperty("fromColumns")
  private final List<String> fromColumns;

  @JsonProperty("toDataset")
  private final String toDataset;

  @JsonProperty("toColumns")
  private final List<String> toColumns;

  /** Default constructor for Jackson deserialization. */
  public PlanJoinDTO() {
    this(null, null, null, null, null, null);
  }

  /**
   * Creates a plan join.
   *
   * @param type The join type.
   * @param relationship The originating relationship name, or {@code null} if implied.
   * @param fromDataset The dataset already in scope.
   * @param fromColumns The join columns of the source dataset.
   * @param toDataset The dataset this join brings into scope.
   * @param toColumns The join columns of the target dataset.
   */
  public PlanJoinDTO(
      String type,
      @Nullable String relationship,
      String fromDataset,
      List<String> fromColumns,
      String toDataset,
      List<String> toColumns) {
    this.type = type;
    this.relationship = relationship;
    this.fromDataset = fromDataset;
    this.fromColumns = fromColumns == null ? null : new ArrayList<>(fromColumns);
    this.toDataset = toDataset;
    this.toColumns = toColumns == null ? null : new ArrayList<>(toColumns);
  }

  /**
   * Validates the join.
   *
   * @throws IllegalArgumentException If a field is missing or the join type is unsupported.
   */
  public void validate() {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(type), "\"type\" field is required and cannot be empty");
    parseType();
    Preconditions.checkArgument(
        relationship == null || StringUtils.isNotBlank(relationship),
        "\"relationship\" field cannot be empty when present");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(fromDataset),
        "\"fromDataset\" field is required and cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(toDataset), "\"toDataset\" field is required and cannot be empty");
    Preconditions.checkArgument(
        fromColumns != null && !fromColumns.isEmpty(),
        "\"fromColumns\" field is required and cannot be empty");
    Preconditions.checkArgument(
        toColumns != null && !toColumns.isEmpty(),
        "\"toColumns\" field is required and cannot be empty");
    Preconditions.checkArgument(
        fromColumns.size() == toColumns.size(),
        "\"fromColumns\" and \"toColumns\" must have the same length");
    for (int index = 0; index < fromColumns.size(); index++) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(fromColumns.get(index)),
          "\"fromColumns\"[%s] field cannot be null or empty",
          index);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(toColumns.get(index)),
          "\"toColumns\"[%s] field cannot be null or empty",
          index);
    }
  }

  /**
   * Converts this DTO to an API plan join.
   *
   * @return The plan join.
   * @throws IllegalArgumentException If the DTO is invalid.
   */
  public PlanJoin toPlanJoin() {
    validate();
    return PlanJoin.of(
        parseType(),
        relationship,
        fromDataset,
        fromColumns.toArray(new String[0]),
        toDataset,
        toColumns.toArray(new String[0]));
  }

  /**
   * Creates a DTO from an API plan join.
   *
   * @param join The API plan join.
   * @return The plan join DTO.
   */
  public static PlanJoinDTO fromPlanJoin(PlanJoin join) {
    return new PlanJoinDTO(
        join.type().name(),
        join.relationship(),
        join.fromDataset(),
        Arrays.asList(join.fromColumns()),
        join.toDataset(),
        Arrays.asList(join.toColumns()));
  }

  private PlanJoin.Type parseType() {
    try {
      return PlanJoin.Type.valueOf(type.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Unsupported join type '%s'", type), e);
    }
  }
}
