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
import java.util.Locale;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.semantic.PlanSelection;

/** Represents a selected metric or dimension in a Semantic Model query plan. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"name", "role", "aggregate", "field"})
public class PlanSelectionDTO {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("role")
  private final String role;

  @JsonProperty("aggregate")
  private final boolean aggregate;

  @JsonProperty("field")
  private final PlanFieldRefDTO field;

  /** Default constructor for Jackson deserialization. */
  public PlanSelectionDTO() {
    this(null, null, false, null);
  }

  /**
   * Creates a plan selection.
   *
   * @param name The metric or field name as requested.
   * @param role The selection role.
   * @param aggregate Whether the selection aggregates.
   * @param field The selected field.
   */
  public PlanSelectionDTO(String name, String role, boolean aggregate, PlanFieldRefDTO field) {
    this.name = name;
    this.role = role;
    this.aggregate = aggregate;
    this.field = field;
  }

  /**
   * Validates the selection.
   *
   * @throws IllegalArgumentException If a field is missing or the role is unsupported.
   */
  public void validate() {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(role), "\"role\" field is required and cannot be empty");
    PlanSelection.Role parsedRole = parseRole();
    Preconditions.checkArgument(
        aggregate == (parsedRole == PlanSelection.Role.METRIC),
        "\"aggregate\" must be true exactly for METRIC selections");
    Preconditions.checkArgument(field != null, "\"field\" field is required");
    field.validate();
  }

  /**
   * Converts this DTO to an API plan selection.
   *
   * @return The plan selection.
   * @throws IllegalArgumentException If the DTO is invalid.
   */
  public PlanSelection toPlanSelection() {
    validate();
    return PlanSelection.of(name, parseRole(), aggregate, field.toPlanFieldRef());
  }

  /**
   * Creates a DTO from an API plan selection.
   *
   * @param selection The API plan selection.
   * @return The plan selection DTO.
   */
  public static PlanSelectionDTO fromPlanSelection(PlanSelection selection) {
    return new PlanSelectionDTO(
        selection.name(),
        selection.role().name(),
        selection.aggregate(),
        PlanFieldRefDTO.fromPlanFieldRef(selection.field()));
  }

  private PlanSelection.Role parseRole() {
    try {
      return PlanSelection.Role.valueOf(role.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Unsupported selection role '%s'", role), e);
    }
  }
}
