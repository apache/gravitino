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
import java.util.List;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.semantic.PlanFilter;
import org.apache.gravitino.semantic.PlanJoin;
import org.apache.gravitino.semantic.PlanSelection;
import org.apache.gravitino.semantic.PlanSource;
import org.apache.gravitino.semantic.SemanticQueryPlan;

/** Represents the dialect-neutral logical plan resolved from a Semantic Model query. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "semanticModel",
  "semanticModelVersion",
  "sources",
  "joins",
  "selections",
  "filters",
  "groupBy",
  "limit",
  "warnings"
})
public class SemanticQueryPlanDTO {

  @JsonProperty("semanticModel")
  private final List<String> semanticModel;

  @JsonProperty("semanticModelVersion")
  private final long semanticModelVersion;

  @JsonProperty("sources")
  private final List<PlanSourceDTO> sources;

  @JsonProperty("joins")
  private final List<PlanJoinDTO> joins;

  @JsonProperty("selections")
  private final List<PlanSelectionDTO> selections;

  @JsonProperty("filters")
  private final List<PlanFilterDTO> filters;

  @JsonProperty("groupBy")
  private final List<String> groupBy;

  @Nullable
  @JsonProperty("limit")
  private final Long limit;

  @JsonProperty("warnings")
  private final List<String> warnings;

  /** Default constructor for Jackson deserialization. */
  public SemanticQueryPlanDTO() {
    this(null, 0, null, null, null, null, null, null, null);
  }

  /**
   * Creates a plan DTO.
   *
   * @param semanticModel The complete Semantic Model identifier levels.
   * @param semanticModelVersion The committed Semantic Model version the plan was resolved against.
   * @param sources The datasets required by the plan.
   * @param joins The joins that connect the sources.
   * @param selections The selected metrics and dimensions.
   * @param filters The filters, ANDed together.
   * @param groupBy The non-aggregate selection names.
   * @param limit The row limit, or {@code null} for no limit.
   * @param warnings Non-fatal warnings.
   */
  public SemanticQueryPlanDTO(
      List<String> semanticModel,
      long semanticModelVersion,
      List<PlanSourceDTO> sources,
      List<PlanJoinDTO> joins,
      List<PlanSelectionDTO> selections,
      List<PlanFilterDTO> filters,
      List<String> groupBy,
      @Nullable Long limit,
      List<String> warnings) {
    this.semanticModel = semanticModel == null ? null : new ArrayList<>(semanticModel);
    this.semanticModelVersion = semanticModelVersion;
    this.sources = sources == null ? null : new ArrayList<>(sources);
    this.joins = joins == null ? null : new ArrayList<>(joins);
    this.selections = selections == null ? null : new ArrayList<>(selections);
    this.filters = filters == null ? null : new ArrayList<>(filters);
    this.groupBy = groupBy == null ? null : new ArrayList<>(groupBy);
    this.limit = limit;
    this.warnings = warnings == null ? null : new ArrayList<>(warnings);
  }

  /**
   * Validates the plan.
   *
   * @throws IllegalArgumentException If the plan violates the wire contract.
   */
  public void validate() {
    Preconditions.checkArgument(
        semanticModel != null && !semanticModel.isEmpty(),
        "\"semanticModel\" field is required and cannot be empty");
    for (int index = 0; index < semanticModel.size(); index++) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(semanticModel.get(index)),
          "\"semanticModel\"[%s] field cannot be null or empty",
          index);
    }
    Preconditions.checkArgument(
        semanticModelVersion > 0, "\"semanticModelVersion\" must be positive");
    Preconditions.checkArgument(
        sources != null && !sources.isEmpty(), "\"sources\" field is required and cannot be empty");
    for (int index = 0; index < sources.size(); index++) {
      Preconditions.checkArgument(sources.get(index) != null, "\"sources\"[%s] is required", index);
      sources.get(index).validate();
    }
    Preconditions.checkArgument(joins != null, "\"joins\" field is required");
    for (int index = 0; index < joins.size(); index++) {
      Preconditions.checkArgument(joins.get(index) != null, "\"joins\"[%s] is required", index);
      joins.get(index).validate();
    }
    Preconditions.checkArgument(
        selections != null && !selections.isEmpty(),
        "\"selections\" field is required and cannot be empty");
    for (int index = 0; index < selections.size(); index++) {
      Preconditions.checkArgument(
          selections.get(index) != null, "\"selections\"[%s] is required", index);
      selections.get(index).validate();
    }
    Preconditions.checkArgument(filters != null, "\"filters\" field is required");
    for (int index = 0; index < filters.size(); index++) {
      Preconditions.checkArgument(filters.get(index) != null, "\"filters\"[%s] is required", index);
      filters.get(index).validate();
    }
    Preconditions.checkArgument(groupBy != null, "\"groupBy\" field is required");
    for (int index = 0; index < groupBy.size(); index++) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(groupBy.get(index)),
          "\"groupBy\"[%s] field cannot be null or empty",
          index);
    }
    Preconditions.checkArgument(
        limit == null || limit >= 0, "\"limit\" must be null or non-negative");
    Preconditions.checkArgument(warnings != null, "\"warnings\" field is required");
    for (int index = 0; index < warnings.size(); index++) {
      Preconditions.checkArgument(
          warnings.get(index) != null, "\"warnings\"[%s] cannot be null", index);
    }
  }

  /**
   * Converts this DTO to an API plan.
   *
   * @return The plan.
   * @throws IllegalArgumentException If the DTO is invalid.
   */
  public SemanticQueryPlan toPlan() {
    validate();
    List<PlanSource> planSources = new ArrayList<>(sources.size());
    for (PlanSourceDTO source : sources) {
      planSources.add(source.toPlanSource());
    }
    List<PlanJoin> planJoins = new ArrayList<>(joins.size());
    for (PlanJoinDTO join : joins) {
      planJoins.add(join.toPlanJoin());
    }
    List<PlanSelection> planSelections = new ArrayList<>(selections.size());
    for (PlanSelectionDTO selection : selections) {
      planSelections.add(selection.toPlanSelection());
    }
    List<PlanFilter> planFilters = new ArrayList<>(filters.size());
    for (PlanFilterDTO filter : filters) {
      planFilters.add(filter.toPlanFilter());
    }
    return SemanticQueryPlan.builder()
        .withSemanticModel(NameIdentifier.of(semanticModel.toArray(new String[0])))
        .withSemanticModelVersion(semanticModelVersion)
        .withSources(planSources)
        .withJoins(planJoins)
        .withSelections(planSelections)
        .withFilters(planFilters)
        .withGroupBy(new ArrayList<>(groupBy))
        .withLimit(limit)
        .withWarnings(new ArrayList<>(warnings))
        .build();
  }

  /**
   * Creates a DTO from an API plan.
   *
   * @param plan The API plan.
   * @return The plan DTO.
   */
  public static SemanticQueryPlanDTO fromPlan(SemanticQueryPlan plan) {
    NameIdentifier identifier = plan.semanticModel();
    List<String> levels = new ArrayList<>();
    for (String level : identifier.namespace().levels()) {
      levels.add(level);
    }
    levels.add(identifier.name());

    List<PlanSourceDTO> planSources = new ArrayList<>(plan.sources().size());
    for (PlanSource source : plan.sources()) {
      planSources.add(PlanSourceDTO.fromPlanSource(source));
    }
    List<PlanJoinDTO> planJoins = new ArrayList<>(plan.joins().size());
    for (PlanJoin join : plan.joins()) {
      planJoins.add(PlanJoinDTO.fromPlanJoin(join));
    }
    List<PlanSelectionDTO> planSelections = new ArrayList<>(plan.selections().size());
    for (PlanSelection selection : plan.selections()) {
      planSelections.add(PlanSelectionDTO.fromPlanSelection(selection));
    }
    List<PlanFilterDTO> planFilters = new ArrayList<>(plan.filters().size());
    for (PlanFilter filter : plan.filters()) {
      planFilters.add(PlanFilterDTO.fromPlanFilter(filter));
    }
    return new SemanticQueryPlanDTO(
        levels,
        plan.semanticModelVersion(),
        planSources,
        planJoins,
        planSelections,
        planFilters,
        plan.groupBy(),
        plan.limit(),
        plan.warnings());
  }
}
