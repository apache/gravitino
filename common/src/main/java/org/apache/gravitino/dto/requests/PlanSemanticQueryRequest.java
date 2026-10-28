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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.semantic.SemanticQueryFilterDTO;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.semantic.SemanticQuery;
import org.apache.gravitino.semantic.SemanticQueryFilter;

/** Represents a request to plan a Semantic Model query without executing it. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"metrics", "dimensions", "filters", "dialect"})
public class PlanSemanticQueryRequest implements RESTRequest {

  @JsonProperty("metrics")
  private final List<String> metrics;

  @JsonProperty("dimensions")
  private final List<String> dimensions;

  @JsonProperty("filters")
  private final List<SemanticQueryFilterDTO> filters;

  @JsonProperty("dialect")
  private final String dialect;

  /** Default constructor for Jackson deserialization. */
  public PlanSemanticQueryRequest() {
    this(null, null, null, null);
  }

  /**
   * Creates a plan request.
   *
   * @param metrics The selected metric names, or {@code null} for none.
   * @param dimensions The selected dimension names, or {@code null} for none.
   * @param filters The filters, or {@code null} for none.
   * @param dialect The target engine dialect.
   */
  public PlanSemanticQueryRequest(
      List<String> metrics,
      List<String> dimensions,
      List<SemanticQueryFilterDTO> filters,
      String dialect) {
    this.metrics = metrics;
    this.dimensions = dimensions;
    this.filters = filters;
    this.dialect = dialect;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(dialect), "\"dialect\" field is required and cannot be empty");
    if (metrics != null) {
      for (int index = 0; index < metrics.size(); index++) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(metrics.get(index)),
            "\"metrics\"[%s] field cannot be null or empty",
            index);
      }
    }
    if (dimensions != null) {
      for (int index = 0; index < dimensions.size(); index++) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(dimensions.get(index)),
            "\"dimensions\"[%s] field cannot be null or empty",
            index);
      }
    }
    if (filters != null) {
      for (int index = 0; index < filters.size(); index++) {
        Preconditions.checkArgument(
            filters.get(index) != null, "\"filters\"[%s] field cannot be null", index);
        filters.get(index).validate();
      }
    }
    Preconditions.checkArgument(
        (metrics != null && !metrics.isEmpty()) || (dimensions != null && !dimensions.isEmpty()),
        "At least one metric or dimension must be selected");
  }

  /**
   * Converts this request to an API query.
   *
   * @return The API query.
   * @throws IllegalArgumentException If the request is invalid.
   */
  public SemanticQuery toQuery() {
    validate();
    List<SemanticQueryFilter> queryFilters =
        filters == null
            ? Collections.emptyList()
            : filters.stream().map(SemanticQueryFilterDTO::toFilter).collect(Collectors.toList());
    return SemanticQuery.builder()
        .withMetrics(metrics == null ? Collections.emptyList() : new ArrayList<>(metrics))
        .withDimensions(dimensions == null ? Collections.emptyList() : new ArrayList<>(dimensions))
        .withFilters(queryFilters)
        .withDialect(dialect)
        .build();
  }

  /**
   * Creates a request from an API query.
   *
   * @param query The API query.
   * @return The plan request.
   */
  public static PlanSemanticQueryRequest fromQuery(SemanticQuery query) {
    return new PlanSemanticQueryRequest(
        query.metrics(),
        query.dimensions(),
        query.filters().stream()
            .map(SemanticQueryFilterDTO::fromFilter)
            .collect(Collectors.toList()),
        query.dialect());
  }
}
