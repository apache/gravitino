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
import org.apache.gravitino.semantic.PlanFilter;
import org.apache.gravitino.semantic.SemanticQueryFilter;

/** Represents a filter in a Semantic Model query plan. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"field", "operator", "value"})
public class PlanFilterDTO {

  @JsonProperty("field")
  private final PlanFieldRefDTO field;

  @JsonProperty("operator")
  private final String operator;

  @JsonProperty("value")
  private final String value;

  /** Default constructor for Jackson deserialization. */
  public PlanFilterDTO() {
    this(null, null, null);
  }

  /**
   * Creates a plan filter.
   *
   * @param field The filtered field.
   * @param operator The comparison operator.
   * @param value The compared value as untyped text.
   */
  public PlanFilterDTO(PlanFieldRefDTO field, String operator, String value) {
    this.field = field;
    this.operator = operator;
    this.value = value;
  }

  /**
   * Validates the filter.
   *
   * @throws IllegalArgumentException If a field is missing or the operator is unsupported.
   */
  public void validate() {
    Preconditions.checkArgument(field != null, "\"field\" field is required");
    field.validate();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(operator), "\"operator\" field is required and cannot be empty");
    parseOperator();
    Preconditions.checkArgument(value != null, "\"value\" field is required and cannot be null");
  }

  /**
   * Converts this DTO to an API plan filter.
   *
   * @return The plan filter.
   * @throws IllegalArgumentException If the DTO is invalid.
   */
  public PlanFilter toPlanFilter() {
    validate();
    return PlanFilter.of(field.toPlanFieldRef(), parseOperator(), value);
  }

  /**
   * Creates a DTO from an API plan filter.
   *
   * @param filter The API plan filter.
   * @return The plan filter DTO.
   */
  public static PlanFilterDTO fromPlanFilter(PlanFilter filter) {
    return new PlanFilterDTO(
        PlanFieldRefDTO.fromPlanFieldRef(filter.field()), filter.operator().name(), filter.value());
  }

  private SemanticQueryFilter.Operator parseOperator() {
    try {
      return SemanticQueryFilter.Operator.valueOf(operator.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Unsupported filter operator '%s'", operator), e);
    }
  }
}
