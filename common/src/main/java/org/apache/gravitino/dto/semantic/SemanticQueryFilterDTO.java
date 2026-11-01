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
import org.apache.gravitino.semantic.SemanticQueryFilter;

/** Represents one filter of a Semantic Model query. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"field", "operator", "value"})
public class SemanticQueryFilterDTO {

  @JsonProperty("field")
  private final String field;

  @JsonProperty("operator")
  private final String operator;

  @JsonProperty("value")
  private final String value;

  /** Default constructor for Jackson deserialization. */
  public SemanticQueryFilterDTO() {
    this(null, null, null);
  }

  /**
   * Creates a query filter.
   *
   * @param field The field name to filter on.
   * @param operator The comparison operator.
   * @param value The literal value to compare with.
   */
  public SemanticQueryFilterDTO(String field, String operator, String value) {
    this.field = field;
    this.operator = operator;
    this.value = value;
  }

  /**
   * Validates the filter.
   *
   * @throws IllegalArgumentException If a field is missing or the operator is unknown.
   */
  public void validate() {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(field), "\"field\" field is required and cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(operator), "\"operator\" field is required and cannot be empty");
    Preconditions.checkArgument(value != null, "\"value\" field is required and cannot be null");
    parseOperator();
  }

  /**
   * Converts this DTO to an API filter.
   *
   * @return The API filter.
   * @throws IllegalArgumentException If the filter is invalid.
   */
  public SemanticQueryFilter toFilter() {
    validate();
    return SemanticQueryFilter.of(field, parseOperator(), value);
  }

  /**
   * Creates a DTO from an API filter.
   *
   * @param filter The API filter.
   * @return The filter DTO.
   */
  public static SemanticQueryFilterDTO fromFilter(SemanticQueryFilter filter) {
    return new SemanticQueryFilterDTO(
        filter.field(), filter.operator().name().toLowerCase(Locale.ROOT), filter.value());
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
