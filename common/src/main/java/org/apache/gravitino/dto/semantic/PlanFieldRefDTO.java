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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.PlanFieldRef;

/** Represents a reference to a field of a dataset in a Semantic Model query plan. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"dataset", "expression", "column", "datatype"})
public class PlanFieldRefDTO {

  @JsonProperty("dataset")
  private final String dataset;

  @Nullable
  @JsonProperty("expression")
  private final Map<String, String> expression;

  @Nullable
  @JsonProperty("column")
  private final String column;

  @Nullable
  @JsonProperty("datatype")
  private final String datatype;

  /** Default constructor for Jackson deserialization. */
  public PlanFieldRefDTO() {
    this(null, null, null, null);
  }

  /**
   * Creates a plan field reference.
   *
   * @param dataset The dataset that provides the field.
   * @param expression The per-dialect field expressions, or {@code null} for a physical column.
   * @param column The physical column name, or {@code null} for a declared field.
   * @param datatype The logical data type name, or {@code null} if the model declares none.
   */
  public PlanFieldRefDTO(
      String dataset,
      @Nullable Map<String, String> expression,
      @Nullable String column,
      @Nullable String datatype) {
    this.dataset = dataset;
    this.expression = expression == null ? null : new LinkedHashMap<>(expression);
    this.column = column;
    this.datatype = datatype;
  }

  /**
   * Validates the field reference.
   *
   * @throws IllegalArgumentException If the DTO is not a valid field reference.
   */
  public void validate() {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(dataset), "\"dataset\" field is required and cannot be empty");
    Preconditions.checkArgument(
        (expression == null) != (column == null),
        "exactly one of \"expression\" and \"column\" must be set");
    Preconditions.checkArgument(
        column == null || StringUtils.isNotBlank(column),
        "\"column\" field cannot be empty when present");
    if (expression != null) {
      Preconditions.checkArgument(
          !expression.isEmpty(), "\"expression\" field cannot be empty when present");
      for (Map.Entry<String, String> entry : expression.entrySet()) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(entry.getKey()),
            "\"expression\" dialect names cannot be null or empty");
        Preconditions.checkArgument(
            StringUtils.isNotBlank(entry.getValue()),
            "\"expression\" for dialect '%s' cannot be null or empty",
            entry.getKey());
      }
    }
    if (datatype != null) {
      parseDataType();
    }
  }

  /**
   * Converts this DTO to an API field reference.
   *
   * @return The field reference.
   * @throws IllegalArgumentException If the DTO is invalid.
   */
  public PlanFieldRef toPlanFieldRef() {
    validate();
    if (column != null) {
      return PlanFieldRef.ofColumn(dataset, column, datatype == null ? null : parseDataType());
    }
    List<DialectExpression> expressions = new ArrayList<>();
    for (Map.Entry<String, String> entry : expression.entrySet()) {
      expressions.add(
          DialectExpression.builder()
              .withDialect(entry.getKey())
              .withExpression(entry.getValue())
              .build());
    }
    return PlanFieldRef.ofExpression(
        dataset,
        Expression.builder().withDialects(expressions.toArray(new DialectExpression[0])).build(),
        datatype == null ? null : parseDataType());
  }

  /**
   * Creates a DTO from an API field reference.
   *
   * @param field The API field reference.
   * @return The field reference DTO.
   */
  public static PlanFieldRefDTO fromPlanFieldRef(PlanFieldRef field) {
    Map<String, String> expressions = null;
    if (field.expression() != null) {
      expressions = new LinkedHashMap<>();
      for (DialectExpression dialectExpression : field.expression().dialects()) {
        expressions.put(dialectExpression.dialect(), dialectExpression.expression());
      }
    }
    return new PlanFieldRefDTO(
        field.dataset(),
        expressions,
        field.column(),
        field.datatype() == null ? null : field.datatype().name());
  }

  private DataType parseDataType() {
    try {
      return DataType.valueOf(datatype.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Unsupported Semantic Model data type '%s'", datatype), e);
    }
  }
}
