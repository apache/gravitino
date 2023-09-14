/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.requests;

import com.datastrato.graviton.dto.rel.ColumnDTO;
import com.datastrato.graviton.dto.rel.DistributionDTO;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.Expression;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.FieldExpression;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.FunctionExpression;
import com.datastrato.graviton.dto.rel.Partition;
import com.datastrato.graviton.dto.rel.SortOrderDTO;
import com.datastrato.graviton.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class TableCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @Nullable
  @JsonProperty("comment")
  private final String comment;

  @JsonProperty("columns")
  private final ColumnDTO[] columns;

  @Nullable
  @JsonProperty("properties")
  private final Map<String, String> properties;

  @JsonProperty("sortOrders")
  @Nullable
  private final SortOrderDTO[] sortOrders;

  @JsonProperty("distribution")
  @Nullable
  private final DistributionDTO distribution;

  @Nullable
  @JsonProperty("partitions")
  private final Partition[] partitions;

  public TableCreateRequest() {
    this(null, null, null, null, null, null, null);
  }

  public TableCreateRequest(
      String name, String comment, ColumnDTO[] columns, Map<String, String> properties) {
    this(
        name,
        comment,
        columns,
        properties,
        new SortOrderDTO[0],
        DistributionDTO.NONE,
        new Partition[0]);
  }

  public TableCreateRequest(
      String name,
      String comment,
      ColumnDTO[] columns,
      Map<String, String> properties,
      SortOrderDTO[] sortOrders,
      DistributionDTO distribution,
      @Nullable Partition[] partitions) {
    this.name = name;
    this.columns = columns;
    this.comment = comment;
    this.properties = properties;
    this.sortOrders = sortOrders;
    this.distribution = distribution;
    this.partitions = partitions;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
    Preconditions.checkArgument(
        columns != null && columns.length != 0,
        "\"columns\" field is required and cannot be empty");

    List<String> columnNames =
        Arrays.stream(columns).map(ColumnDTO::name).collect(Collectors.toList());
    if (ArrayUtils.isNotEmpty(sortOrders)) {
      Arrays.stream(sortOrders)
          .forEach(sortOrder -> validateExpresion(sortOrder.getExpression(), columnNames));
    }

    if (distribution != null) {
      Arrays.stream(distribution.getExpressions())
          .forEach(expression -> validateExpresion(expression, columnNames));
    }

    if (partitions != null) {
      Arrays.stream(partitions).forEach(p -> p.validate(columns));
    }
  }

  // Check column name in sort order and distribution expressions are in table columns
  private void validateExpresion(Expression expression, List<String> columnNames) {
    if (expression instanceof FieldExpression) {
      FieldExpression nameRefernce = (FieldExpression) expression;
      String columnName = nameRefernce.getFieldName()[0];
      Preconditions.checkArgument(
          columnNames.contains(columnName),
          "Column name %s in sort order expression is not found in table columns",
          columnName);
    } else if (expression instanceof FunctionExpression) {
      FunctionExpression function = (FunctionExpression) expression;
      for (Expression arg : function.getArgs()) {
        validateExpresion(arg, columnNames);
      }
    }
  }
}
