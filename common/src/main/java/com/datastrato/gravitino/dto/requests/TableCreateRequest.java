/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.DistributionDTO;
import com.datastrato.gravitino.dto.rel.SortOrderDTO;
import com.datastrato.gravitino.dto.rel.expressions.FunctionArg;
import com.datastrato.gravitino.dto.rel.indexes.IndexDTO;
import com.datastrato.gravitino.dto.rel.partitioning.Partitioning;
import com.datastrato.gravitino.rest.RESTRequest;
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
import org.apache.commons.lang3.StringUtils;

/** Represents a request to create a table. */
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
  @JsonProperty("partitioning")
  private final Partitioning[] partitioning;

  @Nullable
  @JsonProperty("indexes")
  private final IndexDTO[] indexes;

  /** Default constructor for Jackson deserialization. */
  public TableCreateRequest() {
    this(null, null, null, null, null, null, null, null);
  }

  /**
   * Creates a new TableCreateRequest.
   *
   * @param name The name of the table.
   * @param comment The comment of the table.
   * @param columns The columns of the table.
   * @param properties The properties of the table.
   * @param sortOrders The sort orders of the table.
   * @param distribution The distribution of the table.
   * @param partitioning The partitioning of the table.
   * @param indexes The indexes of the table.
   */
  public TableCreateRequest(
      String name,
      @Nullable String comment,
      ColumnDTO[] columns,
      @Nullable Map<String, String> properties,
      @Nullable SortOrderDTO[] sortOrders,
      @Nullable DistributionDTO distribution,
      @Nullable Partitioning[] partitioning,
      @Nullable IndexDTO[] indexes) {
    this.name = name;
    this.columns = columns;
    this.comment = comment;
    this.properties = properties;
    this.sortOrders = sortOrders;
    this.distribution = distribution;
    this.partitioning = partitioning;
    this.indexes = indexes;
  }

  /**
   * Validates the {@link TableCreateRequest} request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
    Preconditions.checkArgument(
        columns != null && columns.length != 0,
        "\"columns\" field is required and cannot be empty");

    if (sortOrders != null) {
      Arrays.stream(sortOrders).forEach(sortOrder -> sortOrder.validate(columns));
    }

    if (distribution != null) {
      Arrays.stream((FunctionArg[]) distribution.expressions())
          .forEach(expression -> expression.validate(columns));
    }

    if (partitioning != null) {
      Arrays.stream(partitioning).forEach(p -> p.validate(columns));
    }

    List<ColumnDTO> autoIncrementCols =
        Arrays.stream(columns)
            .map(
                column -> {
                  column.validate();
                  return column;
                })
            .filter(ColumnDTO::autoIncrement)
            .collect(Collectors.toList());
    String autoIncrementColsStr =
        autoIncrementCols.stream().map(ColumnDTO::name).collect(Collectors.joining(",", "[", "]"));
    Preconditions.checkArgument(
        autoIncrementCols.size() <= 1,
        "Only one column can be auto-incremented. There are multiple auto-increment columns in your table: "
            + autoIncrementColsStr);

    if (indexes != null && indexes.length > 0) {
      Arrays.stream(indexes)
          .forEach(
              index -> {
                Preconditions.checkArgument(index.type() != null, "Index type cannot be null");
                Preconditions.checkArgument(
                    index.fieldNames().length > 0, "Index field names cannot be null");
              });
    }
  }
}
