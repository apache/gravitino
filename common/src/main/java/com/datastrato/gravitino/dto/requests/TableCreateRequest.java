/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.DistributionDTO;
import com.datastrato.gravitino.dto.rel.SortOrderDTO;
import com.datastrato.gravitino.dto.rel.expressions.FunctionArg;
import com.datastrato.gravitino.dto.rel.partitions.Partitioning;
import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
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
  @JsonProperty("partitioning")
  private final Partitioning[] partitioning;

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
        new Partitioning[0]);
  }

  public TableCreateRequest(
      String name,
      @Nullable String comment,
      ColumnDTO[] columns,
      @Nullable Map<String, String> properties,
      @Nullable SortOrderDTO[] sortOrders,
      @Nullable DistributionDTO distribution,
      @Nullable Partitioning[] partitioning) {
    this.name = name;
    this.columns = columns;
    this.comment = comment;
    this.properties = properties;
    this.sortOrders = sortOrders;
    this.distribution = distribution;
    this.partitioning = partitioning;
  }

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
  }
}
