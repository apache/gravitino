/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.requests;

import com.datastrato.graviton.dto.rel.ColumnDTO;
import com.datastrato.graviton.dto.rel.Partition;
import com.datastrato.graviton.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

@Getter
@EqualsAndHashCode
@ToString
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

  @Nullable
  @JsonProperty("partitions")
  private final Partition[] partitions;

  public TableCreateRequest() {
    this(null, null, null, null, null);
  }

  public TableCreateRequest(
      String name, String comment, ColumnDTO[] columns, Map<String, String> properties) {
    this(name, comment, columns, properties, new Partition[0]);
  }

  public TableCreateRequest(
      String name,
      String comment,
      ColumnDTO[] columns,
      Map<String, String> properties,
      @Nullable Partition[] partitions) {
    this.name = name;
    this.columns = columns;
    this.comment = comment;
    this.properties = properties;
    this.partitions = partitions;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
    Preconditions.checkArgument(
        columns != null && columns.length != 0,
        "\"columns\" field is required and cannot be empty");

    if (partitions != null) {
      Arrays.stream(partitions).forEach(p -> p.validate(columns));
    }
  }
}
