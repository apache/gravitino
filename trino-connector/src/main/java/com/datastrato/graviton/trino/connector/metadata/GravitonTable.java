/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.metadata;

import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Table;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class GravitonTable {
  private final String schemaName;
  private final Table tableMetadata;
  private final ImmutableList<GravitonColumn> columns;
  private final Map<String, String> properties;

  @JsonCreator
  public GravitonTable(String schemaName, Table tableMetadata) {
    this.schemaName = schemaName;
    this.tableMetadata = tableMetadata;

    ImmutableList.Builder<GravitonColumn> table_columns = ImmutableList.builder();
    int i = 0;
    for (Column column : tableMetadata.columns()) {
      table_columns.add(new GravitonColumn(column, i));
      i++;
    }
    this.columns = table_columns.build();

    properties = tableMetadata.properties();
  }

  public String getName() {
    return tableMetadata.name();
  }

  public List<GravitonColumn> getColumns() {
    return columns;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public GravitonColumn getColumn(String columName) {
    Optional<GravitonColumn> entry =
        columns.stream().filter((column -> column.getName().equals(columName))).findFirst();
    return entry.get();
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getComment() {
    return tableMetadata.comment();
  }
}
