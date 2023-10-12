/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.metadata;

import com.datastrato.graviton.dto.rel.ColumnDTO;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Table;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Help Graviton connector access TableMetadata from graviton client. */
public class GravitonTable {
  private final String schemaName;
  private final String tableName;

  private final List<GravitonColumn> columns;

  private final String comment;
  private final Map<String, String> properties;

  @JsonCreator
  public GravitonTable(String schemaName, String tableName, Table tableMetadata) {
    this.schemaName = schemaName;
    this.tableName = tableName;

    ImmutableList.Builder<GravitonColumn> table_columns = ImmutableList.builder();
    int i = 0;
    for (Column column : tableMetadata.columns()) {
      table_columns.add(new GravitonColumn(column, i));
      i++;
    }
    this.columns = table_columns.build();
    this.comment = tableMetadata.comment();
    properties = tableMetadata.properties();
  }

  public GravitonTable(
      String schemaName,
      String tableName,
      List<GravitonColumn> columns,
      String comment,
      Map<String, String> properties) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.columns = columns;
    this.comment = comment;
    this.properties = properties;
  }

  public String getName() {
    return tableName;
  }

  public List<GravitonColumn> getColumns() {
    return columns;
  }

  public ColumnDTO[] getColumnDTOs() {
    ColumnDTO[] gravitonColumns = new ColumnDTO[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      gravitonColumns[i] =
          ColumnDTO.builder()
              .withName(columns.get(i).getName())
              .withDataType(columns.get(i).getType())
              .withComment(columns.get(i).getComment())
              .build();
    }
    return gravitonColumns;
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
    return comment;
  }
}
