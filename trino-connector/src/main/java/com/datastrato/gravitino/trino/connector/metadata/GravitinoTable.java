/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_COLUMN_NOT_EXISTS;

import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.TableDTO;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Help Gravitino connector access TableMetadata from gravitino client. */
public class GravitinoTable {
  private final String schemaName;
  private final String tableName;

  private final List<GravitinoColumn> columns;

  private final String comment;
  private final Map<String, String> properties;

  @JsonCreator
  public GravitinoTable(String schemaName, String tableName, Table tableMetadata) {
    this.schemaName = schemaName;
    this.tableName = tableName;

    ImmutableList.Builder<GravitinoColumn> tableColumns = ImmutableList.builder();
    for (int i = 0; i < tableMetadata.columns().length; i++) {
      tableColumns.add(new GravitinoColumn(tableMetadata.columns()[i], i));
    }
    this.columns = tableColumns.build();
    this.comment = tableMetadata.comment();
    properties = tableMetadata.properties();
  }

  public GravitinoTable(
      String schemaName,
      String tableName,
      List<GravitinoColumn> columns,
      String comment,
      Map<String, String> properties) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.columns = columns;
    this.comment = comment;
    this.properties = properties;
  }

  public GravitinoTable(
      String schemaName,
      String tableName,
      Column[] columns,
      String comment,
      Map<String, String> properties) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    ImmutableList.Builder<GravitinoColumn> tableColumns = ImmutableList.builder();
    for (int i = 0; i < columns.length; i++) {
      tableColumns.add(new GravitinoColumn(columns[i], i));
    }
    this.columns = tableColumns.build();
    this.comment = comment;
    this.properties = properties;
  }

  public String getName() {
    return tableName;
  }

  public List<GravitinoColumn> getColumns() {
    return columns;
  }

  public ColumnDTO[] getColumnDTOs() {
    ColumnDTO[] gravitinoColumns = new ColumnDTO[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      gravitinoColumns[i] =
          ColumnDTO.builder()
              .withName(columns.get(i).getName())
              .withDataType(columns.get(i).getType())
              .withComment(columns.get(i).getComment())
              .build();
    }
    return gravitinoColumns;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public GravitinoColumn getColumn(String columName) {
    Optional<GravitinoColumn> entry =
        columns.stream().filter((column -> column.getName().equals(columName))).findFirst();
    if (entry.isEmpty()) {
      throw new TrinoException(
          GRAVITINO_COLUMN_NOT_EXISTS, String.format("Column `%s` does not exist", columName));
    }
    return entry.get();
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getComment() {
    return comment;
  }

  public TableDTO getTableDTO() {
    return TableDTO.builder()
        .withName(tableName)
        .withComment(comment)
        .withColumns(getColumnDTOs())
        .withProperties(properties)
        .withAudit(new AuditDTO.Builder().build())
        .build();
  }
}
