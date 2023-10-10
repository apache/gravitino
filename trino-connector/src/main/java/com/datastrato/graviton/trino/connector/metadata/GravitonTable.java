/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.metadata;

import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_COLUMN_NOT_EXISTS;

import com.datastrato.graviton.rel.Table;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Help Graviton connector access TableMetadata from graviton client. */
public class GravitonTable {
  private final String schemaName;
  private final Table tableMetadata;
  private final ImmutableList<GravitonColumn> columns;
  private final Map<String, String> properties;

  public GravitonTable(String schemaName, Table tableMetadata) {
    this.schemaName = schemaName;
    this.tableMetadata = tableMetadata;

    ImmutableList.Builder<GravitonColumn> tableColumns = ImmutableList.builder();
    for (int i = 0; i < tableMetadata.columns().length; i++) {
      tableColumns.add(new GravitonColumn(tableMetadata.columns()[i], i));
    }
    this.columns = tableColumns.build();

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
    if (entry.isEmpty()) {
      throw new TrinoException(
          GRAVITON_COLUMN_NOT_EXISTS, String.format("Column %s does not exist", columName));
    }

    return entry.get();
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getComment() {
    return tableMetadata.comment();
  }
}
