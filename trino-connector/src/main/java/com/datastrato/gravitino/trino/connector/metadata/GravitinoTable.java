/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_COLUMN_NOT_EXISTS;

import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
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

  private SortOrder[] sortOrders = new SortOrder[0];
  private Transform[] partitioning = new Transform[0];
  private Distribution distribution = Distributions.NONE;

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

    sortOrders = tableMetadata.sortOrder();
    partitioning = tableMetadata.partitioning();
    distribution = tableMetadata.distribution();
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

  public Column[] getRawColumns() {
    Column[] gravitinoColumns = new Column[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      GravitinoColumn column = columns.get(i);
      gravitinoColumns[i] =
          Column.of(
              column.getName(),
              column.getType(),
              column.getComment(),
              column.isNullable(),
              column.isAutoIncrement(),
              null);
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

  public void setSortOrders(SortOrder[] sortOrders) {
    this.sortOrders = sortOrders;
  }

  public void setPartitioning(Transform[] partitioning) {
    this.partitioning = partitioning;
  }

  public void setDistribution(Distribution distribution) {
    this.distribution = distribution;
  }

  public SortOrder[] getSortOrders() {
    return sortOrders;
  }

  public Transform[] getPartitioning() {
    return partitioning;
  }

  public Distribution getDistribution() {
    return distribution;
  }
}
