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
package org.apache.gravitino.trino.connector.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;

/** Help Apache Gravitino connector access TableMetadata from Gravitino client. */
public class GravitinoTable {
  private final String schemaName;
  private final String tableName;

  private final List<GravitinoColumn> columns;

  private final String comment;
  private final Map<String, String> properties;

  private SortOrder[] sortOrders = new SortOrder[0];
  private Transform[] partitioning = new Transform[0];
  private Distribution distribution = Distributions.NONE;
  private Index[] indexes = new Index[0];

  /**
   * Constructs a new GravitinoTable with the specified schema name, table name, and table metadata.
   *
   * @param schemaName the schema name
   * @param tableName the table name
   * @param tableMetadata the table metadata
   */
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
    indexes = tableMetadata.index();
  }

  /**
   * Constructs a new GravitinoTable with the specified schema name, table name, columns, comment,
   * and properties.
   *
   * @param schemaName the schema name
   * @param tableName the table name
   * @param columns the columns
   * @param comment the comment
   * @param properties the properties
   */
  public GravitinoTable(
      String schemaName,
      String tableName,
      List<GravitinoColumn> columns,
      String comment,
      Map<String, String> properties) {
    this(schemaName, tableName, columns, comment, properties, new Index[0]);
  }

  /**
   * Constructs a new GravitinoTable with the specified schema name, table name, columns, comment,
   * properties and indexes.
   *
   * @param schemaName the schema name
   * @param tableName the table name
   * @param columns the columns
   * @param comment the comment
   * @param properties the properties
   * @param indexes the indexes
   */
  public GravitinoTable(
      String schemaName,
      String tableName,
      List<GravitinoColumn> columns,
      String comment,
      Map<String, String> properties,
      Index[] indexes) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.columns = columns;
    this.comment = comment;
    this.properties = properties;
    this.indexes = indexes;
  }

  /**
   * Constructs a new GravitinoTable with the specified schema name, table name, columns, comment,
   * and properties.
   *
   * @param schemaName the schema name
   * @param tableName the table name
   * @param columns the columns
   * @param comment the comment
   * @param properties the properties
   */
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

  /**
   * Retrieves the name of the table.
   *
   * @return the name of the table
   */
  public String getName() {
    return tableName;
  }

  /**
   * Retrieves the columns of the table.
   *
   * @return the columns of the table
   */
  public List<GravitinoColumn> getColumns() {
    return columns;
  }

  /**
   * Retrieves the raw columns of the table.
   *
   * @return the raw columns of the table
   */
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
              column.getDefaultValue());
    }
    return gravitinoColumns;
  }

  /**
   * Retrieves the properties of the table.
   *
   * @return the properties of the table
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * Retrieves the column of the table.
   *
   * @param columName the name of the column
   * @return the column of the table
   */
  public GravitinoColumn getColumn(String columName) {
    Optional<GravitinoColumn> entry =
        columns.stream().filter((column -> column.getName().equals(columName))).findFirst();
    if (entry.isEmpty()) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_COLUMN_NOT_EXISTS,
          String.format("Column `%s` does not exist", columName));
    }
    return entry.get();
  }

  /**
   * Retrieves the schema name of the table.
   *
   * @return the schema name of the table
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * Retrieves the comment of the table.
   *
   * @return the comment of the table
   */
  public String getComment() {
    return comment;
  }

  /**
   * Sets the sort orders of the table.
   *
   * @param sortOrders the sort orders
   */
  public void setSortOrders(SortOrder[] sortOrders) {
    this.sortOrders = sortOrders;
  }

  /**
   * Sets the partitioning of the table.
   *
   * @param partitioning the partitioning
   */
  public void setPartitioning(Transform[] partitioning) {
    this.partitioning = partitioning;
  }

  /**
   * Sets the distribution of the table.
   *
   * @param distribution the distribution
   */
  public void setDistribution(Distribution distribution) {
    this.distribution = distribution;
  }

  /**
   * Sets the indexes of the table.
   *
   * @param indexes the indexes
   */
  public void setIndexes(Index[] indexes) {
    this.indexes = indexes;
  }

  /**
   * Retrieves the sort orders of the table.
   *
   * @return the sort orders
   */
  public SortOrder[] getSortOrders() {
    return sortOrders;
  }

  /**
   * Retrieves the partitioning of the table.
   *
   * @return the partitioning
   */
  public Transform[] getPartitioning() {
    return partitioning;
  }

  /**
   * Retrieves the distribution of the table.
   *
   * @return the distribution
   */
  public Distribution getDistribution() {
    return distribution;
  }

  /**
   * Retrieves the indexes of the table.
   *
   * @return the indexes
   */
  public Index[] getIndexes() {
    return indexes;
  }
}
