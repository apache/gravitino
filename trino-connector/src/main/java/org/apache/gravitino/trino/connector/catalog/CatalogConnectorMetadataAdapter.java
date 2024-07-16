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
package org.apache.gravitino.trino.connector.catalog;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.session.PropertyMetadata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.gravitino.trino.connector.metadata.GravitinoColumn;
import org.apache.gravitino.trino.connector.metadata.GravitinoSchema;
import org.apache.gravitino.trino.connector.metadata.GravitinoTable;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This interface is used to handle different parts of catalog metadata from different catalog
 * connectors.
 */
public class CatalogConnectorMetadataAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogConnectorMetadataAdapter.class);
  protected final List<PropertyMetadata<?>> schemaProperties;
  protected final List<PropertyMetadata<?>> tableProperties;
  protected final List<PropertyMetadata<?>> columnProperties;

  protected final GeneralDataTypeTransformer dataTypeTransformer;

  protected CatalogConnectorMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties,
      GeneralDataTypeTransformer dataTypeTransformer) {
    this.schemaProperties = schemaProperties;
    this.tableProperties = tableProperties;
    this.columnProperties = columnProperties;
    this.dataTypeTransformer = dataTypeTransformer;
  }

  public Map<String, Object> getSchemaProperties(GravitinoSchema schema) {
    return toTrinoSchemaProperties(schema.getProperties());
  }

  public GeneralDataTypeTransformer getDataTypeTransformer() {
    return dataTypeTransformer;
  }

  /** Transform Gravitino table metadata to Trino ConnectorTableMetadata */
  public ConnectorTableMetadata getTableMetadata(GravitinoTable gravitinoTable) {
    SchemaTableName schemaTableName =
        new SchemaTableName(gravitinoTable.getSchemaName(), gravitinoTable.getName());
    ArrayList<ColumnMetadata> columnMetadataList = new ArrayList<>();
    for (GravitinoColumn column : gravitinoTable.getColumns()) {
      columnMetadataList.add(getColumnMetadata(column));
    }

    Map<String, Object> properties = toTrinoTableProperties(gravitinoTable.getProperties());
    return new ConnectorTableMetadata(
        schemaTableName,
        columnMetadataList,
        properties,
        Optional.ofNullable(gravitinoTable.getComment()));
  }

  /** Transform Trino ConnectorTableMetadata to Gravitino table metadata */
  public GravitinoTable createTable(ConnectorTableMetadata tableMetadata) {
    String tableName = tableMetadata.getTableSchema().getTable().getTableName();
    String schemaName = tableMetadata.getTableSchema().getTable().getSchemaName();
    String comment = tableMetadata.getComment().orElse("");
    Map<String, String> properties = toGravitinoTableProperties(tableMetadata.getProperties());

    List<GravitinoColumn> columns = new ArrayList<>();
    for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
      ColumnMetadata column = tableMetadata.getColumns().get(i);
      columns.add(
          new GravitinoColumn(
              column.getName(),
              dataTypeTransformer.getGravitinoType(column.getType()),
              i,
              column.getComment(),
              column.isNullable(),
              false,
              column.getProperties()));
    }

    return new GravitinoTable(schemaName, tableName, columns, comment, properties);
  }

  protected Map<String, Object> removeKeys(
      Map<String, Object> properties, Set<String> keyToDelete) {
    return properties.entrySet().stream()
        .filter(entry -> !keyToDelete.contains(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /** Transform Trino schema metadata to Gravitino schema metadata */
  public GravitinoSchema createSchema(String schemaName, Map<String, Object> properties) {
    return new GravitinoSchema(schemaName, toGravitinoSchemaProperties(properties), "");
  }

  /** Transform Gravitino column metadata to Trino ColumnMetadata */
  public ColumnMetadata getColumnMetadata(GravitinoColumn column) {
    return ColumnMetadata.builder()
        .setName(column.getName())
        .setType(dataTypeTransformer.getTrinoType(column.getType()))
        .setComment(Optional.ofNullable(column.getComment()))
        .setNullable(column.isNullable())
        .setHidden(column.isHidden())
        .setProperties(column.getProperties())
        .build();
  }

  /** Transform Gravitino table properties to Trino ConnectorTableProperties */
  public ConnectorTableProperties getTableProperties(GravitinoTable table) {
    throw new NotImplementedException();
  }

  /** Normalize Gravitino attributes for Trino */
  private Map<String, Object> normalizeProperties(
      Map<String, String> properties, List<PropertyMetadata<?>> propertyTemplate) {
    // TODO yuhui redo this function once Gravitino table properties are supported..
    // Trino only supports properties defined in the propertyTemplate.
    Map<String, Object> validProperties = new HashMap<>();
    for (PropertyMetadata<?> propertyMetadata : propertyTemplate) {
      String name = propertyMetadata.getName();
      if (properties.containsKey(name)) {
        validProperties.put(name, properties.get(name));
      } else {
        LOG.warn("Property {} is not defined in Trino, we will ignore it", name);
      }
    }
    return validProperties;
  }

  /** Normalize Gravitino table attributes for Trino */
  public Map<String, Object> toTrinoTableProperties(Map<String, String> properties) {
    return normalizeProperties(properties, tableProperties);
  }

  /** Normalize Gravitino schema attributes for Trino */
  public Map<String, Object> toTrinoSchemaProperties(Map<String, String> properties) {
    return normalizeProperties(properties, schemaProperties);
  }

  /** Normalize Trino table attributes for Gravitino */
  public Map<String, String> toGravitinoTableProperties(Map<String, Object> properties) {
    return removeUnsetProperties(properties);
  }

  /** Normalize Trino schema attributes for Gravitino */
  public Map<String, String> toGravitinoSchemaProperties(Map<String, Object> properties) {
    return removeUnsetProperties(properties);
  }

  /** Remove Trino unset attributes for Gravitino */
  private Map<String, String> removeUnsetProperties(Map<String, Object> properties) {
    return properties.entrySet().stream()
        .filter(e -> e.getValue() != null)
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
  }

  public GravitinoColumn createColumn(ColumnMetadata column) {
    return new GravitinoColumn(
        column.getName(),
        dataTypeTransformer.getGravitinoType(column.getType()),
        -1,
        column.getComment(),
        column.isNullable(),
        false,
        column.getProperties());
  }
}
