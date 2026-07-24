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

import com.google.common.base.Preconditions;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
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
import org.apache.gravitino.trino.connector.metadata.GravitinoView;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import org.apache.gravitino.trino.connector.util.json.JsonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This interface is used to handle different parts of catalog metadata from different catalog
 * connectors.
 */
public class CatalogConnectorMetadataAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogConnectorMetadataAdapter.class);

  /** The list of schema properties supported by this catalog connector. */
  protected final List<PropertyMetadata<?>> schemaProperties;

  /** The list of table properties supported by this catalog connector. */
  protected final List<PropertyMetadata<?>> tableProperties;

  /** The list of column properties supported by this catalog connector. */
  protected final List<PropertyMetadata<?>> columnProperties;

  /** The data type transformer used to convert between Gravitino and Trino types. */
  protected final GeneralDataTypeTransformer dataTypeTransformer;

  /**
   * Constructs a new CatalogConnectorMetadataAdapter.
   *
   * @param schemaProperties The list of schema properties supported by this catalog connector
   * @param tableProperties The list of table properties supported by this catalog connector
   * @param columnProperties The list of column properties supported by this catalog connector
   * @param dataTypeTransformer The data type transformer used to convert between Gravitino and
   *     Trino types
   */
  public CatalogConnectorMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties,
      GeneralDataTypeTransformer dataTypeTransformer) {
    this.schemaProperties = schemaProperties;
    this.tableProperties = tableProperties;
    this.columnProperties = columnProperties;
    this.dataTypeTransformer = dataTypeTransformer;
  }

  /**
   * Retrieves the schema properties for the specified Gravitino schema.
   *
   * @param schema the Gravitino schema
   * @return a map of schema properties
   */
  public Map<String, Object> getSchemaProperties(GravitinoSchema schema) {
    return toTrinoSchemaProperties(schema.getProperties());
  }

  /**
   * Retrieves the data type transformer for the specified Gravitino schema.
   *
   * @return the data type transformer
   */
  public GeneralDataTypeTransformer getDataTypeTransformer() {
    return dataTypeTransformer;
  }

  /**
   * Transform Gravitino table metadata to Trino ConnectorTableMetadata
   *
   * @param gravitinoTable the Gravitino table
   * @return the Trino ConnectorTableMetadata
   */
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

  /**
   * Transform Trino ConnectorTableMetadata to Gravitino table metadata
   *
   * @param tableMetadata the Trino ConnectorTableMetadata
   * @return the Gravitino table metadata
   */
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

  /**
   * Transform Gravitino view metadata to Trino ConnectorViewDefinition. Owner is not supported by
   * Gravitino views, so the resulting definition always has an empty owner; since Trino requires an
   * owner for run-as-definer views, {@code runAsInvoker} is always {@code true}.
   *
   * @param view the Gravitino view
   * @return the Trino ConnectorViewDefinition
   */
  public ConnectorViewDefinition getViewDefinition(GravitinoView view) {
    Preconditions.checkArgument(
        view.getSql() != null,
        "View %s.%s has no Trino dialect SQL representation",
        view.getSchemaName(),
        view.getName());
    List<ViewColumn> columns =
        view.getColumns().stream()
            .map(
                column ->
                    new ViewColumn(
                        column.getName(),
                        dataTypeTransformer.getTrinoType(column.getType()).getTypeId(),
                        Optional.ofNullable(column.getComment())))
            .collect(Collectors.toList());

    return new ConnectorViewDefinition(
        view.getSql(),
        Optional.ofNullable(view.getDefaultCatalog()),
        Optional.ofNullable(view.getDefaultSchema()),
        columns,
        Optional.ofNullable(view.getComment()),
        Optional.empty(),
        true,
        List.of());
  }

  /**
   * Transform Trino ConnectorViewDefinition to Gravitino view metadata. The {@code viewProperties}
   * are merged as-is into the resulting view's generic properties.
   *
   * @param viewName the schema-qualified view name
   * @param definition the Trino ConnectorViewDefinition
   * @param viewProperties the Trino view properties
   * @return the Gravitino view metadata
   */
  public GravitinoView createView(
      SchemaTableName viewName,
      ConnectorViewDefinition definition,
      Map<String, Object> viewProperties) {
    TypeManager typeManager = JsonCodec.getTypeManager(getClass().getClassLoader());
    List<GravitinoColumn> columns = new ArrayList<>();
    List<ViewColumn> viewColumns = definition.getColumns();
    for (int i = 0; i < viewColumns.size(); i++) {
      ViewColumn column = viewColumns.get(i);
      Type trinoType = typeManager.getType(column.getType());
      columns.add(
          new GravitinoColumn(
              column.getName(),
              dataTypeTransformer.getGravitinoType(trinoType),
              i,
              column.getComment().orElse(null),
              true,
              false,
              Map.of()));
    }

    Map<String, String> properties = toGravitinoTableProperties(viewProperties);
    return new GravitinoView(
        viewName.getSchemaName(),
        viewName.getTableName(),
        columns,
        definition.getComment().orElse(null),
        properties,
        definition.getOriginalSql(),
        definition.getCatalog().orElse(null),
        definition.getSchema().orElse(null));
  }

  /**
   * Removes specified keys from a map of properties.
   *
   * @param properties the map of properties to remove keys from
   * @param keyToDelete the set of keys to remove
   * @return a new map with the specified keys removed
   */
  protected Map<String, Object> removeKeys(
      Map<String, Object> properties, Set<String> keyToDelete) {
    return properties.entrySet().stream()
        .filter(entry -> !keyToDelete.contains(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Transform Trino schema metadata to Gravitino schema metadata
   *
   * @param schemaName the name of the schema
   * @param properties the properties of the schema
   * @return the Gravitino schema metadata
   */
  public GravitinoSchema createSchema(String schemaName, Map<String, Object> properties) {
    return new GravitinoSchema(schemaName, toGravitinoSchemaProperties(properties), "");
  }

  /**
   * Transform Gravitino column metadata to Trino ColumnMetadata
   *
   * @param column the Gravitino column
   * @return the Trino ColumnMetadata
   */
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

  /**
   * Transform Gravitino table properties to Trino ConnectorTableProperties
   *
   * @param table the Gravitino table
   * @return the Trino ConnectorTableProperties
   */
  public ConnectorTableProperties getTableProperties(GravitinoTable table) {
    throw new NotImplementedException();
  }

  /**
   * Normalize Gravitino attributes for Trino
   *
   * @param properties the Gravitino properties
   * @param propertyTemplate the Trino property template
   * @return the normalized properties
   */
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

  /**
   * Normalize Gravitino table attributes for Trino
   *
   * @param properties the Gravitino properties
   * @return the Trino properties
   */
  public Map<String, Object> toTrinoTableProperties(Map<String, String> properties) {
    return normalizeProperties(properties, tableProperties);
  }

  /**
   * Normalize Gravitino schema attributes for Trino
   *
   * @param properties the Gravitino properties
   * @return the Trino properties
   */
  public Map<String, Object> toTrinoSchemaProperties(Map<String, String> properties) {
    return normalizeProperties(properties, schemaProperties);
  }

  /**
   * Normalize Trino table attributes for Gravitino
   *
   * @param properties the Trino properties
   * @return the Gravitino properties
   */
  public Map<String, String> toGravitinoTableProperties(Map<String, Object> properties) {
    return removeUnsetProperties(properties);
  }

  /**
   * Normalize Trino schema attributes for Gravitino
   *
   * @param properties the Trino properties
   * @return the Gravitino properties
   */
  public Map<String, String> toGravitinoSchemaProperties(Map<String, Object> properties) {
    return removeUnsetProperties(properties);
  }

  /**
   * Remove Trino unset attributes for Gravitino
   *
   * @param properties the Trino properties
   * @return the Gravitino properties
   */
  private Map<String, String> removeUnsetProperties(Map<String, Object> properties) {
    return properties.entrySet().stream()
        .filter(e -> e.getValue() != null)
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
  }

  /**
   * Creates a new Gravitino column from a Trino ColumnMetadata.
   *
   * @param column the Trino ColumnMetadata
   * @return the new Gravitino column
   */
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
