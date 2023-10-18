/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog;

import com.datastrato.gravitino.trino.connector.metadata.GravitinoColumn;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoSchema;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoTable;
import com.datastrato.gravitino.trino.connector.util.DataTypeTransformer;
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
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;

/**
 * This interface is used to handle different parts of catalog metadata from different catalog
 * connectors.
 */
public class CatalogConnectorMetadataAdapter {

  protected final List<PropertyMetadata<?>> schemaProperties;
  protected final List<PropertyMetadata<?>> tableProperties;
  protected final List<PropertyMetadata<?>> columnProperties;

  protected CatalogConnectorMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {
    this.schemaProperties = schemaProperties;
    this.tableProperties = tableProperties;
    this.columnProperties = columnProperties;
  }

  public Map<String, Object> getSchemaProperties(GravitinoSchema schema) {
    return normalizeProperties(schema.getProperties(), schemaProperties);
  }

  /** Transform gravitino table metadata to trino ConnectorTableMetadata */
  public ConnectorTableMetadata getTableMetadata(GravitinoTable gravitinoTable) {
    SchemaTableName schemaTableName =
        new SchemaTableName(gravitinoTable.getSchemaName(), gravitinoTable.getName());
    ArrayList<ColumnMetadata> columnMetadataList = new ArrayList<>();
    for (GravitinoColumn column : gravitinoTable.getColumns()) {
      columnMetadataList.add(getColumnMetadata(column));
    }

    Map<String, Object> properties =
        normalizeProperties(gravitinoTable.getProperties(), tableProperties);
    return new ConnectorTableMetadata(
        schemaTableName, columnMetadataList, properties, Optional.of(gravitinoTable.getComment()));
  }

  /** Transform trino ConnectorTableMetadata to gravitino table metadata */
  public GravitinoTable createTable(ConnectorTableMetadata tableMetadata) {
    String tableName = tableMetadata.getTableSchema().getTable().getTableName();
    String schemaName = tableMetadata.getTableSchema().getTable().getSchemaName();
    String comment = tableMetadata.getComment().orElse("");
    Map<String, String> properties = removeUnsetProperties(tableMetadata.getProperties());

    List<GravitinoColumn> columns = new ArrayList<>();
    int index = 0;
    for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
      ColumnMetadata column = tableMetadata.getColumns().get(i);
      columns.add(
          new GravitinoColumn(
              column.getName(),
              DataTypeTransformer.getGravitinoType(column.getType(), column.isNullable()),
              index,
              column.getComment()));
      index++;
    }
    return new GravitinoTable(schemaName, tableName, columns, comment, properties);
  }

  /** Transform trino schema metadata to gravitino schema metadata */
  public GravitinoSchema createSchema(String schemaName, Map<String, Object> properties) {
    return new GravitinoSchema(schemaName, removeUnsetProperties(properties), "");
  }

  /** Transform gravitino column metadata to trino ColumnMetadata */
  public ColumnMetadata getColumnMetadata(GravitinoColumn column) {
    return new ColumnMetadata(column.getName(), DataTypeTransformer.getTrinoType(column.getType()));
  }

  /** Transform gravitino table properties to trino ConnectorTableProperties */
  public ConnectorTableProperties getTableProperties(GravitinoTable table) {
    throw new NotImplementedException();
  }

  /** Normalize gravitino attributes for trino */
  protected Map<String, Object> normalizeProperties(
      Map<String, String> properties, List<PropertyMetadata<?>> propertyTemplate) {
    // TODO yuhui redo this function once graviton table properties are supported..
    // Trino only supports properties defined in the propertyTemplate.
    Map<String, Object> validProperties = new HashMap<>();
    for (PropertyMetadata<?> propertyMetadata : propertyTemplate) {
      String name = propertyMetadata.getName();
      if (properties.containsKey(name)) {
        validProperties.put(name, properties.get(name));
      }
    }
    return validProperties;
  }

  /** Remove trino unset attributes fro gravitino */
  protected Map<String, String> removeUnsetProperties(Map<String, Object> properties) {
    return properties.entrySet().stream()
        .filter(e -> e.getValue() != null)
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
  }
}
