/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog.hive;

import com.datastrato.graviton.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.graviton.trino.connector.metadata.GravitonColumn;
import com.datastrato.graviton.trino.connector.metadata.GravitonSchema;
import com.datastrato.graviton.trino.connector.metadata.GravitonTable;
import com.datastrato.graviton.trino.connector.util.DataTypeTransform;
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

/** Transforming graviton hive metadata to trino. */
public class HiveMetadataAdapter implements CatalogConnectorMetadataAdapter {
  private final List<PropertyMetadata<?>> schemaProperties;
  private final List<PropertyMetadata<?>> tableProperties;
  private final List<PropertyMetadata<?>> columnProperties;

  public HiveMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {
    this.schemaProperties = schemaProperties;
    this.tableProperties = tableProperties;
    this.columnProperties = columnProperties;
  }

  public ConnectorTableMetadata getTableMetaData(GravitonTable gravitonTable) {
    SchemaTableName schemaTableName =
        new SchemaTableName(gravitonTable.getSchemaName(), gravitonTable.getName());
    ArrayList<ColumnMetadata> columnMetadataList = new ArrayList<>();
    for (GravitonColumn column : gravitonTable.getColumns()) {
      columnMetadataList.add(getColumnMetadata(column));
    }

    Map<String, Object> properties =
        gravitonTable.getProperties().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return new ConnectorTableMetadata(
        schemaTableName, columnMetadataList, properties, Optional.of(gravitonTable.getComment()));
  }

  @Override
  public ColumnMetadata getColumnMetadata(GravitonColumn column) {
    return new ColumnMetadata(column.getName(), DataTypeTransform.getTrinoType(column.getType()));
  }

  @Override
  public ConnectorTableProperties getTableProperties(GravitonTable table) {
    throw new NotImplementedException();
  }

  @Override
  public Map<String, Object> getSchemaProperties(GravitonSchema schema) {
    return normalizeProperties(schema.properties(), schemaProperties);
  }

  @Override
  public GravitonTable createTable(ConnectorTableMetadata tableMetadata) {
    throw new NotImplementedException();
  }

  @Override
  public GravitonSchema createSchema(String schemaName, Map<String, Object> properties) {
    throw new NotImplementedException();
  }

  private Map<String, Object> normalizeProperties(
      Map<String, String> properties, List<PropertyMetadata<?>> propertyTemplate) {
    // TODO yuhui redo this function on graviton table properties supported.
    Map<String, Object> validProperties = new HashMap<>();
    for (PropertyMetadata<?> propertyMetadata : propertyTemplate) {
      String name = propertyMetadata.getName();
      if (properties.containsKey(name)) {
        if (propertyMetadata.getJavaType() == String.class)
          validProperties.put(name, properties.get(name));

        if (name.equals("format")) validProperties.put(name, properties.get(name));
      }
    }
    return validProperties;
  }
}
