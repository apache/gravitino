/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql;

import com.datastrato.catalog.property.PropertyConverter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoColumn;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoTable;
import com.google.common.collect.Maps;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.session.PropertyMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Transforming gravitino MySQL metadata to trino. */
public class MySQLMetadataAdapter extends CatalogConnectorMetadataAdapter {

  private final PropertyConverter tableConverter;

  public MySQLMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {

    super(schemaProperties, tableProperties, columnProperties, new MySQLDataTypeTransformer());
    this.tableConverter = new MySQLTablePropertyConverter();
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, Object> properties) {
    Map<String, Object> stringMap = tableConverter.engineToGravitinoProperties(properties);
    return super.toGravitinoTableProperties(stringMap);
  }

  @Override
  public Map<String, Object> toTrinoTableProperties(Map<String, String> properties) {
    Map<String, String> objectMap = tableConverter.gravitinoToEngineProperties(properties);
    return super.toTrinoTableProperties(objectMap);
  }

  /** Transform trino ConnectorTableMetadata to gravitino table metadata */
  @Override
  public GravitinoTable createTable(ConnectorTableMetadata tableMetadata) {
    String tableName = tableMetadata.getTableSchema().getTable().getTableName();
    String schemaName = tableMetadata.getTableSchema().getTable().getSchemaName();
    String comment = tableMetadata.getComment().orElse("");
    Map<String, String> properties = toGravitinoTableProperties(tableMetadata.getProperties());

    List<GravitinoColumn> columns = new ArrayList<>();
    for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
      ColumnMetadata column = tableMetadata.getColumns().get(i);
      boolean autoIncrement =
          (boolean) column.getProperties().getOrDefault(MySQLPropertyMeta.AUTO_INCREMENT, false);

      columns.add(
          new GravitinoColumn(
              column.getName(),
              dataTypeTransformer.getGravitinoType(column.getType()),
              i,
              column.getComment(),
              column.isNullable(),
              autoIncrement,
              column.getProperties()));
    }

    return new GravitinoTable(schemaName, tableName, columns, comment, properties);
  }

  @Override
  public ColumnMetadata getColumnMetadata(GravitinoColumn column) {
    Map<String, Object> propertyMap = Maps.newHashMap(column.getProperties());
    if (column.isAutoIncrement()) {
      propertyMap.put(MySQLPropertyMeta.AUTO_INCREMENT, true);
    }

    return ColumnMetadata.builder()
        .setName(column.getName())
        .setType(dataTypeTransformer.getTrinoType(column.getType()))
        .setComment(Optional.ofNullable(column.getComment()))
        .setNullable(column.isNullable())
        .setHidden(column.isHidden())
        .setProperties(propertyMap)
        .build();
  }
}
