/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.jdbc;

import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCommonPropertyNames.AUTO_INCREMENT;

import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoColumn;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoTable;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import com.google.common.collect.Maps;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.session.PropertyMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** The basic class of JDBC metadata adapter. */
public class DefaultJDBCMetadataAdapter extends CatalogConnectorMetadataAdapter {

  private final PropertyConverter propertyConverter;

  public DefaultJDBCMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties,
      GeneralDataTypeTransformer dataTypeTransformer,
      PropertyConverter tablePropertyConverter) {
    super(schemaProperties, tableProperties, columnProperties, dataTypeTransformer);
    this.propertyConverter = tablePropertyConverter;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, Object> properties) {
    Map<String, Object> stringMap = propertyConverter.engineToGravitinoProperties(properties);
    return super.toGravitinoTableProperties(stringMap);
  }

  @Override
  public Map<String, Object> toTrinoTableProperties(Map<String, String> properties) {
    Map<String, String> objectMap = propertyConverter.gravitinoToEngineProperties(properties);
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
      boolean autoIncrement = (boolean) column.getProperties().getOrDefault(AUTO_INCREMENT, false);

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
      propertyMap.put(AUTO_INCREMENT, true);
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
