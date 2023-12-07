/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import java.util.Map;

/** Transforming gravitino Iceberg metadata to trino. */
public class IcebergMetadataAdapter extends CatalogConnectorMetadataAdapter {

  // Move all this logic to CatalogConnectorMetadataAdapter
  private final PropertyConverter tableConverter;
  private final PropertyConverter schemaConverter;

  public IcebergMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {
    super(schemaProperties, tableProperties, columnProperties, new IcebergDataTypeTransformer());
    this.tableConverter = new IcebergTablePropertyConverter();
    this.schemaConverter = new IcebergSchemaPropertyConverter();
  }

  @Override
  public Map<String, Object> toTrinoTableProperties(Map<String, String> properties) {
    Map<String, String> objectMap = tableConverter.toTrinoProperties(properties);
    return super.toTrinoTableProperties(objectMap);
  }

  @Override
  public Map<String, Object> toTrinoSchemaProperties(Map<String, String> properties) {
    Map<String, String> objectMap = schemaConverter.toTrinoProperties(properties);
    return super.toTrinoSchemaProperties(objectMap);
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, Object> properties) {
    Map<String, Object> stringMap = tableConverter.toGravitinoProperties(properties);
    return super.toGravitinoTableProperties(stringMap);
  }

  @Override
  public Map<String, String> toGravitinoSchemaProperties(Map<String, Object> properties) {
    Map<String, Object> stringMap = schemaConverter.toGravitinoProperties(properties);
    return super.toGravitinoSchemaProperties(stringMap);
  }
}
