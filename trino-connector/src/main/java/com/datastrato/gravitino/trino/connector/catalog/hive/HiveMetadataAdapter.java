/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.engine.PropertyConverter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Transforming gravitino hive metadata to trino. */
public class HiveMetadataAdapter extends CatalogConnectorMetadataAdapter {

  private final PropertyConverter tableConverter;
  private final PropertyConverter schemaConverter;

  public HiveMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {
    super(schemaProperties, tableProperties, columnProperties, new HiveDataTypeTransformer());
    this.tableConverter = new HiveTablePropertyConverterV2();
    this.schemaConverter = new HiveSchemaPropertyConverterV2();
  }

  @Override
  public Map<String, Object> toTrinoTableProperties(Map<String, String> properties) {
    Map<String, String> objectMap = tableConverter.fromGravitino(properties);
    return super.toTrinoTableProperties(objectMap);
  }

  @Override
  public Map<String, Object> toTrinoSchemaProperties(Map<String, String> properties) {
    Map<String, String> objectMap = schemaConverter.fromGravitino(properties);
    return super.toTrinoSchemaProperties(objectMap);
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, Object> properties) {
    Map<String, String> stringMap = tableConverter.toGravitino(properties);
    Map<String, Object> objectMap =
        stringMap.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> (Object) entry.getValue()));
    return super.toGravitinoTableProperties(objectMap);
  }

  @Override
  public Map<String, String> toGravitinoSchemaProperties(Map<String, Object> properties) {
    Map<String, String> stringMap = schemaConverter.toGravitino(properties);
    Map<String, Object> objectMap =
        stringMap.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> (Object) entry.getValue()));
    return super.toGravitinoSchemaProperties(objectMap);
  }
}
