/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

/** Transforming gravitino hive metadata to trino. */
public class HiveMetadataAdapter extends CatalogConnectorMetadataAdapter {

  public HiveMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {
    super(
        schemaProperties, tableProperties, columnProperties, new HiveDataTypeTransformer());
  }
}
