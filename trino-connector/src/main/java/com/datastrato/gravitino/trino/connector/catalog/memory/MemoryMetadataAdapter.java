/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.memory;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

/** Support trino memory connector for testing. */
public class MemoryMetadataAdapter extends CatalogConnectorMetadataAdapter {

  MemoryMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {

    super(schemaProperties, tableProperties, columnProperties);
  }
}
