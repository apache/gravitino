/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.memory;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

/** Support Trino memory connector for testing. */
public class MemoryMetadataAdapter extends CatalogConnectorMetadataAdapter {

  public MemoryMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {

    super(schemaProperties, tableProperties, columnProperties, new GeneralDataTypeTransformer());
  }
}
