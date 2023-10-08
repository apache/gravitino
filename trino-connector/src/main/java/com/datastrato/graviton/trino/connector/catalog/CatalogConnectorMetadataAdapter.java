/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import com.datastrato.graviton.trino.connector.metadata.GravitonColumn;
import com.datastrato.graviton.trino.connector.metadata.GravitonSchema;
import com.datastrato.graviton.trino.connector.metadata.GravitonTable;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import java.util.Map;

/**
 * This interface is used to handle different parts of catalog metadata from different catalog
 * connectors.
 */
public interface CatalogConnectorMetadataAdapter {
  Map<String, Object> getSchemaProperties(GravitonSchema schema);

  ConnectorTableMetadata getTableMetaData(GravitonTable gravitonTable);

  GravitonTable createTable(ConnectorTableMetadata tableMetadata);

  GravitonSchema createSchema(String schemaName, Map<String, Object> properties);

  ColumnMetadata getColumnMetadata(GravitonColumn column);

  ConnectorTableProperties getTableProperties(GravitonTable table);
}
