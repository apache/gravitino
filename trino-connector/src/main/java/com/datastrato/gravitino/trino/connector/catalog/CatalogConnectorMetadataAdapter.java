/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog;

import com.datastrato.gravitino.trino.connector.metadata.GravitinoColumn;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoSchema;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoTable;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import java.util.Map;

/**
 * This interface is used to handle different parts of catalog metadata from different catalog
 * connectors.
 */
public interface CatalogConnectorMetadataAdapter {
  Map<String, Object> getSchemaProperties(GravitinoSchema schema);

  /** Transform gravitino table metadata to trino ConnectorTableMetadata */
  ConnectorTableMetadata getTableMetadata(GravitinoTable gravitinoTable);

  /** Transform trino ConnectorTableMetadata to gravitino table metadata */
  GravitinoTable createTable(ConnectorTableMetadata tableMetadata);

  /** Transform trino schema metadata to gravitino schema metadata */
  GravitinoSchema createSchema(String schemaName, Map<String, Object> properties);

  /** Transform gravitino column metadata to trino ColumnMetadata */
  ColumnMetadata getColumnMetadata(GravitinoColumn column);

  /** Transform gravitino table properties to trino ConnectorTableProperties */
  ConnectorTableProperties getTableProperties(GravitinoTable table);
}
