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

  /**
   * Transform graviton table metadata to trino ConnectorTableMetadata
   *
   * @param gravitonTable
   * @return
   */
  ConnectorTableMetadata getTableMetaData(GravitonTable gravitonTable);

  /**
   * Transform trino ConnectorTableMetadata to graviton table metadata
   *
   * @param tableMetadata
   * @return
   */
  GravitonTable createTable(ConnectorTableMetadata tableMetadata);

  /**
   * Transform trino schema metadata to graviton schema metadata
   *
   * @param schemaName
   * @param properties
   * @return
   */
  GravitonSchema createSchema(String schemaName, Map<String, Object> properties);

  /**
   * Transform graviton column metadata to trino ColumnMetadata
   *
   * @param column
   * @return
   */
  ColumnMetadata getColumnMetadata(GravitonColumn column);

  /**
   * Transform graviton table properties to trino ConnectorTableProperties
   *
   * @param table
   * @return
   */
  ConnectorTableProperties getTableProperties(GravitonTable table);
}
