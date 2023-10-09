/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import static java.util.Collections.emptyList;

import com.datastrato.graviton.trino.connector.metadata.GravitonCatalog;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import java.util.Map;

/**
 * This interface is used to handle different parts of connectors from different catalog connectors.
 */
public interface CatalogConnectorAdapter {

  /** @return TableProperties list that used to validate table properties. */
  default List<PropertyMetadata<?>> getTableProperties() {
    return emptyList();
  }

  /** @return Return internal connector config with trino. */
  Map<String, Object> buildInternalConnectorConfig(GravitonCatalog catalog);

  /**
   * Return MetaDataAdapter for special catalog connector.
   *
   * @return
   */
  CatalogConnectorMetadataAdapter getMetaDataAdapter();
}
