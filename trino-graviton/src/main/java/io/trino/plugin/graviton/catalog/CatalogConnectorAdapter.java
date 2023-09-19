/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package io.trino.plugin.graviton.catalog;

import static java.util.Collections.emptyList;

import io.trino.spi.session.PropertyMetadata;
import java.util.List;

/**
 * * This interface is used to handle different parts of connectors from different catalog
 * connectors.
 */
public interface CatalogConnectorAdapter {

  default List<PropertyMetadata<?>> getTableProperties() {
    return emptyList();
  }
}
