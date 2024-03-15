/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.TableCatalog;
import java.io.Closeable;
import java.util.Map;

/**
 * A catalog operation interface that is used to trigger the operations of a catalog. This interface
 * should be mixed with other Catalog interface like {@link SupportsSchemas} to provide schema
 * operation, {@link TableCatalog} to support table operations, etc.
 */
public interface CatalogOperations extends Closeable, HasPropertyMetadata {

  /**
   * Initialize the CatalogOperation with specified configuration. This method is called after
   * CatalogOperation object is created, but before any other method is called. The method is used
   * to initialize the connection to the underlying metadata source. RuntimeException will be thrown
   * if the initialization failed.
   *
   * @param config The configuration of this Catalog.
   * @param entity The catalog entity associated with this operations instance.
   * @throws RuntimeException if the initialization failed.
   */
  void initialize(Map<String, String> config, CatalogEntity entity) throws RuntimeException;
}
