/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.PropertyMeta;
import com.datastrato.graviton.catalog.BaseCatalog;
import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.TableCatalog;
import com.google.common.collect.Maps;
import java.util.Map;

/** Implementation of a Hive catalog in Graviton. */
public class HiveCatalog extends BaseCatalog<HiveCatalog> {

  /**
   * Returns the short name of the Hive catalog.
   *
   * @return The short name of the catalog.
   */
  @Override
  public String shortName() {
    return "hive";
  }

  /**
   * Creates a new instance of {@link HiveCatalogOperations} with the provided configuration.
   *
   * @param config The configuration map for the Hive catalog operations.
   * @return A new instance of {@link HiveCatalogOperations}.
   */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    HiveCatalogOperations ops = new HiveCatalogOperations(entity());
    ops.initialize(config);
    return ops;
  }

  /**
   * Returns the Hive catalog operations as a {@link SupportsSchemas}.
   *
   * @return The Hive catalog operations as {@link HiveCatalogOperations}.
   */
  @Override
  public SupportsSchemas asSchemas() {
    return (HiveCatalogOperations) ops();
  }

  /**
   * Returns the Hive catalog operations as a {@link TableCatalog}.
   *
   * @return The Hive catalog operations as {@link HiveCatalogOperations}.
   */
  @Override
  public TableCatalog asTableCatalog() {
    return (HiveCatalogOperations) ops();
  }

  @Override
  public Map<String, PropertyMeta<?>> getPropertyMetas() {
    // TO IMPLEMENT
    return Maps.newHashMap();
  }
}
