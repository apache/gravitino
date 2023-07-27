/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.catalog.BaseCatalog;
import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.TableCatalog;
import java.util.Map;

public class HiveCatalog extends BaseCatalog<HiveCatalog> {

  @Override
  public String shortName() {
    return "hive";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    HiveCatalogOperations ops = new HiveCatalogOperations(entity());
    ops.initialize(config);
    return ops;
  }

  @Override
  public SupportsSchemas asSchemas() {
    return (HiveCatalogOperations) ops();
  }

  @Override
  public TableCatalog asTableCatalog() {
    return (HiveCatalogOperations) ops();
  }
}
