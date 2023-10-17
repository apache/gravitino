/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.catalog.BaseCatalog;
import com.datastrato.gravitino.catalog.CatalogOperations;
import com.datastrato.gravitino.rel.TableCatalog;
import java.util.Map;

public class TestCatalog extends BaseCatalog<TestCatalog> {

  public TestCatalog() {}

  @Override
  public String shortName() {
    return "test";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new TestCatalogOperations(config);
  }

  @Override
  public TableCatalog asTableCatalog() {
    return (TableCatalog) ops();
  }
}
