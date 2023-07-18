/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton;

import com.datastrato.graviton.catalog.BaseCatalog;
import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.rel.TableCatalog;
import java.util.Map;

public class TestCatalog extends BaseCatalog<TestCatalog> {

  public TestCatalog() {}

  @Override
  public String shortName() {
    return "test";
  }

  @Override
  protected CatalogOperations newOps(Map config) {
    return new TestCatalogOperations();
  }

  @Override
  public TableCatalog asTableCatalog() {
    return (TableCatalog) ops();
  }
}
