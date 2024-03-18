/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import static com.datastrato.gravitino.TestCatalogOperations.FAIL_CREATE;

import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.rel.TableCatalog;
import java.util.Map;
import java.util.Objects;

public class TestCatalog extends BaseCatalog<TestCatalog> {

  public TestCatalog() {}

  @Override
  public String shortName() {
    return "test";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    if (Objects.equals(config.get(FAIL_CREATE), "true")) {
      throw new RuntimeException("Failed to create Test catalog");
    }
    return new TestCatalogOperations(config);
  }

  @Override
  public TableCatalog asTableCatalog() {
    return (TableCatalog) ops();
  }
}
