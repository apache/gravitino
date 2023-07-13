package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.catalog.BaseCatalog;
import com.datastrato.graviton.catalog.CatalogOperations;
import java.io.IOException;
import java.util.Map;

public class TestCatalog extends BaseCatalog<TestCatalog> {
  @Override
  public String shortName() {
    return "test";
  }

  @Override
  protected CatalogOperations newOps(Map config) {
    return new CatalogOperations() {
      @Override
      public void initialize(Map<String, String> config) throws RuntimeException {}

      @Override
      public void close() throws IOException {}
    };
  }
}
