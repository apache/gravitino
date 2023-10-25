/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.datastrato.gravitino.catalog.BaseCatalog;
import com.datastrato.gravitino.catalog.CatalogOperations;
import com.datastrato.gravitino.catalog.PropertiesMetadata;
import com.google.common.collect.Maps;
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
      public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
      }

      @Override
      public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void initialize(Map<String, String> config) throws RuntimeException {}

      @Override
      public void close() throws IOException {}

      public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
        return Maps::newHashMap;
      }
    };
  }
}
