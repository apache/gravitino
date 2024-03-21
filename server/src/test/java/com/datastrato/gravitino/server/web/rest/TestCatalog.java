/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.CatalogInfo;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.PropertiesMetadata;
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
      public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
      }

      @Override
      public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void initialize(Map<String, String> config, CatalogInfo info)
          throws RuntimeException {}

      @Override
      public void close() throws IOException {}

      public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
        return Maps::newHashMap;
      }
    };
  }
}
