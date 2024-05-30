/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.CatalogInfo;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.HasPropertyMetadata;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;

public class TestCatalog extends BaseCatalog<TestCatalog> {
  private static final PropertiesMetadata PROPERTIES_METADATA = ImmutableMap::of;

  @Override
  public String shortName() {
    return "test";
  }

  @Override
  protected CatalogOperations newOps(Map config) {
    return new CatalogOperations() {

      @Override
      public void initialize(
          Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
          throws RuntimeException {}

      @Override
      public void close() throws IOException {}
    };
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return PROPERTIES_METADATA;
  }
}
