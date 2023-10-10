/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.catalog.BaseCatalog;
import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.catalog.PropertiesMetadata;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
      public void initialize(Map<String, String> config) throws RuntimeException {}

      @Override
      public void close() throws IOException {}

      public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
        return Maps::newHashMap;
      }
    };
  }

  @Test
  void test() {
    TestCatalog testCatalog = new TestCatalog();
    // This will load the log4j2.xml file from the classpath
    Map<String, String> configs = testCatalog.loadCatalogSpecificProperties("log4j2.properties");
    Assertions.assertTrue(configs.containsKey("status"));
    Assertions.assertEquals("warn", configs.get("status"));
  }
}
