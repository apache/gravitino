/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.TestCatalog;
import com.datastrato.gravitino.TestCatalogOperations;
import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestBaseCatalog {

  @Test
  void testCustomCatalogOperations() {
    CatalogEntity entity = Mockito.mock(CatalogEntity.class);

    TestCatalog catalog =
        new TestCatalog().withCatalogConf(ImmutableMap.of()).withCatalogEntity(entity);
    CatalogOperations testCatalogOperations = catalog.ops();
    Assertions.assertTrue(testCatalogOperations instanceof TestCatalogOperations);

    TestCatalog catalog2 =
        new TestCatalog()
            .withCatalogConf(
                ImmutableMap.of(
                    BaseCatalog.CATALOG_OPERATION_IMPL, DummyCatalogOperations.class.getName()))
            .withCatalogEntity(entity);
    CatalogOperations dummyCatalogOperations = catalog2.ops();
    Assertions.assertTrue(dummyCatalogOperations instanceof DummyCatalogOperations);
  }
}
