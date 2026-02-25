/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFunctionOperationDispatcher {

  private static final String METALAKE = "test_metalake";
  private static final String ICEBERG_CATALOG = "iceberg_catalog";
  private static final String HIVE_CATALOG = "hive_catalog";

  private final IdGenerator idGenerator = new RandomIdGenerator();

  private FunctionOperationDispatcher dispatcher;
  private CatalogManager catalogManager;
  private SchemaOperationDispatcher schemaOps;
  private EntityStore store;

  @BeforeEach
  public void setUp() {
    catalogManager = mock(CatalogManager.class);
    schemaOps = mock(SchemaOperationDispatcher.class);
    store = mock(EntityStore.class);

    CatalogManager.CatalogWrapper icebergWrapper = createMockCatalogWrapper("lakehouse-iceberg");
    CatalogManager.CatalogWrapper hiveWrapper = createMockCatalogWrapper("hive");

    when(catalogManager.loadCatalogAndWrap(NameIdentifier.of(METALAKE, ICEBERG_CATALOG)))
        .thenReturn(icebergWrapper);
    when(catalogManager.loadCatalogAndWrap(NameIdentifier.of(METALAKE, HIVE_CATALOG)))
        .thenReturn(hiveWrapper);

    dispatcher = new FunctionOperationDispatcher(catalogManager, schemaOps, store, idGenerator);
  }

  @Test
  public void testIcebergSystemSchemaRestriction() {
    NameIdentifier icebergSystemFunc =
        NameIdentifier.of(Namespace.of(METALAKE, ICEBERG_CATALOG, "system"), "my_func");
    FunctionDefinition[] defs = new FunctionDefinition[] {mock(FunctionDefinition.class)};

    // All write operations on Iceberg system schema should be rejected
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.registerFunction(icebergSystemFunc, "c", FunctionType.SCALAR, true, defs));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            dispatcher.alterFunction(
                icebergSystemFunc, FunctionChange.updateComment("new comment")));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> dispatcher.dropFunction(icebergSystemFunc));

    // Non-system schema in Iceberg catalog should pass validation
    NameIdentifier icebergUserFunc =
        NameIdentifier.of(Namespace.of(METALAKE, ICEBERG_CATALOG, "user_schema"), "my_func");
    try {
      dispatcher.registerFunction(icebergUserFunc, "c", FunctionType.SCALAR, true, defs);
    } catch (IllegalArgumentException e) {
      Assertions.assertFalse(
          e.getMessage().contains("Iceberg reserved schema"),
          "Should not reject non-system schema");
    } catch (Exception ignored) {
      // Other exceptions (e.g., LockManager not initialized) are expected in unit test
    }

    // System schema in non-Iceberg catalog should pass validation
    NameIdentifier hiveSystemFunc =
        NameIdentifier.of(Namespace.of(METALAKE, HIVE_CATALOG, "system"), "my_func");
    try {
      dispatcher.registerFunction(hiveSystemFunc, "c", FunctionType.SCALAR, true, defs);
    } catch (IllegalArgumentException e) {
      Assertions.assertFalse(
          e.getMessage().contains("Iceberg reserved schema"),
          "Should not reject system schema in non-Iceberg catalog");
    } catch (Exception ignored) {
      // Other exceptions (e.g., LockManager not initialized) are expected in unit test
    }
  }

  private CatalogManager.CatalogWrapper createMockCatalogWrapper(String provider) {
    BaseCatalog catalog = mock(BaseCatalog.class);
    when(catalog.provider()).thenReturn(provider);

    CatalogManager.CatalogWrapper wrapper = mock(CatalogManager.CatalogWrapper.class);
    when(wrapper.catalog()).thenReturn(catalog);
    return wrapper;
  }
}
