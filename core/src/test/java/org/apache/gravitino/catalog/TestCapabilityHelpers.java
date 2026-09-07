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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCapabilityHelpers {

  @Test
  void testGetCapabilityPropagatesNoSuchCatalogException() {
    NameIdentifier tableIdent =
        NameIdentifier.of(Namespace.of("metalake", "catalog", "schema"), "table");
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    Mockito.when(catalogManager.doWithCatalog(Mockito.any(), Mockito.any()))
        .thenThrow(new NoSuchCatalogException("Catalog %s does not exist", tableIdent));

    // A missing catalog must stay a NoSuchCatalogException (mapped to 404 by the REST layer)
    // instead of being wrapped into a plain RuntimeException (a 500).
    Assertions.assertThrows(
        NoSuchCatalogException.class,
        () -> CapabilityHelpers.getCapability(tableIdent, catalogManager));
  }

  @Test
  void testGetCapabilityWrapsCapabilityFailure() throws Exception {
    NameIdentifier tableIdent =
        NameIdentifier.of(Namespace.of("metalake", "catalog", "schema"), "table");
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    CatalogTestUtils.mockDoWithCatalog(catalogManager, catalog);
    Mockito.when(catalog.capability()).thenThrow(new IllegalStateException("boom"));

    RuntimeException e =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> CapabilityHelpers.getCapability(tableIdent, catalogManager));

    Assertions.assertInstanceOf(IllegalStateException.class, e.getCause());
  }
}
