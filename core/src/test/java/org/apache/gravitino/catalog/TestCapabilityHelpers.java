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

import java.io.IOException;
import java.util.Collections;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCapabilityHelpers {

  @Test
  void testNewColumnDoesNotUseExistingNameRules() {
    Capability capability =
        new Capability() {
          @Override
          public CapabilityResult specificationOnExistingName(Scope scope, String name) {
            throw new AssertionError("New columns must use creation rules");
          }
        };
    TableChange[] changes =
        CapabilityHelpers.applyCapabilities(
            capability,
            TableChange.addColumn(new String[] {"new_column"}, Types.IntegerType.get()));
    Assertions.assertEquals(1, changes.length);
  }

  /** Checks existing references and new names use their respective provider rules. */
  @Test
  public void testExistingColumnReferencesDoNotUseCreationPattern() {
    Capability strict =
        new Capability() {
          @Override
          public CapabilityResult specificationOnName(Scope scope, String name) {
            return name.matches("[a-zA-Z_][a-zA-Z0-9_]*")
                ? CapabilityResult.SUPPORTED
                : CapabilityResult.unsupported("invalid new name");
          }

          @Override
          public CapabilityResult specificationOnExistingName(Scope scope, String name) {
            return CapabilityResult.SUPPORTED;
          }
        };
    TableChange[] changes =
        CapabilityHelpers.applyCapabilities(
            strict,
            TableChange.updateColumnComment(new String[] {"native col"}, "comment"),
            TableChange.renameColumn(new String[] {"native col"}, "valid_name"),
            TableChange.deleteColumn(new String[] {"native col"}, false),
            TableChange.updateColumnPosition(
                new String[] {"native col"}, TableChange.ColumnPosition.after("other native col")));
    Assertions.assertEquals(4, changes.length);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            CapabilityHelpers.applyCapabilities(
                strict, TableChange.addColumn(new String[] {"new col"}, Types.IntegerType.get())));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            CapabilityHelpers.applyCapabilities(
                strict, TableChange.renameColumn(new String[] {"native col"}, "new col")));
  }

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

  @Test
  void testNormalizeMetadataIdentifierKeepsExactStoredColumnName() throws IOException {
    CatalogManager catalogManager = foldingCatalogManager();
    EntityStore store = Mockito.mock(EntityStore.class);
    NameIdentifier table = NameIdentifier.of("metalake", "catalog", "schema", "orders");
    TableEntity tableEntity = Mockito.mock(TableEntity.class);
    ColumnEntity column = Mockito.mock(ColumnEntity.class);
    Mockito.when(column.name()).thenReturn("MyCol");
    Mockito.when(tableEntity.columns()).thenReturn(Collections.singletonList(column));
    Mockito.when(store.get(table, Entity.EntityType.TABLE, TableEntity.class))
        .thenReturn(tableEntity);

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "orders", "MyCol"),
        CapabilityHelpers.normalizeMetadataIdentifier(
            NameIdentifier.of("metalake", "catalog", "SCHEMA", "ORDERS", "MyCol"),
            Capability.Scope.COLUMN,
            catalogManager,
            () -> store));
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "orders", "othercol"),
        CapabilityHelpers.normalizeMetadataIdentifier(
            NameIdentifier.of("metalake", "catalog", "SCHEMA", "ORDERS", "OtherCol"),
            Capability.Scope.COLUMN,
            catalogManager,
            () -> store));
    Mockito.verify(store, Mockito.times(2)).get(table, Entity.EntityType.TABLE, TableEntity.class);

    // A name that folding does not change needs no stored-column read.
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "orders", "mycol"),
        CapabilityHelpers.normalizeMetadataIdentifier(
            NameIdentifier.of("metalake", "catalog", "SCHEMA", "ORDERS", "mycol"),
            Capability.Scope.COLUMN,
            catalogManager,
            () -> store));
    Mockito.verify(store, Mockito.times(2)).get(table, Entity.EntityType.TABLE, TableEntity.class);
  }

  @Test
  void testNormalizeMetadataIdentifierHandlesMissingOrFailedStoredTable() throws IOException {
    CatalogManager catalogManager = foldingCatalogManager();
    EntityStore store = Mockito.mock(EntityStore.class);
    NameIdentifier table = NameIdentifier.of("metalake", "catalog", "schema", "orders");
    Mockito.when(store.get(table, Entity.EntityType.TABLE, TableEntity.class))
        .thenThrow(new NoSuchEntityException("missing"));
    NameIdentifier column = NameIdentifier.of("metalake", "catalog", "schema", "ORDERS", "MyCol");
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "orders", "mycol"),
        CapabilityHelpers.normalizeMetadataIdentifier(
            column, Capability.Scope.COLUMN, catalogManager, () -> store));

    IOException failure = new IOException("store down");
    Mockito.reset(store);
    Mockito.when(store.get(table, Entity.EntityType.TABLE, TableEntity.class)).thenThrow(failure);
    RuntimeException error =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                CapabilityHelpers.normalizeMetadataIdentifier(
                    column, Capability.Scope.COLUMN, catalogManager, () -> store));
    Assertions.assertSame(failure, error.getCause());
    Assertions.assertTrue(error.getMessage().contains(table.toString()));
  }

  private static CatalogManager foldingCatalogManager() {
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalog.capability())
        .thenReturn(
            new Capability() {
              @Override
              public CapabilityResult caseSensitiveOnName(Scope scope) {
                return CapabilityResult.unsupported("case insensitive");
              }
            });
    CatalogTestUtils.mockDoWithCatalog(catalogManager, catalog);
    return catalogManager;
  }
}
