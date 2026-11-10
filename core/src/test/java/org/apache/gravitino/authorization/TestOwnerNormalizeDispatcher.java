/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.authorization;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

class TestOwnerNormalizeDispatcher {
  private final OwnerDispatcher delegate = mock(OwnerDispatcher.class);
  private final CatalogManager catalogs = mock(CatalogManager.class);
  private final EntityStore store = mock(EntityStore.class);
  private final BaseCatalog catalog = mock(BaseCatalog.class);
  private OwnerNormalizeDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    when(catalog.capability())
        .thenReturn(
            new Capability() {
              @Override
              public CapabilityResult caseSensitiveOnName(Scope scope) {
                return scope == Scope.SCHEMA || scope == Scope.TABLE
                    ? CapabilityResult.unsupported("case insensitive")
                    : CapabilityResult.SUPPORTED;
              }
            });
    doAnswer(
            call -> {
              ThrowableFunction<BaseCatalog, Object> operation = call.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogs)
        .doWithCatalog(any(), any());
    dispatcher = new OwnerNormalizeDispatcher(delegate, catalogs, store);
  }

  @ParameterizedTest
  @CsvSource({
    "SCHEMA, cat.ANALYTICS, cat.analytics",
    "TABLE, cat.ANALYTICS.ORDERS, cat.analytics.orders",
    "COLUMN, cat.ANALYTICS.ORDERS.MixedColumn, cat.analytics.orders.MixedColumn",
  })
  void testReadsAndWritesUseCatalogNames(MetadataObject.Type type, String request, String stored) {
    MetadataObject object = MetadataObjects.parse(request, type);
    MetadataObject normalized = MetadataObjects.parse(stored, type);
    dispatcher.setOwner("m", object, "MixedUser", Owner.Type.USER);
    dispatcher.getOwner("m", object);
    verify(delegate).setOwner("m", normalized, "MixedUser", Owner.Type.USER);
    verify(delegate).getOwner("m", normalized);
  }

  @ParameterizedTest
  @CsvSource({
    "METALAKE, MixedLake",
    "CATALOG, MixedCatalog",
    "TAG, MixedTag",
    "POLICY, MixedPolicy",
    "ROLE, MixedRole",
    "JOB_TEMPLATE, MixedTemplate"
  })
  void testNamesOutsideCatalogRulesArePreserved(MetadataObject.Type type, String name) {
    MetadataObject object = MetadataObjects.parse(name, type);
    dispatcher.setOwner("m", object, "MixedGroup", Owner.Type.GROUP);
    dispatcher.getOwner("m", object);
    verify(delegate).setOwner("m", object, "MixedGroup", Owner.Type.GROUP);
    verify(delegate).getOwner("m", object);
    verifyNoInteractions(catalogs, store);
  }

  @Test
  void testCaseSensitiveNamesArePreserved() {
    when(catalog.capability()).thenReturn(new Capability() {});
    MetadataObject object =
        MetadataObjects.parse("cat.Analytics.Orders", MetadataObject.Type.TABLE);
    dispatcher.setOwner("m", object, "owner", Owner.Type.USER);
    verify(delegate).setOwner("m", object, "owner", Owner.Type.USER);
  }

  @Test
  void testExactStoredColumnSurvivesCustomFolding() throws Exception {
    when(catalog.capability())
        .thenReturn(
            new Capability() {
              @Override
              public CapabilityResult caseSensitiveOnName(Scope scope) {
                return CapabilityResult.unsupported("case insensitive");
              }
            });
    TableEntity table = mock(TableEntity.class);
    ColumnEntity column = mock(ColumnEntity.class);
    when(column.name()).thenReturn("MixedColumn");
    when(table.columns()).thenReturn(List.of(column));
    when(store.get(
            NameIdentifier.of("m", "cat", "analytics", "orders"),
            Entity.EntityType.TABLE,
            TableEntity.class))
        .thenReturn(table);
    MetadataObject request =
        MetadataObjects.parse("cat.ANALYTICS.ORDERS.MixedColumn", MetadataObject.Type.COLUMN);
    MetadataObject expected =
        MetadataObjects.parse("cat.analytics.orders.MixedColumn", MetadataObject.Type.COLUMN);
    dispatcher.setOwner("m", request, "owner", Owner.Type.USER);
    verify(delegate).setOwner("m", expected, "owner", Owner.Type.USER);
  }

  @Test
  void testBatchDeduplicatesEquivalentNames() {
    MetadataObject lower = MetadataObjects.parse("cat.analytics.orders", MetadataObject.Type.TABLE);
    MetadataObject upper = MetadataObjects.parse("cat.ANALYTICS.ORDERS", MetadataObject.Type.TABLE);
    dispatcher.setOwners("m", List.of(lower, upper), "owner", Owner.Type.USER);
    verify(delegate).setOwners("m", List.of(lower), "owner", Owner.Type.USER);
  }

  @Test
  void testResolutionFailureDoesNotWriteAnyPartOfBatch() {
    Mockito.doThrow(new NoSuchCatalogException("missing"))
        .when(catalogs)
        .doWithCatalog(Mockito.eq(NameIdentifier.of("m", "missing")), any());
    MetadataObject valid = MetadataObjects.parse("cat.ANALYTICS.ORDERS", MetadataObject.Type.TABLE);
    MetadataObject invalid =
        MetadataObjects.parse("missing.analytics.orders", MetadataObject.Type.TABLE);
    assertThrows(
        NoSuchCatalogException.class,
        () -> dispatcher.setOwners("m", List.of(valid, invalid), "owner", Owner.Type.USER));
    assertThrows(
        NoSuchCatalogException.class,
        () -> dispatcher.setOwner("m", invalid, "owner", Owner.Type.USER));
    verifyNoInteractions(delegate);
  }

  @Test
  void testEmptyAndNullBatch() {
    dispatcher.setOwners("m", List.of(), "owner", Owner.Type.USER);
    assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.setOwners("m", null, "owner", Owner.Type.USER));
    verifyNoInteractions(delegate, catalogs, store);
  }
}
