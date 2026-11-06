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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class TestTableExistence {

  private static final NameIdentifier IDENT = NameIdentifier.of("ml", "cat", "s", "t");

  private BaseCatalog<?> catalog;
  private CatalogManager manager;
  private TableOperationDispatcher dispatcher;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws Exception {
    manager = mock(CatalogManager.class);
    catalog = mock(BaseCatalog.class);
    when(catalog.capability()).thenReturn(new Capability() {});
    CatalogTestUtils.mockDoWithCatalog(manager, catalog);
    CatalogManager.CatalogWrapper wrapper =
        CatalogTestUtils.mockDetachConnectorResult(mock(CatalogManager.CatalogWrapper.class));
    doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog, Object> operation = invocation.getArgument(0);
              return operation.apply(catalog);
            })
        .when(wrapper)
        .doWithCatalog(any());
    doAnswer(
            invocation -> {
              ThrowableFunction<CatalogManager.CatalogWrapper, Object> operation =
                  invocation.getArgument(1);
              return operation.apply(wrapper);
            })
        .when(manager)
        .doWithCatalogWrapper(any(), any());
    dispatcher =
        spy(
            new TableOperationDispatcher(
                manager, mock(EntityStore.class), mock(IdGenerator.class)));
  }

  @Test
  void testUnsupportedCatalogHasNoTables() {
    when(catalog.ops()).thenReturn(mock(CatalogOperations.class));
    Assertions.assertFalse(dispatcher.tableExists(IDENT));
    verify(dispatcher, never()).loadTable(any());
  }

  @Test
  void testSupportedCatalogStillLoadsAndImportsTables() {
    when(catalog.ops()).thenReturn(mock(TableCatalogOperations.class));
    doReturn(mock(Table.class)).when(dispatcher).loadTable(IDENT);
    Assertions.assertTrue(dispatcher.tableExists(IDENT));
    verify(dispatcher).loadTable(IDENT);

    doThrow(new NoSuchTableException("missing")).when(dispatcher).loadTable(IDENT);
    Assertions.assertFalse(dispatcher.tableExists(IDENT));

    doThrow(new UnsupportedOperationException("backend failure")).when(dispatcher).loadTable(IDENT);
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> dispatcher.tableExists(IDENT));
  }

  @Test
  void testNormalizedMetadataExistenceCheckReturnsNotFound() {
    when(catalog.ops()).thenReturn(mock(CatalogOperations.class));
    TableNormalizeDispatcher normalized = new TableNormalizeDispatcher(dispatcher, manager);
    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.internalTableDispatcher()).thenReturn(normalized);
    try (MockedStatic<GravitinoEnv> mocked = mockStatic(GravitinoEnv.class)) {
      mocked.when(GravitinoEnv::getInstance).thenReturn(env);
      for (MetadataObject object :
          new MetadataObject[] {
            MetadataObjects.parse("cat.s.t", MetadataObject.Type.TABLE),
            MetadataObjects.parse("cat.s.t.c", MetadataObject.Type.COLUMN)
          }) {
        Assertions.assertThrows(
            NoSuchMetadataObjectException.class,
            () -> MetadataObjectUtil.checkMetadataObject("ml", object));
      }
    }
    verify(dispatcher, never()).loadTable(any());
  }

  private interface TableCatalogOperations extends CatalogOperations, TableCatalog {}
}
