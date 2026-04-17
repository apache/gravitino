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

package org.apache.gravitino.flink.connector.paimon;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkTableFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Unit tests for {@link GravitinoPaimonCatalog}. */
public class TestGravitinoPaimonCatalog {

  private static final String CATALOG_NAME = "test-paimon-catalog";
  private static final String TEST_DB = "testDb";
  private static final String TEST_TABLE = "testTable";

  private FlinkCatalog mockPaimonCatalog;
  private GravitinoPaimonCatalog catalog;

  @BeforeEach
  void setUp() {
    mockPaimonCatalog = Mockito.mock(FlinkCatalog.class);
    CatalogFactory.Context mockContext = Mockito.mock(CatalogFactory.Context.class);
    when(mockContext.getName()).thenReturn(CATALOG_NAME);
    when(mockContext.getOptions())
        .thenReturn(
            ImmutableMap.of(
                "type", GravitinoPaimonCatalogFactoryOptions.IDENTIFIER,
                "warehouse", "file:///tmp/test_warehouse",
                "metastore", "filesystem"));

    try (MockedConstruction<FlinkCatalogFactory> ignored =
        Mockito.mockConstruction(
            FlinkCatalogFactory.class,
            (mock, ctx) -> when(mock.createCatalog(any())).thenReturn(mockPaimonCatalog))) {
      catalog =
          new GravitinoPaimonCatalog(
              mockContext,
              "default",
              Mockito.mock(SchemaAndTablePropertiesConverter.class),
              Mockito.mock(PartitionConverter.class));
    }
  }

  // Helper: wire up GravitinoCatalogManager and return the TableCatalog mock.
  private TableCatalog setupGravitinoCatalogMock(
      MockedStatic<GravitinoCatalogManager> mgrStatic, Catalog mockGravitinoCatalog) {
    GravitinoCatalogManager mockMgr = Mockito.mock(GravitinoCatalogManager.class);
    mgrStatic.when(GravitinoCatalogManager::get).thenReturn(mockMgr);
    when(mockMgr.getGravitinoCatalogInfo(CATALOG_NAME)).thenReturn(mockGravitinoCatalog);
    TableCatalog mockTableCatalog = Mockito.mock(TableCatalog.class);
    when(mockGravitinoCatalog.asTableCatalog()).thenReturn(mockTableCatalog);
    return mockTableCatalog;
  }

  // ── realCatalog / getFactory ──────────────────────────────────────────────

  @Test
  public void testRealCatalogReturnsPaimonCatalog() {
    Assertions.assertSame(mockPaimonCatalog, catalog.realCatalog());
  }

  @Test
  public void testGetFactoryReturnsFlinkTableFactory() {
    Assertions.assertTrue(catalog.getFactory().isPresent());
    Assertions.assertInstanceOf(FlinkTableFactory.class, catalog.getFactory().get());
  }

  // ── dropTable ─────────────────────────────────────────────────────────────

  @Test
  public void testDropTableCallsPurgeOnGravitino() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockTableCatalog.purgeTable(identifier)).thenReturn(true);

      catalog.dropTable(tablePath, false);

      verify(mockTableCatalog).purgeTable(identifier);
      // Paimon is NOT touched directly; the Gravitino server handles metastore sync.
      verify(mockPaimonCatalog, never()).dropTable(any(), Mockito.anyBoolean());
    }
  }

  @Test
  public void testDropTableThrowsWhenTableNotExistsAndIgnoreIsFalse() {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockTableCatalog.purgeTable(identifier)).thenReturn(false);

      Assertions.assertThrows(
          TableNotExistException.class, () -> catalog.dropTable(tablePath, false));
    }
  }

  @Test
  public void testDropTableSucceedsWhenTableNotExistsAndIgnoreIsTrue() {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockTableCatalog.purgeTable(identifier)).thenReturn(false);

      // ignoreIfNotExists=true must suppress TableNotExistException.
      Assertions.assertDoesNotThrow(() -> catalog.dropTable(tablePath, true));
    }
  }

  // ── toFlinkTable (exercised via getTable) ─────────────────────────────────
  //
  // GravitinoPaimonCatalog overrides toFlinkTable() to return
  // paimonCatalog.getTable(), which carries a proper CatalogEnvironment.
  // BaseCatalog.getTable() enforces Gravitino authorization first, then
  // delegates to toFlinkTable().

  @Test
  public void testGetTableReturnsPaimonNativeTable()
      throws TableNotExistException, CatalogException {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);
    CatalogBaseTable paimonTable = Mockito.mock(CatalogBaseTable.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      // Gravitino auth succeeds.
      when(mockTableCatalog.loadTable(identifier)).thenReturn(Mockito.mock(Table.class));
      // Paimon returns its native DataCatalogTable.
      when(mockPaimonCatalog.getTable(tablePath)).thenReturn(paimonTable);

      CatalogBaseTable result = catalog.getTable(tablePath);

      Assertions.assertSame(paimonTable, result);
      verify(mockTableCatalog).loadTable(identifier);
      verify(mockPaimonCatalog).getTable(tablePath);
    }
  }

  @Test
  public void testGetTableThrowsTableNotExistWhenGravitinoHasNoTable() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, "nonExistingTable");
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, "nonExistingTable");
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockTableCatalog.loadTable(identifier))
          .thenThrow(new NoSuchTableException("table not found"));

      Assertions.assertThrows(TableNotExistException.class, () -> catalog.getTable(tablePath));
      // Paimon must NOT be queried when Gravitino says the table does not exist.
      verify(mockPaimonCatalog, never()).getTable(any());
    }
  }

  @Test
  public void testGetTableThrowsCatalogExceptionWhenGravitinoFails() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockTableCatalog.loadTable(identifier))
          .thenThrow(new RuntimeException("authorization error"));

      // Non-NoSuchTableException from Gravitino becomes a CatalogException.
      Assertions.assertThrows(CatalogException.class, () -> catalog.getTable(tablePath));
      // Paimon must NOT be called when Gravitino authorization fails.
      verify(mockPaimonCatalog, never()).getTable(any());
    }
  }

  @Test
  public void testGetTableThrowsCatalogExceptionWhenPaimonOutOfSync()
      throws TableNotExistException, CatalogException {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      // Gravitino says the table exists ...
      when(mockTableCatalog.loadTable(identifier)).thenReturn(Mockito.mock(Table.class));
      // ... but Paimon / Hive metastore does not have it.
      when(mockPaimonCatalog.getTable(tablePath))
          .thenThrow(new TableNotExistException(CATALOG_NAME, tablePath));

      // Out-of-sync state must surface as a CatalogException, not TableNotExistException,
      // because authorization already passed in Gravitino.
      Assertions.assertThrows(CatalogException.class, () -> catalog.getTable(tablePath));
    }
  }
}
