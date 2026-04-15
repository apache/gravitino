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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Test for {@link GravitinoPaimonCatalog} */
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

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  /**
   * Registers a {@link GravitinoCatalogManager} mock in the given static scope and returns the
   * mocked {@link TableCatalog} so individual tests only need to set up operation-specific stubs.
   */
  private TableCatalog setupGravitinoCatalogMock(
      MockedStatic<GravitinoCatalogManager> mgrStatic, Catalog mockGravitinoCatalog) {
    GravitinoCatalogManager mockMgr = Mockito.mock(GravitinoCatalogManager.class);
    mgrStatic.when(GravitinoCatalogManager::get).thenReturn(mockMgr);
    when(mockMgr.getGravitinoCatalogInfo(CATALOG_NAME)).thenReturn(mockGravitinoCatalog);
    TableCatalog mockTableCatalog = Mockito.mock(TableCatalog.class);
    when(mockGravitinoCatalog.asTableCatalog()).thenReturn(mockTableCatalog);
    return mockTableCatalog;
  }

  // -----------------------------------------------------------------------
  // dropTable
  // -----------------------------------------------------------------------

  @Test
  public void testDropTableSyncsPaimonWhenDropped() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockTableCatalog.purgeTable(identifier)).thenReturn(true);
      doNothing().when(mockPaimonCatalog).dropTable(eq(tablePath), eq(true));

      catalog.dropTable(tablePath, false);

      verify(mockTableCatalog).purgeTable(identifier);
      verify(mockPaimonCatalog).dropTable(tablePath, true);
    }
  }

  @Test
  public void testDropTableDoesNotSyncPaimonWhenNotDropped() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      // purgeTable returns false: table did not exist in Gravitino
      when(mockTableCatalog.purgeTable(identifier)).thenReturn(false);

      // ignoreIfNotExists=true should not throw even when the table was not found
      catalog.dropTable(tablePath, true);

      verify(mockTableCatalog).purgeTable(identifier);
      // Paimon must NOT be touched when Gravitino reported nothing was dropped
      verify(mockPaimonCatalog, never()).dropTable(any(), eq(true));
    }
  }

  @Test
  public void testDropTableThrowsWhenNotExistsAndNotIgnoring() {
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
  public void testDropTablePaimonSyncFailureThrowsCatalogException() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockTableCatalog.purgeTable(identifier)).thenReturn(true);
      doThrow(new RuntimeException("paimon error"))
          .when(mockPaimonCatalog)
          .dropTable(eq(tablePath), eq(true));

      Assertions.assertThrows(CatalogException.class, () -> catalog.dropTable(tablePath, false));
      // Gravitino purge was still attempted
      verify(mockTableCatalog).purgeTable(identifier);
    }
  }

  // -----------------------------------------------------------------------
  // createTable — Paimon registered before Gravitino; rollback on failure
  // -----------------------------------------------------------------------

  @Test
  public void testCreateTableCallsPaimonBeforeGravitino() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    CatalogBaseTable mockTable = Mockito.mock(CatalogBaseTable.class);

    // paimon.createTable succeeds; super.createTable will throw because mockTable is not a
    // ResolvedCatalogBaseTable — that is expected; we only assert that paimon was called first.
    doNothing().when(mockPaimonCatalog).createTable(eq(tablePath), eq(mockTable), eq(false));

    Assertions.assertThrows(
        Exception.class, () -> catalog.createTable(tablePath, mockTable, false));

    verify(mockPaimonCatalog).createTable(tablePath, mockTable, false);
  }

  @Test
  public void testCreateTableRollsBackPaimonWhenGravitinoFails() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    CatalogBaseTable mockTable = Mockito.mock(CatalogBaseTable.class);

    // Paimon-side creation succeeds.
    doNothing().when(mockPaimonCatalog).createTable(eq(tablePath), eq(mockTable), eq(false));
    // Rollback (dropTable) must also succeed.
    doNothing().when(mockPaimonCatalog).dropTable(eq(tablePath), eq(true));

    // Gravitino will fail because mockTable is not a ResolvedCatalogBaseTable.
    Assertions.assertThrows(
        Exception.class, () -> catalog.createTable(tablePath, mockTable, false));

    // Paimon creation was attempted, and then rolled back.
    verify(mockPaimonCatalog).createTable(tablePath, mockTable, false);
    verify(mockPaimonCatalog).dropTable(tablePath, true);
  }

  @Test
  public void testCreateTableWrapsPaimonException() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    CatalogBaseTable mockTable = Mockito.mock(CatalogBaseTable.class);

    doThrow(new RuntimeException("hive error"))
        .when(mockPaimonCatalog)
        .createTable(eq(tablePath), eq(mockTable), eq(false));

    CatalogException ex =
        Assertions.assertThrows(
            CatalogException.class, () -> catalog.createTable(tablePath, mockTable, false));

    Assertions.assertTrue(ex.getMessage().contains(tablePath.toString()));
  }

  // -----------------------------------------------------------------------
  // alterTable — Paimon synced before Gravitino; early return when absent
  // -----------------------------------------------------------------------

  /**
   * When the table exists in Paimon, alterTable must sync to Paimon first. super.alterTable() is
   * then called; because loadTable returns no valid table (mocked to throw), the
   * ignoreIfNotExists=true path in BaseCatalog returns without further action.
   */
  @Test
  public void testAlterTableSyncsPaimonWhenTableExists() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    CatalogBaseTable newTable = Mockito.mock(CatalogBaseTable.class);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockPaimonCatalog.tableExists(tablePath)).thenReturn(true);
      doNothing().when(mockPaimonCatalog).alterTable(eq(tablePath), eq(newTable), eq(true));
      // loadTable throws so super.alterTable() returns early (ignoreIfNotExists=true)
      when(mockTableCatalog.loadTable(identifier)).thenThrow(new NoSuchTableException("not found"));

      catalog.alterTable(tablePath, newTable, true);

      verify(mockPaimonCatalog).tableExists(tablePath);
      verify(mockPaimonCatalog).alterTable(tablePath, newTable, true);
    }
  }

  /**
   * When the table is absent from Paimon and ignoreIfNotExists=true, the method must return early
   * without touching Gravitino — updating Gravitino alone would diverge the two stores.
   */
  @Test
  public void testAlterTableSkipsPaimonSyncWhenTableNotExists() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    CatalogBaseTable newTable = Mockito.mock(CatalogBaseTable.class);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockPaimonCatalog.tableExists(tablePath)).thenReturn(false);

      catalog.alterTable(tablePath, newTable, true);

      verify(mockPaimonCatalog).tableExists(tablePath);
      // Paimon must NOT be called when tableExists returns false
      verify(mockPaimonCatalog, never()).alterTable(any(), any(CatalogBaseTable.class), eq(true));
      // Gravitino must NOT be updated either to prevent divergence
      verify(mockTableCatalog, never()).loadTable(identifier);
    }
  }

  @Test
  public void testAlterTablePaimonSyncFailureThrowsCatalogException() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    CatalogBaseTable newTable = Mockito.mock(CatalogBaseTable.class);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockPaimonCatalog.tableExists(tablePath)).thenReturn(true);
      doThrow(new CatalogException("paimon sync error"))
          .when(mockPaimonCatalog)
          .alterTable(eq(tablePath), eq(newTable), eq(false));

      Assertions.assertThrows(
          CatalogException.class, () -> catalog.alterTable(tablePath, newTable, false));
    }
  }

  @Test
  public void testAlterTableWithTableChangesSyncsPaimon() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    CatalogBaseTable newTable = Mockito.mock(CatalogBaseTable.class);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockPaimonCatalog.tableExists(tablePath)).thenReturn(true);
      doNothing()
          .when(mockPaimonCatalog)
          .alterTable(eq(tablePath), eq(newTable), eq(Collections.emptyList()), eq(true));
      when(mockTableCatalog.loadTable(identifier)).thenThrow(new NoSuchTableException("not found"));

      catalog.alterTable(tablePath, newTable, Collections.emptyList(), true);

      verify(mockPaimonCatalog).tableExists(tablePath);
      verify(mockPaimonCatalog).alterTable(tablePath, newTable, Collections.emptyList(), true);
    }
  }

  /**
   * Mirror of {@link #testAlterTableSkipsPaimonSyncWhenTableNotExists} for the TableChanges
   * overload.
   */
  @Test
  public void testAlterTableWithTableChangesSkipsPaimonWhenNotExists() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    CatalogBaseTable newTable = Mockito.mock(CatalogBaseTable.class);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockPaimonCatalog.tableExists(tablePath)).thenReturn(false);

      catalog.alterTable(tablePath, newTable, Collections.emptyList(), true);

      verify(mockPaimonCatalog).tableExists(tablePath);
      verify(mockPaimonCatalog, never())
          .alterTable(any(), any(CatalogBaseTable.class), any(), eq(true));
      // Gravitino must NOT be updated to prevent divergence
      verify(mockTableCatalog, never()).loadTable(identifier);
    }
  }

  // -----------------------------------------------------------------------
  // Partition DDL delegation
  // -----------------------------------------------------------------------

  @Test
  public void testCreatePartitionDelegatesToPaimon() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    CatalogPartitionSpec spec = Mockito.mock(CatalogPartitionSpec.class);
    CatalogPartition partition = Mockito.mock(CatalogPartition.class);
    doNothing().when(mockPaimonCatalog).createPartition(tablePath, spec, partition, false);

    catalog.createPartition(tablePath, spec, partition, false);

    verify(mockPaimonCatalog).createPartition(tablePath, spec, partition, false);
  }

  @Test
  public void testDropPartitionDelegatesToPaimon() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    CatalogPartitionSpec spec = Mockito.mock(CatalogPartitionSpec.class);
    doNothing().when(mockPaimonCatalog).dropPartition(tablePath, spec, false);

    catalog.dropPartition(tablePath, spec, false);

    verify(mockPaimonCatalog).dropPartition(tablePath, spec, false);
  }

  @Test
  public void testAlterPartitionDelegatesToPaimon() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    CatalogPartitionSpec spec = Mockito.mock(CatalogPartitionSpec.class);
    CatalogPartition newPartition = Mockito.mock(CatalogPartition.class);
    doNothing().when(mockPaimonCatalog).alterPartition(tablePath, spec, newPartition, false);

    catalog.alterPartition(tablePath, spec, newPartition, false);

    verify(mockPaimonCatalog).alterPartition(tablePath, spec, newPartition, false);
  }

  // -----------------------------------------------------------------------
  // realCatalog / getFactory — unchanged behaviour
  // -----------------------------------------------------------------------

  @Test
  public void testRealCatalogReturnsPaimonCatalog() {
    Assertions.assertSame(mockPaimonCatalog, catalog.realCatalog());
  }

  @Test
  public void testGetFactoryReturnsFlinkTableFactory() {
    Assertions.assertTrue(catalog.getFactory().isPresent());
    Assertions.assertInstanceOf(
        org.apache.paimon.flink.FlinkTableFactory.class, catalog.getFactory().get());
  }

  // -----------------------------------------------------------------------
  // getTable — must enforce Gravitino authorization then return Paimon-native table
  //
  // Root-cause context: BaseCatalog.getTable() returns a plain CatalogTable
  // built from Gravitino metadata, which causes
  // AbstractFlinkTableFactory.buildPaimonTable() to create a FileStoreTable
  // with CatalogEnvironment.empty() (catalogLoader = null).  This makes
  // partitionHandler() return null so AddPartitionCommitCallback is never
  // registered, and Hive partition metadata is never updated after commits.
  //
  // Fix: (1) call Gravitino loadTable for authorization, then (2) return
  // paimonCatalog.getTable() which is a DataCatalogTable with proper
  // CatalogEnvironment.
  // -----------------------------------------------------------------------

  @Test
  public void testGetTableEnforcesGravitinoAuthAndReturnsPaimonTable()
      throws TableNotExistException, CatalogException {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);
    CatalogBaseTable paimonTable = Mockito.mock(CatalogBaseTable.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockTableCatalog.loadTable(identifier)).thenReturn(Mockito.mock(Table.class));
      when(mockPaimonCatalog.getTable(tablePath)).thenReturn(paimonTable);

      CatalogBaseTable result = catalog.getTable(tablePath);

      // Gravitino authorization must be enforced first.
      verify(mockTableCatalog).loadTable(identifier);
      // The returned object is the Paimon-native DataCatalogTable (with proper CatalogEnvironment).
      Assertions.assertSame(paimonTable, result);
      Mockito.verify(mockPaimonCatalog, Mockito.times(1)).getTable(tablePath);
    }
  }

  @Test
  public void testGetTablePropagatesNoSuchTableExceptionFromGravitino() {
    ObjectPath tablePath = new ObjectPath(TEST_DB, "nonExistingTable");
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, "nonExistingTable");
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockTableCatalog.loadTable(identifier))
          .thenThrow(new NoSuchTableException("table not found"));

      Assertions.assertThrows(TableNotExistException.class, () -> catalog.getTable(tablePath));
      // Paimon must NOT be called when Gravitino says the table does not exist.
      Mockito.verify(mockPaimonCatalog, never()).getTable(any());
    }
  }

  @Test
  public void testGetTablePaimonNotCalledWhenGravitinoThrows() {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);
    Catalog mockGravitinoCatalog = Mockito.mock(Catalog.class);

    try (MockedStatic<GravitinoCatalogManager> mgrStatic =
        Mockito.mockStatic(GravitinoCatalogManager.class)) {
      TableCatalog mockTableCatalog = setupGravitinoCatalogMock(mgrStatic, mockGravitinoCatalog);
      when(mockTableCatalog.loadTable(identifier))
          .thenThrow(new RuntimeException("authorization error"));

      Assertions.assertThrows(CatalogException.class, () -> catalog.getTable(tablePath));
      // Paimon must NOT be called when Gravitino authorization fails.
      Mockito.verify(mockPaimonCatalog, never()).getTable(any());
    }
  }
}
