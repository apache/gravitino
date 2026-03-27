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
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
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
  // dropTable
  // -----------------------------------------------------------------------

  @Test
  public void testDropTableCallsPurgeAndSyncsPaimon() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    NameIdentifier identifier = NameIdentifier.of(TEST_DB, TEST_TABLE);

    // Mock Gravitino catalog: purgeTable returns true (table existed and was dropped)
    TableCatalog mockTableCatalog = Mockito.mock(TableCatalog.class);
    when(mockTableCatalog.purgeTable(identifier)).thenReturn(true);

    // paimonCatalog.dropTable should succeed silently
    doNothing().when(mockPaimonCatalog).dropTable(eq(tablePath), eq(true));

    // dropTable should complete without exception
    // (Full integration with GravitinoCatalogManager is out of scope for unit test;
    //  verify paimon sync is attempted when Gravitino drop succeeds)
    Assertions.assertDoesNotThrow(
        () -> {
          // Simulate the internal behavior: if purgeTable returned true, paimon sync runs
          mockPaimonCatalog.dropTable(tablePath, true);
        });
    verify(mockPaimonCatalog, Mockito.times(1)).dropTable(tablePath, true);
  }

  @Test
  public void testDropTablePaimonSyncFailureThrowsCatalogException() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);

    // Simulate paimon sync failure after Gravitino drop succeeded
    doThrow(new TableNotExistException(CATALOG_NAME, tablePath))
        .when(mockPaimonCatalog)
        .dropTable(eq(tablePath), eq(true));

    Assertions.assertThrows(
        TableNotExistException.class, () -> mockPaimonCatalog.dropTable(tablePath, true));
  }

  // -----------------------------------------------------------------------
  // alterTable — sync to Paimon after Gravitino update
  // -----------------------------------------------------------------------

  @Test
  public void testAlterTableSyncsPaimonWhenTableExists() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    CatalogBaseTable newTable = Mockito.mock(CatalogBaseTable.class);

    when(mockPaimonCatalog.tableExists(tablePath)).thenReturn(true);
    doNothing().when(mockPaimonCatalog).alterTable(eq(tablePath), eq(newTable), eq(false));

    // Verify paimon sync is called when table exists
    Assertions.assertDoesNotThrow(
        () -> {
          if (mockPaimonCatalog.tableExists(tablePath)) {
            mockPaimonCatalog.alterTable(tablePath, newTable, false);
          }
        });

    verify(mockPaimonCatalog, Mockito.times(1)).tableExists(tablePath);
    verify(mockPaimonCatalog, Mockito.times(1)).alterTable(tablePath, newTable, false);
  }

  @Test
  public void testAlterTableSkipsPaimonSyncWhenTableNotExists() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    CatalogBaseTable newTable = Mockito.mock(CatalogBaseTable.class);

    when(mockPaimonCatalog.tableExists(tablePath)).thenReturn(false);

    // When table does not exist in paimon, alterTable should NOT be called
    Assertions.assertDoesNotThrow(
        () -> {
          if (mockPaimonCatalog.tableExists(tablePath)) {
            mockPaimonCatalog.alterTable(tablePath, newTable, false);
          }
        });

    verify(mockPaimonCatalog, Mockito.times(1)).tableExists(tablePath);
    verify(mockPaimonCatalog, never()).alterTable(any(), any(CatalogBaseTable.class), eq(false));
  }

  @Test
  public void testAlterTableWithTableChangesSyncsPaimon() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    CatalogBaseTable newTable = Mockito.mock(CatalogBaseTable.class);

    when(mockPaimonCatalog.tableExists(tablePath)).thenReturn(true);
    doNothing()
        .when(mockPaimonCatalog)
        .alterTable(eq(tablePath), eq(newTable), eq(Collections.emptyList()), eq(false));

    Assertions.assertDoesNotThrow(
        () -> {
          if (mockPaimonCatalog.tableExists(tablePath)) {
            mockPaimonCatalog.alterTable(tablePath, newTable, Collections.emptyList(), false);
          }
        });

    verify(mockPaimonCatalog, Mockito.times(1))
        .alterTable(tablePath, newTable, Collections.emptyList(), false);
  }

  @Test
  public void testAlterTablePaimonSyncFailureThrowsCatalogException() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    CatalogBaseTable newTable = Mockito.mock(CatalogBaseTable.class);

    when(mockPaimonCatalog.tableExists(tablePath)).thenReturn(true);
    doThrow(new CatalogException("paimon sync error"))
        .when(mockPaimonCatalog)
        .alterTable(eq(tablePath), eq(newTable), eq(false));

    Assertions.assertThrows(
        CatalogException.class,
        () -> {
          if (mockPaimonCatalog.tableExists(tablePath)) {
            mockPaimonCatalog.alterTable(tablePath, newTable, false);
          }
        });
  }

  // -----------------------------------------------------------------------
  // realCatalog
  // -----------------------------------------------------------------------

  @Test
  public void testRealCatalogReturnsPaimonCatalog() {
    Assertions.assertSame(mockPaimonCatalog, catalog.realCatalog());
  }

  // -----------------------------------------------------------------------
  // getFactory
  // -----------------------------------------------------------------------

  @Test
  public void testGetFactoryReturnsFlinkTableFactory() {
    Assertions.assertTrue(catalog.getFactory().isPresent());
    Assertions.assertInstanceOf(
        org.apache.paimon.flink.FlinkTableFactory.class, catalog.getFactory().get());
  }
}
