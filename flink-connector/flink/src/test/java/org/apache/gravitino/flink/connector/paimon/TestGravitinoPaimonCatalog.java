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
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

/**
 * Unit tests for {@link GravitinoPaimonCatalog}, specifically verifying that {@link
 * GravitinoPaimonCatalog#getTable(ObjectPath)} and {@link
 * GravitinoPaimonCatalog#tableExists(ObjectPath)} delegate to the underlying Paimon FlinkCatalog.
 *
 * <p>Background: the parent {@link
 * org.apache.gravitino.flink.connector.catalog.BaseCatalog#getTable(ObjectPath)} loads a table via
 * Gravitino's REST API and returns a plain Flink {@link
 * org.apache.flink.table.catalog.CatalogTable}. This causes {@code
 * AbstractFlinkTableFactory.buildPaimonTable()} to create a {@code FileStoreTable} with {@code
 * CatalogEnvironment.empty()} (catalogLoader = null), so {@code partitionHandler()} returns null
 * and {@code AddPartitionCommitCallback} is never registered, resulting in Hive partition metadata
 * never being updated after commits.
 *
 * <p>The fix overrides {@code getTable()} to delegate to the underlying Paimon {@code
 * FlinkCatalog}, which returns a {@code DataCatalogTable} wrapping a {@code FileStoreTable} with a
 * proper {@code CatalogEnvironment}. These tests verify this delegation contract.
 */
public class TestGravitinoPaimonCatalog {

  private static final String TEST_DB = "testDb";
  private static final String TEST_TABLE = "testTable";

  private FlinkCatalog mockPaimonCatalog;
  private GravitinoPaimonCatalog catalog;

  @BeforeEach
  void setUp() {
    mockPaimonCatalog = Mockito.mock(FlinkCatalog.class);
    CatalogFactory.Context mockContext = Mockito.mock(CatalogFactory.Context.class);
    when(mockContext.getName()).thenReturn("test-paimon-catalog");
    when(mockContext.getOptions())
        .thenReturn(
            ImmutableMap.of(
                "type", GravitinoPaimonCatalogFactoryOptions.IDENTIFIER,
                "warehouse", "file:///tmp/test_warehouse",
                "metastore", "filesystem"));

    // MockedConstruction intercepts new FlinkCatalogFactory() during GravitinoPaimonCatalog's
    // constructor, injecting mockPaimonCatalog. The try block only needs to cover construction;
    // the already-created mock remains valid after the MockedConstruction is closed.
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

  @Test
  public void testGetTableDelegatesToPaimonCatalog()
      throws TableNotExistException, CatalogException {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    CatalogBaseTable expectedTable = Mockito.mock(CatalogBaseTable.class);
    when(mockPaimonCatalog.getTable(tablePath)).thenReturn(expectedTable);

    CatalogBaseTable result = catalog.getTable(tablePath);

    // The returned object must be exactly what paimonCatalog returned (e.g. DataCatalogTable),
    // not a copy or re-converted plain CatalogTable from BaseCatalog.toFlinkTable().
    Assertions.assertSame(expectedTable, result);
    Mockito.verify(mockPaimonCatalog, Mockito.times(1)).getTable(tablePath);
  }

  @Test
  public void testGetTablePropagatesTableNotExistException()
      throws TableNotExistException, CatalogException {
    ObjectPath tablePath = new ObjectPath(TEST_DB, "nonExistingTable");
    when(mockPaimonCatalog.getTable(tablePath))
        .thenThrow(new TableNotExistException("test-paimon-catalog", tablePath));

    Assertions.assertThrows(TableNotExistException.class, () -> catalog.getTable(tablePath));
    Mockito.verify(mockPaimonCatalog, Mockito.times(1)).getTable(tablePath);
  }

  @Test
  public void testTableExistsDelegatesToPaimonCatalogWhenTrue() throws CatalogException {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    when(mockPaimonCatalog.tableExists(tablePath)).thenReturn(true);

    Assertions.assertTrue(catalog.tableExists(tablePath));
    Mockito.verify(mockPaimonCatalog, Mockito.times(1)).tableExists(tablePath);
  }

  @Test
  public void testTableExistsDelegatesToPaimonCatalogWhenFalse() throws CatalogException {
    ObjectPath tablePath = new ObjectPath(TEST_DB, "nonExistingTable");
    when(mockPaimonCatalog.tableExists(tablePath)).thenReturn(false);

    Assertions.assertFalse(catalog.tableExists(tablePath));
    Mockito.verify(mockPaimonCatalog, Mockito.times(1)).tableExists(tablePath);
  }

  @Test
  public void testGetTableAndTableExistsAreConsistent()
      throws TableNotExistException, CatalogException {
    ObjectPath existingPath = new ObjectPath(TEST_DB, TEST_TABLE);
    ObjectPath missingPath = new ObjectPath(TEST_DB, "missing");

    when(mockPaimonCatalog.tableExists(existingPath)).thenReturn(true);
    when(mockPaimonCatalog.tableExists(missingPath)).thenReturn(false);
    when(mockPaimonCatalog.getTable(existingPath)).thenReturn(Mockito.mock(CatalogBaseTable.class));
    when(mockPaimonCatalog.getTable(missingPath))
        .thenThrow(new TableNotExistException("test-paimon-catalog", missingPath));

    Assertions.assertTrue(catalog.tableExists(existingPath));
    Assertions.assertNotNull(catalog.getTable(existingPath));

    Assertions.assertFalse(catalog.tableExists(missingPath));
    Assertions.assertThrows(TableNotExistException.class, () -> catalog.getTable(missingPath));
  }
}
