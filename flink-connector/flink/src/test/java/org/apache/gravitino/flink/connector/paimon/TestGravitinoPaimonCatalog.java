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
import org.apache.gravitino.rel.Table;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

/** Test for {@link TestGravitinoPaimonCatalog} */
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
  public void testToFlinkTableDelegatesToPaimonCatalog()
      throws TableNotExistException, CatalogException {
    ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
    CatalogBaseTable expectedTable = Mockito.mock(CatalogBaseTable.class);
    when(mockPaimonCatalog.getTable(tablePath)).thenReturn(expectedTable);

    // toFlinkTable is protected and accessible from this same package
    CatalogBaseTable result = catalog.toFlinkTable(Mockito.mock(Table.class), tablePath);

    // The returned object must be exactly the DataCatalogTable from paimonCatalog,
    // not a plain CatalogTable reconstructed from Gravitino metadata.
    Assertions.assertSame(expectedTable, result);
    Mockito.verify(mockPaimonCatalog, Mockito.times(1)).getTable(tablePath);
  }

  @Test
  public void testToFlinkTableWrapsTableNotExistException()
      throws TableNotExistException, CatalogException {
    ObjectPath tablePath = new ObjectPath(TEST_DB, "nonExistingTable");
    when(mockPaimonCatalog.getTable(tablePath))
        .thenThrow(new TableNotExistException("test-paimon-catalog", tablePath));

    // TableNotExistException from Paimon must be wrapped in CatalogException because
    // toFlinkTable() is called from BaseCatalog.getTable() after Gravitino already confirmed
    // the table exists; reaching this point with a missing Paimon table is a metadata mismatch.
    Assertions.assertThrows(
        CatalogException.class, () -> catalog.toFlinkTable(Mockito.mock(Table.class), tablePath));
    Mockito.verify(mockPaimonCatalog, Mockito.times(1)).getTable(tablePath);
  }
}
