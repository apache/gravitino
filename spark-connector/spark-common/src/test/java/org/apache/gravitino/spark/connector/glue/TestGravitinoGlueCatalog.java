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

package org.apache.gravitino.spark.connector.glue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

@TestInstance(Lifecycle.PER_CLASS)
public class TestGravitinoGlueCatalog {

  private GravitinoGlueCatalog gravitinoGlueCatalog;

  @BeforeAll
  void initCatalogManager() {
    // GravitinoGlueCatalog extends BaseCatalog which calls GravitinoCatalogManager.get()
    // in its constructor, so we must initialize the manager first.
    GravitinoClient mockClient = mock(GravitinoClient.class);
    GravitinoCatalogManager.create(() -> mockClient);
  }

  @AfterAll
  void cleanupCatalogManager() {
    GravitinoCatalogManager.get().close();
  }

  @BeforeEach
  void setUp() {
    gravitinoGlueCatalog = new GravitinoGlueCatalog();
  }

  // -------------------------------------------------------------------------
  // Test isIcebergTable (static package-private method)
  // -------------------------------------------------------------------------

  @Test
  void testIsIcebergTableWithIcebergFormat() {
    Table mockTable =
        createMockGravitinoTable(
            ImmutableMap.of(GlueConstants.TABLE_FORMAT, GlueConstants.TABLE_FORMAT_ICEBERG));
    Assertions.assertTrue(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithIcebergFormatLowercase() {
    Table mockTable =
        createMockGravitinoTable(ImmutableMap.of(GlueConstants.TABLE_FORMAT, "iceberg"));
    Assertions.assertTrue(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithIcebergFormatMixedCase() {
    Table mockTable =
        createMockGravitinoTable(ImmutableMap.of(GlueConstants.TABLE_FORMAT, "IcEbErG"));
    Assertions.assertTrue(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithHiveFormat() {
    Table mockTable = createMockGravitinoTable(ImmutableMap.of(GlueConstants.TABLE_FORMAT, "HIVE"));
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithDeltaFormat() {
    Table mockTable =
        createMockGravitinoTable(ImmutableMap.of(GlueConstants.TABLE_FORMAT, "DELTA"));
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithEmptyProperties() {
    Table mockTable = createMockGravitinoTable(ImmutableMap.of());
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithNullProperties() {
    Table mockTable = mock(Table.class);
    when(mockTable.properties()).thenReturn(null);
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithNoTableFormatProperty() {
    Table mockTable =
        createMockGravitinoTable(ImmutableMap.of("aws-location", "s3://bucket/table"));
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithEmptyTableFormat() {
    Table mockTable = createMockGravitinoTable(ImmutableMap.of(GlueConstants.TABLE_FORMAT, ""));
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithNativeIcebergTableType() {
    // Tables created directly by Iceberg GlueCatalog use table_type=ICEBERG (no table-format key).
    Table mockTable = createMockGravitinoTable(ImmutableMap.of("table_type", "ICEBERG"));
    Assertions.assertTrue(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithNativeIcebergTableTypeLowercase() {
    Table mockTable = createMockGravitinoTable(ImmutableMap.of("table_type", "iceberg"));
    Assertions.assertTrue(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  // -------------------------------------------------------------------------
  // Test getSparkTypeConverter returns correct type
  // -------------------------------------------------------------------------

  @Test
  void testGetSparkTypeConverter() {
    SparkTypeConverter typeConverter = gravitinoGlueCatalog.getSparkTypeConverter();
    Assertions.assertNotNull(typeConverter);
    // Should use Hive type converter
    Assertions.assertInstanceOf(
        org.apache.gravitino.spark.connector.hive.SparkHiveTypeConverter.class, typeConverter);
  }

  // -------------------------------------------------------------------------
  // Test getPropertiesConverter returns GluePropertiesConverter
  // -------------------------------------------------------------------------

  @Test
  void testGetPropertiesConverter() {
    PropertiesConverter converter = gravitinoGlueCatalog.getPropertiesConverter();
    Assertions.assertNotNull(converter);
    Assertions.assertInstanceOf(GluePropertiesConverter.class, converter);
  }

  @Test
  void testGetSparkTransformConverter() {
    SparkTransformConverter transformer = gravitinoGlueCatalog.getSparkTransformConverter();
    Assertions.assertNotNull(transformer);
  }

  // -------------------------------------------------------------------------
  // Test renameTable Derby cleanup
  // -------------------------------------------------------------------------

  @Test
  void testRenameTableDropsDerbyEntryForOldIdentifier()
      throws NoSuchTableException, TableAlreadyExistsException {
    TableCatalog mockCatalog = mock(TableCatalog.class);
    Identifier oldIdent = Identifier.of(new String[] {"db"}, "old_table");
    Identifier newIdent = Identifier.of(new String[] {"db"}, "new_table");

    // Subclass that bypasses the Gravitino API call to isolate Derby cleanup behavior.
    GravitinoGlueCatalog catalog =
        new GravitinoGlueCatalog() {
          {
            sparkCatalog = mockCatalog;
          }

          @Override
          public void renameTable(Identifier old, Identifier next)
              throws NoSuchTableException, TableAlreadyExistsException {
            // Simulate a successful Gravitino rename, then run Derby cleanup.
            dropFromDerby(old);
          }
        };

    catalog.renameTable(oldIdent, newIdent);

    InOrder order = inOrder(mockCatalog);
    order.verify(mockCatalog).invalidateTable(oldIdent);
    order.verify(mockCatalog).dropTable(oldIdent);
    org.mockito.Mockito.verify(mockCatalog, never()).invalidateTable(newIdent);
    org.mockito.Mockito.verify(mockCatalog, never()).dropTable(newIdent);
  }

  @Test
  void testRenameTableDoesNotCleanDerbyWhenRenameFails() {
    TableCatalog mockCatalog = mock(TableCatalog.class);
    Identifier oldIdent = Identifier.of(new String[] {"db"}, "old_table");
    Identifier newIdent = Identifier.of(new String[] {"db"}, "new_table");

    // Subclass that simulates a failed Gravitino rename (throws before Derby cleanup).
    GravitinoGlueCatalog catalog =
        new GravitinoGlueCatalog() {
          {
            sparkCatalog = mockCatalog;
          }

          @Override
          public void renameTable(Identifier old, Identifier next)
              throws NoSuchTableException, TableAlreadyExistsException {
            throw new RuntimeException("Simulated Gravitino rename failure");
          }
        };

    Assertions.assertThrows(RuntimeException.class, () -> catalog.renameTable(oldIdent, newIdent));
    org.mockito.Mockito.verify(mockCatalog, never()).invalidateTable(oldIdent);
    org.mockito.Mockito.verify(mockCatalog, never()).dropTable(oldIdent);
  }

  // -------------------------------------------------------------------------
  // Test Derby sync: loadSparkTable retries after Derby miss
  // -------------------------------------------------------------------------

  @Test
  @SuppressWarnings("unchecked")
  void testLoadSparkTableSyncsDerbyOnTableMiss() throws Exception {
    TableCatalog mockCatalog = mock(TableCatalog.class);
    org.apache.spark.sql.connector.catalog.Table mockSparkTable =
        mock(org.apache.spark.sql.connector.catalog.Table.class);
    Table mockGravitinoTable = createMockGravitinoTable(ImmutableMap.of());

    Identifier ident = Identifier.of(new String[] {"db"}, "tbl");
    // First loadTable call simulates Derby miss; second returns the table.
    when(mockCatalog.loadTable(any()))
        .thenThrow(new NoSuchTableException(ident))
        .thenReturn(mockSparkTable);

    GravitinoGlueCatalog catalog =
        new GravitinoGlueCatalog() {
          {
            sparkCatalog = mockCatalog;
          }

          @Override
          protected Table loadGravitinoTable(Identifier ident) throws NoSuchTableException {
            return mockGravitinoTable;
          }
        };

    org.apache.spark.sql.connector.catalog.Table result = catalog.loadSparkTable(ident);

    Assertions.assertSame(mockSparkTable, result);
    // createTable must be called exactly once to sync the Derby miss.
    verify(mockCatalog).createTable(eq(ident), any(), any(Transform[].class), any(Map.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testSyncTableToDerbyPassesStorageDescriptorProperties() throws Exception {
    TableCatalog mockCatalog = mock(TableCatalog.class);
    org.apache.spark.sql.connector.catalog.Table mockSparkTable =
        mock(org.apache.spark.sql.connector.catalog.Table.class);
    Table mockGravitinoTable =
        createMockGravitinoTable(
            ImmutableMap.of(
                GlueConstants.LOCATION, "s3://bucket/table",
                GlueConstants.INPUT_FORMAT_CLASS, "org.apache.hadoop.mapred.TextInputFormat",
                GlueConstants.OUTPUT_FORMAT,
                    "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                GlueConstants.SERDE_LIB, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"));

    Identifier ident = Identifier.of(new String[] {"db"}, "tbl");
    when(mockCatalog.loadTable(any()))
        .thenThrow(new NoSuchTableException(ident))
        .thenReturn(mockSparkTable);

    GravitinoGlueCatalog catalog =
        new GravitinoGlueCatalog() {
          {
            sparkCatalog = mockCatalog;
          }

          @Override
          protected Table loadGravitinoTable(Identifier ident) throws NoSuchTableException {
            return mockGravitinoTable;
          }
        };

    catalog.loadSparkTable(ident);

    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockCatalog)
        .createTable(eq(ident), any(), any(Transform[].class), propsCaptor.capture());
    Map<String, String> captured = propsCaptor.getValue();
    Assertions.assertEquals("s3://bucket/table", captured.get("location"));
    Assertions.assertEquals(
        "org.apache.hadoop.mapred.TextInputFormat", captured.get("hive.input-format"));
    Assertions.assertEquals(
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        captured.get("hive.output-format"));
    Assertions.assertEquals(
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", captured.get("hive.serde"));
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  /** Creates a mock Gravitino Table with the given properties. */
  private Table createMockGravitinoTable(java.util.Map<String, String> properties) {
    Table mockTable = mock(Table.class);
    when(mockTable.properties()).thenReturn(new HashMap<>(properties));
    when(mockTable.name()).thenReturn("test_db.test_table");
    when(mockTable.columns())
        .thenReturn(new Column[] {Column.of("id", Types.IntegerType.get(), "id column")});
    return mockTable;
  }
}
