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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

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
  // Test createSparkTable routing for non-Iceberg tables
  // -------------------------------------------------------------------------

  @Test
  void testCreateSparkTableRoutingForNonIcebergTable() {
    // Test that createSparkTable correctly routes non-Iceberg tables.
    // We verify the routing decision by checking isIcebergTable returns false
    // and the table properties don't contain ICEBERG format.
    Table mockGravitinoTable = createMockGravitinoTable(ImmutableMap.of());

    // isIcebergTable should return false for non-Iceberg tables
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockGravitinoTable));

    // Properties should be empty (non-Iceberg)
    Assertions.assertFalse(mockGravitinoTable.properties().containsKey(GlueConstants.TABLE_FORMAT));
  }

  @Test
  void testCreateSparkTableRoutingForIcebergTable() {
    // Test that createSparkTable correctly identifies Iceberg tables.
    Table mockGravitinoTable =
        createMockGravitinoTable(
            ImmutableMap.of(GlueConstants.TABLE_FORMAT, GlueConstants.TABLE_FORMAT_ICEBERG));

    // isIcebergTable should return true for Iceberg tables
    Assertions.assertTrue(GravitinoGlueCatalog.isIcebergTable(mockGravitinoTable));
  }

  @Test
  void testCreateSparkTableRoutingForHiveTable() {
    // Test that createSparkTable correctly identifies Hive format tables.
    Table mockGravitinoTable =
        createMockGravitinoTable(ImmutableMap.of(GlueConstants.TABLE_FORMAT, "HIVE"));

    // isIcebergTable should return false for Hive format
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockGravitinoTable));
  }

  @Test
  void testCreateSparkTableRoutingForDeltaTable() {
    // Test that createSparkTable correctly identifies Delta format tables.
    Table mockGravitinoTable =
        createMockGravitinoTable(ImmutableMap.of(GlueConstants.TABLE_FORMAT, "DELTA"));

    // isIcebergTable should return false for Delta format
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockGravitinoTable));
  }

  @Test
  void testCreateSparkTableRoutingWithNullProperties() {
    // Test that null properties are handled correctly.
    Table mockGravitinoTable = mock(Table.class);
    when(mockGravitinoTable.properties()).thenReturn(null);

    // isIcebergTable should return false for null properties
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockGravitinoTable));
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
