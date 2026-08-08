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
package org.apache.gravitino.hive.converter;

import static org.apache.gravitino.catalog.hive.HiveConstants.TABLE_TYPE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

public class TestHiveTableConverter {

  @Test
  public void testRejectVariantBeforeBuildingHiveTable() {
    Column[] columns = {Column.of("payload", Types.VariantType.get(), "Semi-structured payload")};
    HiveTable hiveTable =
        HiveTable.builder()
            .withName("events")
            .withDatabaseName("db")
            .withColumns(columns)
            .withProperties(new HashMap<>())
            .withAuditInfo(
                AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> HiveTableConverter.toHiveTable(hiveTable));
    assertEquals(
        "Hive cannot preserve the Gravitino variant type because it has no equivalent type.",
        exception.getMessage());
  }

  @Test
  public void testConvertUnknownColumnToHiveVoid() {
    Column[] columns = {Column.of("future_value", Types.NullType.get(), "Unknown value")};
    HiveTable hiveTable =
        HiveTable.builder()
            .withName("events")
            .withDatabaseName("db")
            .withColumns(columns)
            .withProperties(new HashMap<>())
            .withAuditInfo(
                AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
            .build();

    Table convertedTable = HiveTableConverter.toHiveTable(hiveTable);
    assertEquals("void", convertedTable.getSd().getCols().get(0).getType());
    assertEquals(Types.NullType.get(), HiveTableConverter.getColumns(convertedTable)[0].dataType());
  }

  @Test
  public void testRejectGeometryBeforeBuildingHiveTable() {
    Column[] columns = {Column.of("shape", Types.GeometryType.crs84(), "Planar geometry")};
    HiveTable hiveTable =
        HiveTable.builder()
            .withName("places")
            .withDatabaseName("db")
            .withColumns(columns)
            .withProperties(new HashMap<>())
            .withAuditInfo(
                AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> HiveTableConverter.toHiveTable(hiveTable));
    assertEquals(
        "Hive cannot preserve the Gravitino geometry type because it has no geometry type with CRS metadata.",
        exception.getMessage());
  }

  @Test
  public void testRejectGeographyBeforeBuildingHiveTable() {
    Column[] columns = {Column.of("region", Types.GeographyType.crs84(), "Spheroidal geography")};
    HiveTable hiveTable =
        HiveTable.builder()
            .withName("places")
            .withDatabaseName("db")
            .withColumns(columns)
            .withProperties(new HashMap<>())
            .withAuditInfo(
                AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> HiveTableConverter.toHiveTable(hiveTable));
    assertEquals(
        "Hive cannot preserve the Gravitino geography type because it has no geography type with CRS and edge-algorithm metadata.",
        exception.getMessage());
  }

  @Test
  public void testGetColumnsWithNoDuplicates() {
    // Create a table with columns and partition keys that don't overlap
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();

    // Regular columns
    List<FieldSchema> cols =
        Arrays.asList(
            new FieldSchema("id", "int", "ID column"),
            new FieldSchema("name", "string", "Name column"),
            new FieldSchema("age", "int", "Age column"));
    sd.setCols(cols);
    table.setSd(sd);

    // Partition keys (no overlap with regular columns)
    List<FieldSchema> partitionKeys =
        Arrays.asList(
            new FieldSchema("year", "int", "Year partition"),
            new FieldSchema("month", "int", "Month partition"));
    table.setPartitionKeys(partitionKeys);

    Column[] columns = HiveTableConverter.getColumns(table);

    // Should have all 5 columns (3 regular + 2 partition)
    assertEquals(5, columns.length);
    assertEquals("id", columns[0].name());
    assertEquals("name", columns[1].name());
    assertEquals("age", columns[2].name());
    assertEquals("year", columns[3].name());
    assertEquals("month", columns[4].name());
  }

  @Test
  public void testGetColumnsWithDuplicates() {
    // Create a table with columns and partition keys that have duplicates
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();

    // Regular columns
    List<FieldSchema> cols =
        Arrays.asList(
            new FieldSchema("id", "int", "ID column"),
            new FieldSchema("name", "string", "Name column"),
            new FieldSchema("date", "string", "Date column"));
    sd.setCols(cols);
    table.setSd(sd);

    // Partition keys (with duplicates from regular columns)
    List<FieldSchema> partitionKeys =
        Arrays.asList(
            new FieldSchema("date", "string", "Date partition"), // Duplicate!
            new FieldSchema("name", "string", "Name partition"), // Duplicate!
            new FieldSchema("region", "string", "Region partition") // New column
            );
    table.setPartitionKeys(partitionKeys);

    Column[] columns = HiveTableConverter.getColumns(table);

    // Should have only 4 columns (3 regular + 1 unique partition)
    // The duplicates (date and name) should not be added again
    assertEquals(4, columns.length);
    assertEquals("id", columns[0].name());
    assertEquals("name", columns[1].name());
    assertEquals("date", columns[2].name());
    assertEquals("region", columns[3].name());
  }

  @Test
  public void testGetColumnsWithAllDuplicates() {
    // Create a table where all partition keys are duplicates
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();

    // Regular columns
    List<FieldSchema> cols =
        Arrays.asList(
            new FieldSchema("col1", "int", "Column 1"),
            new FieldSchema("col2", "string", "Column 2"),
            new FieldSchema("col3", "double", "Column 3"));
    sd.setCols(cols);
    table.setSd(sd);

    // Partition keys (all duplicates from regular columns)
    List<FieldSchema> partitionKeys =
        Arrays.asList(
            new FieldSchema("col1", "int", "Column 1 partition"),
            new FieldSchema("col2", "string", "Column 2 partition"),
            new FieldSchema("col3", "double", "Column 3 partition"));
    table.setPartitionKeys(partitionKeys);

    Column[] columns = HiveTableConverter.getColumns(table);

    // Should have only 3 columns (all from regular columns, no duplicates from partition keys)
    assertEquals(3, columns.length);
    assertEquals("col1", columns[0].name());
    assertEquals("col2", columns[1].name());
    assertEquals("col3", columns[2].name());
  }

  @Test
  public void testGetColumnsWithEmptyPartitionKeys() {
    // Create a table with no partition keys
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();

    // Regular columns
    List<FieldSchema> cols =
        Arrays.asList(
            new FieldSchema("id", "int", "ID column"),
            new FieldSchema("name", "string", "Name column"));
    sd.setCols(cols);
    table.setSd(sd);

    // Empty partition keys
    table.setPartitionKeys(Arrays.asList());

    Column[] columns = HiveTableConverter.getColumns(table);

    // Should have only 2 columns from regular columns
    assertEquals(2, columns.length);
    assertEquals("id", columns[0].name());
    assertEquals("name", columns[1].name());
  }

  @Test
  public void testToHiveTablePreservesVirtualViewColumnsInStorageDescriptor() {
    Column[] columns = {
      Column.of("id", Types.IntegerType.get(), "ID column"),
      Column.of("name", Types.StringType.get(), "Name column")
    };
    HiveTable hiveTable =
        HiveTable.builder()
            .withName("v_orders")
            .withDatabaseName("db")
            .withColumns(columns)
            .withProperties(new HashMap<>(Map.of(TABLE_TYPE, TableType.VIRTUAL_VIEW.name())))
            .withAuditInfo(
                AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
            .withViewOriginalText("SELECT id, name FROM t")
            .build();

    Table table = HiveTableConverter.toHiveTable(hiveTable);

    assertNotNull(table.getSd());
    assertEquals(2, table.getSd().getColsSize());
    assertEquals("id", table.getSd().getCols().get(0).getName());
    assertEquals("name", table.getSd().getCols().get(1).getName());
    assertEquals("SELECT id, name FROM t", table.getViewOriginalText());
    assertEquals("SELECT id, name FROM t", table.getViewExpandedText());
  }

  @Test
  public void testToHiveTableMirrorsOriginalTextToExpandedText() {
    Column[] columns = {Column.of("id", Types.IntegerType.get(), "ID column")};
    HiveTable hiveTable =
        HiveTable.builder()
            .withName("v_orders")
            .withDatabaseName("db")
            .withColumns(columns)
            .withProperties(new HashMap<>(Map.of(TABLE_TYPE, TableType.VIRTUAL_VIEW.name())))
            .withAuditInfo(
                AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
            .withViewOriginalText("SELECT `db`.`t`.`id` FROM `db`.`t`")
            .build();

    Table table = HiveTableConverter.toHiveTable(hiveTable);

    assertEquals("SELECT `db`.`t`.`id` FROM `db`.`t`", table.getViewOriginalText());
    assertEquals("SELECT `db`.`t`.`id` FROM `db`.`t`", table.getViewExpandedText());
  }

  @Test
  public void testFromVirtualViewWithoutStorageDescriptorDoesNotThrow() {
    Table table = new Table();
    table.setTableName("v_orders");
    table.setDbName("db");
    table.setTableType(TableType.VIRTUAL_VIEW.name());
    table.setParameters(new HashMap<>());
    table.setPartitionKeys(null);
    table.setSd(null);
    table.setViewOriginalText(null);
    table.setViewExpandedText("SELECT `1`");

    HiveTable hiveTable = assertDoesNotThrow(() -> HiveTableConverter.fromHiveTable(table));

    assertEquals("v_orders", hiveTable.name());
    assertEquals(0, hiveTable.columns().length);
    assertEquals("SELECT `1`", hiveTable.viewOriginalText());
  }
}
