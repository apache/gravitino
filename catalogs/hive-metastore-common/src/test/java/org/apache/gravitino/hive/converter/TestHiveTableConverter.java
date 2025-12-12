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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.rel.Column;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

public class TestHiveTableConverter {

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
}
