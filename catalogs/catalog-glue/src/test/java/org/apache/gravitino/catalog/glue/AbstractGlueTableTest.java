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
package org.apache.gravitino.catalog.glue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Table;

/**
 * Abstract base for {@link GlueTable} conversion tests.
 *
 * <p>Subclasses supply the {@link Table} object — either via SDK builder (synthetic) or via the
 * real Glue API — while the test scenarios are defined once here.
 */
abstract class AbstractGlueTableTest {

  private static final GlueTypeConverter TYPE_CONVERTER = new GlueTypeConverter();

  /** Returns a Hive-format Glue table with columns, partition keys, buckets, and sort columns. */
  protected abstract Table provideHiveTable(String schemaName, String tableName);

  /** Returns an Iceberg-format Glue table (empty StorageDescriptor columns). */
  protected abstract Table provideIcebergTable(String schemaName, String tableName);

  /** Returns a table with no StorageDescriptor (edge case). */
  protected abstract Table provideMinimalTable(String schemaName, String tableName);

  /** Clean up after each test. Default: no-op. */
  protected void cleanup(String schemaName, String tableName) {}

  // -------------------------------------------------------------------------
  // Test scenarios
  // -------------------------------------------------------------------------

  @Test
  void testHiveTableColumnMapping() {
    String schema = uniqueName("s");
    String table = uniqueName("hive_tbl");
    Table glueTable = provideHiveTable(schema, table);
    try {
      GlueTable t = GlueTable.fromGlueTable(glueTable, TYPE_CONVERTER);
      assertEquals(table, t.name());

      // data columns: id (bigint) + name (string); partition: dt (date)
      assertEquals(3, t.columns().length);
      Column id = t.columns()[0];
      assertEquals("id", id.name());
      assertEquals(Types.LongType.get(), id.dataType());

      Column name = t.columns()[1];
      assertEquals("name", name.name());
      assertEquals(Types.StringType.get(), name.dataType());

      Column dt = t.columns()[2];
      assertEquals("dt", dt.name());
      assertEquals(Types.DateType.get(), dt.dataType());
    } finally {
      cleanup(schema, table);
    }
  }

  @Test
  void testHiveTablePartitioning() {
    String schema = uniqueName("s");
    String table = uniqueName("part_tbl");
    Table glueTable = provideHiveTable(schema, table);
    try {
      GlueTable t = GlueTable.fromGlueTable(glueTable, TYPE_CONVERTER);
      assertEquals(1, t.partitioning().length);
      assertEquals(Transforms.identity("dt"), t.partitioning()[0]);
    } finally {
      cleanup(schema, table);
    }
  }

  @Test
  void testHiveTableDistribution() {
    String schema = uniqueName("s");
    String table = uniqueName("bucket_tbl");
    Table glueTable = provideHiveTable(schema, table);
    try {
      GlueTable t = GlueTable.fromGlueTable(glueTable, TYPE_CONVERTER);
      // 4 buckets on "id"
      assertEquals(4, t.distribution().number());
    } finally {
      cleanup(schema, table);
    }
  }

  @Test
  void testHiveTableSortOrders() {
    String schema = uniqueName("s");
    String table = uniqueName("sort_tbl");
    Table glueTable = provideHiveTable(schema, table);
    try {
      GlueTable t = GlueTable.fromGlueTable(glueTable, TYPE_CONVERTER);
      assertEquals(1, t.sortOrder().length);
      assertEquals(SortDirection.ASCENDING, t.sortOrder()[0].direction());
    } finally {
      cleanup(schema, table);
    }
  }

  @Test
  void testHiveTableStorageDescriptorProperties() {
    String schema = uniqueName("s");
    String table = uniqueName("sd_props_tbl");
    Table glueTable = provideHiveTable(schema, table);
    try {
      GlueTable t = GlueTable.fromGlueTable(glueTable, TYPE_CONVERTER);
      assertNotNull(t.properties().get(GlueConstants.LOCATION));
      assertNotNull(t.properties().get(GlueConstants.INPUT_FORMAT));
      assertNotNull(t.properties().get(GlueConstants.OUTPUT_FORMAT));
      assertNotNull(t.properties().get(GlueConstants.SERDE_LIB));
      assertEquals("EXTERNAL_TABLE", t.properties().get(GlueConstants.TABLE_TYPE));
    } finally {
      cleanup(schema, table);
    }
  }

  @Test
  void testIcebergTableParametersPassThrough() {
    String schema = uniqueName("s");
    String table = uniqueName("iceberg_tbl");
    Table glueTable = provideIcebergTable(schema, table);
    try {
      GlueTable t = GlueTable.fromGlueTable(glueTable, TYPE_CONVERTER);
      // Iceberg tables may have no data columns
      assertEquals("ICEBERG", t.properties().get(GlueConstants.TABLE_FORMAT));
      assertNotNull(t.properties().get(GlueConstants.METADATA_LOCATION));
      // No partition transforms (Iceberg manages partitioning itself)
      assertEquals(0, t.partitioning().length);
      // No distribution / sort orders
      assertEquals(Distributions.NONE, t.distribution());
      assertEquals(SortOrders.NONE.length, t.sortOrder().length);
    } finally {
      cleanup(schema, table);
    }
  }

  @Test
  void testMinimalTableNoStorageDescriptor() {
    String schema = uniqueName("s");
    String table = uniqueName("minimal_tbl");
    Table glueTable = provideMinimalTable(schema, table);
    try {
      GlueTable t = GlueTable.fromGlueTable(glueTable, TYPE_CONVERTER);
      assertEquals(0, t.columns().length);
      assertEquals(0, t.partitioning().length);
      assertEquals(Distributions.NONE, t.distribution());
      assertTrue(t.properties().isEmpty() || !t.properties().containsKey(GlueConstants.LOCATION));
    } finally {
      cleanup(schema, table);
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  protected String uniqueName(String base) {
    return base + "_" + System.currentTimeMillis();
  }
}
