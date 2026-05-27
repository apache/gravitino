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

import static org.apache.gravitino.catalog.glue.GlueIcebergTableHelper.fromIcebergType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.junit.jupiter.api.Test;

class TestGlueIcebergTableHelper {

  // -------------------------------------------------------------------------
  // fromIcebergType — primitive types
  // -------------------------------------------------------------------------

  @Test
  void testFromIcebergTypePrimitives() {
    assertEquals(Types.BooleanType.get(), fromIcebergType(BooleanType.get()));
    assertEquals(Types.IntegerType.get(), fromIcebergType(IntegerType.get()));
    assertEquals(Types.LongType.get(), fromIcebergType(LongType.get()));
    assertEquals(Types.FloatType.get(), fromIcebergType(FloatType.get()));
    assertEquals(Types.DoubleType.get(), fromIcebergType(DoubleType.get()));
    assertEquals(Types.StringType.get(), fromIcebergType(StringType.get()));
    assertEquals(Types.DateType.get(), fromIcebergType(DateType.get()));
  }

  @Test
  void testFromIcebergTypeTime() {
    assertEquals(Types.TimeType.of(6), fromIcebergType(TimeType.get()));
  }

  @Test
  void testFromIcebergTypeTimestamp() {
    assertEquals(
        Types.TimestampType.withoutTimeZone(6), fromIcebergType(TimestampType.withoutZone()));
    assertEquals(Types.TimestampType.withTimeZone(6), fromIcebergType(TimestampType.withZone()));
  }

  @Test
  void testFromIcebergTypeDecimal() {
    assertEquals(Types.DecimalType.of(10, 2), fromIcebergType(DecimalType.of(10, 2)));
    assertEquals(Types.DecimalType.of(38, 10), fromIcebergType(DecimalType.of(38, 10)));
  }

  @Test
  void testFromIcebergTypeFixed() {
    assertEquals(Types.FixedType.of(16), fromIcebergType(FixedType.ofLength(16)));
  }

  // -------------------------------------------------------------------------
  // fromIcebergType — nested types
  // -------------------------------------------------------------------------

  @Test
  void testFromIcebergTypeList() {
    org.apache.gravitino.rel.types.Type result =
        fromIcebergType(ListType.ofOptional(1, StringType.get()));
    assertEquals(Types.ListType.of(Types.StringType.get(), true), result);

    result = fromIcebergType(ListType.ofRequired(1, IntegerType.get()));
    assertEquals(Types.ListType.of(Types.IntegerType.get(), false), result);
  }

  @Test
  void testFromIcebergTypeMap() {
    org.apache.gravitino.rel.types.Type result =
        fromIcebergType(MapType.ofOptional(1, 2, StringType.get(), LongType.get()));
    assertEquals(Types.MapType.of(Types.StringType.get(), Types.LongType.get(), true), result);
  }

  @Test
  void testFromIcebergTypeStruct() {
    StructType icebergStruct =
        StructType.of(
            NestedField.optional(1, "name", StringType.get(), "the name"),
            NestedField.required(2, "age", IntegerType.get()));
    org.apache.gravitino.rel.types.Type result = fromIcebergType(icebergStruct);

    Types.StructType structType = (Types.StructType) result;
    assertEquals(2, structType.fields().length);
    assertEquals("name", structType.fields()[0].name());
    assertEquals(Types.StringType.get(), structType.fields()[0].type());
    assertEquals(true, structType.fields()[0].nullable());
    assertEquals("age", structType.fields()[1].name());
    assertEquals(Types.IntegerType.get(), structType.fields()[1].type());
    assertEquals(false, structType.fields()[1].nullable());
  }

  // -------------------------------------------------------------------------
  // loadTable — column type overwrite
  // -------------------------------------------------------------------------

  @Test
  void testLoadTableOverwritesColumnTypesFromIcebergSchema() {
    // Build an Iceberg schema with TIME and TIMESTAMP columns.
    Schema icebergSchema =
        new Schema(
            NestedField.optional(1, "ts", TimestampType.withZone()),
            NestedField.optional(2, "t", TimeType.get()),
            NestedField.required(3, "score", DecimalType.of(10, 2)));

    Table mockIcebergTable = mock(Table.class);
    when(mockIcebergTable.schema()).thenReturn(icebergSchema);
    when(mockIcebergTable.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(mockIcebergTable.sortOrder()).thenReturn(SortOrder.unsorted());
    when(mockIcebergTable.properties()).thenReturn(new HashMap<>());

    Catalog mockCatalog = mock(Catalog.class);
    when(mockCatalog.loadTable(any(TableIdentifier.class))).thenReturn(mockIcebergTable);

    // GlueTable with wrong Hive-style column types (as Glue would store them).
    GlueColumn[] glueCols = {
      GlueColumn.builder().withName("ts").withType(Types.StringType.get()).build(),
      GlueColumn.builder().withName("t").withType(Types.StringType.get()).build(),
      GlueColumn.builder().withName("score").withType(Types.StringType.get()).build()
    };
    GlueTable table =
        GlueTable.builder()
            .withName("t1")
            .withColumns(glueCols)
            .withProperties(new HashMap<>())
            .withAuditInfo(AuditInfo.EMPTY)
            .build();

    GlueIcebergTableHelper.loadTable(mockCatalog, "db1", "t1", table);

    Column[] cols = table.columns();
    assertEquals(3, cols.length);
    assertEquals(Types.TimestampType.withTimeZone(6), cols[0].dataType());
    assertEquals(Types.TimeType.of(6), cols[1].dataType());
    assertEquals(Types.DecimalType.of(10, 2), cols[2].dataType());
  }
}
