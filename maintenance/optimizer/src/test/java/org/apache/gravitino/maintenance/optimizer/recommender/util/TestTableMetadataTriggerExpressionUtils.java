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

package org.apache.gravitino.maintenance.optimizer.recommender.util;

import java.util.Map;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestTableMetadataTriggerExpressionUtils {

  @Test
  void testBuildTableMetadataContextWithAllFields() {
    Table tableMetadata = Mockito.mock(Table.class);
    Mockito.when(tableMetadata.columns()).thenReturn(new Column[3]);
    Mockito.when(tableMetadata.partitioning()).thenReturn(new Transform[1]);
    Mockito.when(tableMetadata.sortOrder()).thenReturn(new SortOrder[1]);
    Mockito.when(tableMetadata.properties())
        .thenReturn(Map.of("format", "iceberg", "max_file_size", "1073741824"));

    Map<String, Object> context =
        TableMetadataTriggerExpressionUtils.buildTableMetadataContext(tableMetadata);

    Assertions.assertEquals(5, context.size());
    Assertions.assertEquals(3, context.get("column_count"));
    Assertions.assertEquals(1, context.get("partition_count"));
    Assertions.assertEquals(1, context.get("sort_order_count"));
    Assertions.assertEquals("iceberg", context.get("format"));
    Assertions.assertEquals(1073741824L, context.get("max_file_size"));
  }

  @Test
  void testBuildTableMetadataContextWithEmptyTableMetadata() {
    Table tableMetadata = Mockito.mock(Table.class);
    Mockito.when(tableMetadata.columns()).thenReturn(null);
    Mockito.when(tableMetadata.partitioning()).thenReturn(null);
    Mockito.when(tableMetadata.sortOrder()).thenReturn(null);
    Mockito.when(tableMetadata.properties()).thenReturn(Map.of());

    Map<String, Object> context =
        TableMetadataTriggerExpressionUtils.buildTableMetadataContext(tableMetadata);

    Assertions.assertEquals(3, context.size());
    Assertions.assertEquals(0, context.get("column_count"));
    Assertions.assertEquals(0, context.get("partition_count"));
    Assertions.assertEquals(0, context.get("sort_order_count"));
  }

  @Test
  void testBuildTableMetadataContextWithNullProperties() {
    Table tableMetadata = Mockito.mock(Table.class);
    Mockito.when(tableMetadata.columns()).thenReturn(new Column[1]);
    Mockito.when(tableMetadata.partitioning()).thenReturn(new Transform[0]);
    Mockito.when(tableMetadata.sortOrder()).thenReturn(new SortOrder[0]);
    Mockito.when(tableMetadata.properties()).thenReturn(null);

    Map<String, Object> context =
        TableMetadataTriggerExpressionUtils.buildTableMetadataContext(tableMetadata);

    Assertions.assertEquals(3, context.size());
    Assertions.assertEquals(1, context.get("column_count"));
    Assertions.assertEquals(0, context.get("partition_count"));
    Assertions.assertEquals(0, context.get("sort_order_count"));
  }

  @Test
  void testBuildTableMetadataContextParsesNumericProperties() {
    Table tableMetadata = Mockito.mock(Table.class);
    Mockito.when(tableMetadata.columns()).thenReturn(new Column[1]);
    Mockito.when(tableMetadata.partitioning()).thenReturn(new Transform[0]);
    Mockito.when(tableMetadata.sortOrder()).thenReturn(new SortOrder[0]);
    Mockito.when(tableMetadata.properties())
        .thenReturn(
            Map.of(
                "count", "12345",
                "size", "9876543210",
                "name", "test_table",
                "enabled", "true"));

    Map<String, Object> context =
        TableMetadataTriggerExpressionUtils.buildTableMetadataContext(tableMetadata);

    Assertions.assertEquals(12345L, context.get("count"));
    Assertions.assertEquals(9876543210L, context.get("size"));
    Assertions.assertEquals("test_table", context.get("name"));
    Assertions.assertEquals("true", context.get("enabled"));
  }

  @Test
  void testBuildTableMetadataContextFailsWhenTableMetadataIsNull() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> TableMetadataTriggerExpressionUtils.buildTableMetadataContext(null));
    Assertions.assertTrue(exception.getMessage().contains("Table metadata is null"));
  }
}
