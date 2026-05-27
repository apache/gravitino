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
package org.apache.gravitino.trino.connector.catalog.glue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.catalog.hive.SortingColumn;
import org.apache.gravitino.trino.connector.metadata.GravitinoTable;
import org.junit.jupiter.api.Test;

class TestGlueMetadataAdapter {

  private final GlueMetadataAdapter adapter = new GlueMetadataAdapter(ImmutableList.of());

  @Test
  void testCreateTableKeepsPartitionBucketAndSortAsStructuredFields() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("type", "HIVE");
    properties.put("format", "PARQUET");
    properties.put("location", "s3://bucket/table");
    properties.put("partitioned_by", ImmutableList.of("part_col"));
    properties.put("bucketed_by", ImmutableList.of("bucket_col"));
    properties.put("bucket_count", 8);
    properties.put(
        "sorted_by", ImmutableList.of(SortingColumn.sortingColumnFromString("bucket_col DESC")));

    ConnectorTableMetadata tableMetadata =
        new ConnectorTableMetadata(
            new SchemaTableName("db", "tbl"),
            ImmutableList.of(
                ColumnMetadata.builder().setName("id").setType(BigintType.BIGINT).build(),
                ColumnMetadata.builder().setName("part_col").setType(BigintType.BIGINT).build(),
                ColumnMetadata.builder().setName("bucket_col").setType(BigintType.BIGINT).build()),
            properties);

    GravitinoTable table = adapter.createTable(tableMetadata);

    assertEquals("HIVE", table.getProperties().get("table-format"));
    assertEquals("PARQUET", table.getProperties().get("format"));
    assertEquals("s3://bucket/table", table.getProperties().get("location"));
    assertFalse(table.getProperties().containsKey("partitioned_by"));
    assertFalse(table.getProperties().containsKey("bucketed_by"));
    assertFalse(table.getProperties().containsKey("bucket_count"));
    assertFalse(table.getProperties().containsKey("sorted_by"));

    assertEquals(1, table.getPartitioning().length);
    assertArrayEquals(
        new String[] {"part_col"},
        ((Transform.SingleFieldTransform) table.getPartitioning()[0]).fieldName());
    assertEquals(Strategy.HASH, table.getDistribution().strategy());
    assertEquals(8, table.getDistribution().number());
    assertArrayEquals(
        new String[] {"bucket_col"},
        ((NamedReference) table.getDistribution().expressions()[0]).fieldName());
    assertEquals(1, table.getSortOrders().length);
    assertEquals(SortDirection.DESCENDING, table.getSortOrders()[0].direction());
    assertArrayEquals(
        new String[] {"bucket_col"},
        ((NamedReference) table.getSortOrders()[0].expression()).fieldName());
  }

  @Test
  void testGetTableMetadataRestoresStructuredPartitionBucketAndSortProperties() {
    Map<String, String> properties =
        Map.of("table-format", "HIVE", "format", "PARQUET", "location", "s3://bucket/table");
    Table mockTable =
        mockTable(
            "tbl",
            new Column[] {
              Column.of("id", Types.LongType.get()),
              Column.of("part_col", Types.LongType.get()),
              Column.of("bucket_col", Types.LongType.get())
            },
            properties,
            new Transform[] {Transforms.identity("part_col")},
            new SortOrder[] {SortOrders.descending(NamedReference.field("bucket_col"))},
            Distributions.of(Strategy.HASH, 8, NamedReference.field("bucket_col")));

    ConnectorTableMetadata tableMetadata =
        adapter.getTableMetadata(new GravitinoTable("db", "tbl", mockTable));
    Map<String, Object> trinoProperties = tableMetadata.getProperties();

    assertEquals("HIVE", trinoProperties.get("type"));
    assertEquals("PARQUET", trinoProperties.get("format"));
    assertEquals("s3://bucket/table", trinoProperties.get("location"));
    assertEquals(ImmutableList.of("part_col"), trinoProperties.get("partitioned_by"));
    assertEquals(ImmutableList.of("bucket_col"), trinoProperties.get("bucketed_by"));
    assertEquals(8, trinoProperties.get("bucket_count"));
    assertEquals(
        ImmutableList.of(SortingColumn.sortingColumnFromString("bucket_col DESC")),
        trinoProperties.get("sorted_by"));
  }

  @Test
  void testCreateTableParsesYearPartitionExpression() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("type", "ICEBERG");
    properties.put("format", "PARQUET");
    properties.put("location", "s3://bucket/table");
    properties.put("partitioned_by", ImmutableList.of("year(event_time)"));

    ConnectorTableMetadata tableMetadata =
        new ConnectorTableMetadata(
            new SchemaTableName("db", "tbl"),
            ImmutableList.of(
                ColumnMetadata.builder().setName("id").setType(BigintType.BIGINT).build(),
                ColumnMetadata.builder()
                    .setName("event_time")
                    .setType(TimestampType.TIMESTAMP_MILLIS)
                    .build()),
            properties);

    GravitinoTable table = adapter.createTable(tableMetadata);

    assertEquals(1, table.getPartitioning().length);
    assertEquals("year", table.getPartitioning()[0].name());
    assertArrayEquals(
        new String[] {"event_time"},
        ((Transform.SingleFieldTransform) table.getPartitioning()[0]).fieldName());
  }

  @Test
  void testCreateTableParsesBucketPartitionExpression() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("type", "ICEBERG");
    properties.put("format", "PARQUET");
    properties.put("location", "s3://bucket/table");
    properties.put("partitioned_by", ImmutableList.of("bucket(user_id, 4)"));

    ConnectorTableMetadata tableMetadata =
        new ConnectorTableMetadata(
            new SchemaTableName("db", "tbl"),
            ImmutableList.of(
                ColumnMetadata.builder().setName("id").setType(BigintType.BIGINT).build(),
                ColumnMetadata.builder().setName("user_id").setType(VarcharType.VARCHAR).build()),
            properties);

    GravitinoTable table = adapter.createTable(tableMetadata);

    assertEquals(1, table.getPartitioning().length);
    assertEquals("bucket", table.getPartitioning()[0].name());
    Transforms.BucketTransform bucket = (Transforms.BucketTransform) table.getPartitioning()[0];
    assertEquals(4, bucket.numBuckets());
    assertArrayEquals(new String[] {"user_id"}, bucket.fieldNames()[0]);
  }

  @Test
  void testCreateTableParsesMixedPartitionExpressions() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("type", "ICEBERG");
    properties.put("format", "PARQUET");
    properties.put("location", "s3://bucket/table");
    properties.put(
        "partitioned_by", ImmutableList.of("year(event_time)", "bucket(user_id, 8)", "category"));

    ConnectorTableMetadata tableMetadata =
        new ConnectorTableMetadata(
            new SchemaTableName("db", "tbl"),
            ImmutableList.of(
                ColumnMetadata.builder().setName("id").setType(BigintType.BIGINT).build(),
                ColumnMetadata.builder()
                    .setName("event_time")
                    .setType(TimestampType.TIMESTAMP_MILLIS)
                    .build(),
                ColumnMetadata.builder().setName("user_id").setType(VarcharType.VARCHAR).build(),
                ColumnMetadata.builder().setName("category").setType(VarcharType.VARCHAR).build()),
            properties);

    GravitinoTable table = adapter.createTable(tableMetadata);

    assertEquals(3, table.getPartitioning().length);
    assertEquals("year", table.getPartitioning()[0].name());
    assertEquals("bucket", table.getPartitioning()[1].name());
    assertEquals("identity", table.getPartitioning()[2].name());
  }

  @Test
  void testGetTableMetadataRestoresPartitionExpressions() {
    Table mockTable =
        mockTable(
            "tbl",
            new Column[] {
              Column.of("id", Types.LongType.get()),
              Column.of("event_time", Types.TimestampType.withoutTimeZone()),
              Column.of("user_id", Types.StringType.get())
            },
            Map.of("table-format", "ICEBERG", "format", "PARQUET", "location", "s3://bucket/t"),
            new Transform[] {
              Transforms.year("event_time"), Transforms.bucket(4, new String[] {"user_id"})
            },
            new SortOrder[] {},
            Distributions.of(Strategy.HASH, 0));

    ConnectorTableMetadata tableMetadata =
        adapter.getTableMetadata(new GravitinoTable("db", "tbl", mockTable));
    Map<String, Object> trinoProperties = tableMetadata.getProperties();

    @SuppressWarnings("unchecked")
    List<String> partitionedBy = (List<String>) trinoProperties.get("partitioned_by");
    assertEquals(ImmutableList.of("year(event_time)", "bucket(user_id, 4)"), partitionedBy);
  }

  private static Table mockTable(
      String tableName,
      Column[] columns,
      Map<String, String> properties,
      Transform[] partitioning,
      SortOrder[] sortOrder,
      Distribution distribution) {
    Table table = mock(Table.class);
    when(table.name()).thenReturn(tableName);
    when(table.columns()).thenReturn(columns);
    when(table.comment()).thenReturn("comment");
    when(table.properties()).thenReturn(properties);
    when(table.partitioning()).thenReturn(partitioning);
    when(table.sortOrder()).thenReturn(sortOrder);
    when(table.distribution()).thenReturn(distribution);
    when(table.index()).thenReturn(Indexes.EMPTY_INDEXES);

    Audit audit = mock(Audit.class);
    when(audit.creator()).thenReturn("gravitino");
    when(audit.createTime()).thenReturn(Instant.now());
    when(table.auditInfo()).thenReturn(audit);

    return table;
  }
}
