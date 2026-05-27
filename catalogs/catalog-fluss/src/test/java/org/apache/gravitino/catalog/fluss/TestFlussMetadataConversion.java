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

package org.apache.gravitino.catalog.fluss;

import static org.apache.gravitino.StringIdentifier.ID_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

class TestFlussMetadataConversion {

  @Test
  void testFlussSchemaConvertsDatabaseInfo() {
    DatabaseDescriptor descriptor =
        FlussSchema.toDatabaseDescriptor("database comment", Map.of(ID_KEY, "1", "k", "v"));
    DatabaseInfo databaseInfo = new DatabaseInfo("db", descriptor, 10L, 20L);

    FlussSchema schema = FlussSchema.fromDatabaseInfo(databaseInfo);

    assertEquals("db", schema.name());
    assertEquals("database comment", schema.comment());
    assertEquals("v", schema.properties().get("k"));
    assertEquals(Instant.ofEpochMilli(10L), schema.auditInfo().createTime());
    assertEquals(Instant.ofEpochMilli(20L), schema.auditInfo().lastModifiedTime());
    assertEquals(Map.of("k", "v"), descriptor.getCustomProperties());
    assertFalse(descriptor.getCustomProperties().containsKey(ID_KEY));
  }

  @Test
  void testFlussColumnConvertsFromFlussColumn() {
    Schema.Column flussColumn = new Schema.Column("site_id", DataTypes.INT().copy(false), "site");

    Column column = FlussColumn.fromFlussColumn(flussColumn);

    assertEquals("site_id", column.name());
    assertEquals(Types.IntegerType.get(), column.dataType());
    assertEquals("site", column.comment());
    assertFalse(column.nullable());
    assertEquals(Column.DEFAULT_VALUE_NOT_SET, column.defaultValue());
  }

  @Test
  void testFlussColumnConvertsToFlussColumn() {
    Schema.Column flussColumn =
        FlussColumn.toFlussColumn(
            Column.of(
                "site_id",
                Types.IntegerType.get(),
                "site",
                false,
                false,
                Column.DEFAULT_VALUE_NOT_SET));

    assertEquals("site_id", flussColumn.getName());
    assertEquals("site", flussColumn.getComment().orElse(null));
    assertFalse(flussColumn.getDataType().isNullable());
  }

  @Test
  void testToTableDescriptorWithMultiFieldPartitions() {
    Transform[] partitions = {Transforms.identity("event_day"), Transforms.identity("region")};

    TableDescriptor descriptor =
        FlussTable.toTableDescriptor(
            columns(), null, Map.of(), partitions, Distributions.NONE, null, Indexes.EMPTY_INDEXES);

    assertEquals("event_day", descriptor.getPartitionKeys().get(0));
    assertEquals("region", descriptor.getPartitionKeys().get(1));
  }

  @Test
  void testToTableDescriptorConvertsTableMetadata() {
    TableDescriptor descriptor =
        FlussTable.toTableDescriptor(
            columns(),
            "orders comment",
            Map.of(ID_KEY, "1", "table.log.ttl", "1d"),
            new Transform[] {Transforms.identity("event_day")},
            Distributions.hash(3, NamedReference.field("site_id")),
            SortOrders.NONE,
            new Index[] {Indexes.primary("pk", new String[][] {{"event_day"}, {"site_id"}})});

    assertEquals("orders comment", descriptor.getComment().orElse(null));
    assertEquals("1d", descriptor.getProperties().get("table.log.ttl"));
    assertEquals("event_day", descriptor.getPartitionKeys().get(0));
    assertEquals(List.of("site_id"), descriptor.getBucketKeys());
    assertTrue(descriptor.getSchema().getPrimaryKey().isPresent());
    assertEquals(
        List.of("event_day", "site_id"),
        descriptor.getSchema().getPrimaryKey().get().getColumnNames());
  }

  @Test
  void testToTableDescriptorRejectsNonIdentityPartitionTransform() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                FlussTable.toTableDescriptor(
                    columns(),
                    null,
                    Map.of(),
                    new Transform[] {Transforms.day("event_day")},
                    Distributions.NONE,
                    null,
                    Indexes.EMPTY_INDEXES));

    assertTrue(exception.getMessage().contains("identity partition transforms"));
  }

  @Test
  void testToTableDescriptorRejectsNestedPartitionTransform() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                FlussTable.toTableDescriptor(
                    columns(),
                    null,
                    Map.of(),
                    new Transform[] {Transforms.identity(new String[] {"nested", "field"})},
                    Distributions.NONE,
                    null,
                    Indexes.EMPTY_INDEXES));

    assertTrue(exception.getMessage().contains("top-level fields"));
  }

  @Test
  void testToTableDescriptorRejectsNonPrimaryKeyPartitionForPrimaryKeyTable() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                FlussTable.toTableDescriptor(
                    columns(),
                    null,
                    Map.of(),
                    new Transform[] {Transforms.identity("region")},
                    Distributions.NONE,
                    null,
                    new Index[] {
                      Indexes.primary("pk", new String[][] {{"event_day"}, {"site_id"}})
                    }));

    assertTrue(exception.getMessage().contains("subset of the primary key"));
  }

  @Test
  void testToTableDescriptorRejectsUnsupportedSortOrdersAndDistributions() {
    IllegalArgumentException sortOrderException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                FlussTable.toTableDescriptor(
                    columns(),
                    null,
                    Map.of(),
                    new Transform[0],
                    Distributions.NONE,
                    new org.apache.gravitino.rel.expressions.sorts.SortOrder[] {
                      SortOrders.ascending(NamedReference.field("event_day"))
                    },
                    Indexes.EMPTY_INDEXES));
    assertTrue(sortOrderException.getMessage().contains("sort orders"));

    IllegalArgumentException strategyException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                FlussTable.toTableDescriptor(
                    columns(),
                    null,
                    Map.of(),
                    new Transform[0],
                    Distributions.even(3, NamedReference.field("region")),
                    null,
                    Indexes.EMPTY_INDEXES));
    assertTrue(strategyException.getMessage().contains("HASH or NONE"));

    IllegalArgumentException expressionException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                FlussTable.toTableDescriptor(
                    columns(),
                    null,
                    Map.of(),
                    new Transform[0],
                    Distributions.hash(3, Literals.stringLiteral("region")),
                    null,
                    Indexes.EMPTY_INDEXES));
    assertTrue(expressionException.getMessage().contains("field references"));
  }

  @Test
  void testToTableDescriptorRejectsUnsupportedIndexes() {
    IllegalArgumentException nonPrimaryKeyException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                FlussTable.toTableDescriptor(
                    columns(),
                    null,
                    Map.of(),
                    new Transform[0],
                    Distributions.NONE,
                    null,
                    new Index[] {Indexes.unique("uk", new String[][] {{"site_id"}})}));
    assertTrue(nonPrimaryKeyException.getMessage().contains("primary key indexes"));

    IllegalArgumentException multiplePrimaryKeysException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                FlussTable.toTableDescriptor(
                    columns(),
                    null,
                    Map.of(),
                    new Transform[0],
                    Distributions.NONE,
                    null,
                    new Index[] {
                      Indexes.primary("pk1", new String[][] {{"site_id"}}),
                      Indexes.primary("pk2", new String[][] {{"event_day"}})
                    }));
    assertTrue(multiplePrimaryKeysException.getMessage().contains("one primary key"));
  }

  @Test
  void testToGravitinoTableReportsIdentityPartitioning() {
    Transform[] partitions = {Transforms.identity("event_day"), Transforms.identity("region")};
    TableDescriptor descriptor =
        FlussTable.toTableDescriptor(
            columns(),
            "orders comment",
            Map.of("table.log.ttl", "1d"),
            partitions,
            Distributions.hash(3),
            null,
            Indexes.EMPTY_INDEXES);
    TableInfo tableInfo = TableInfo.of(TablePath.of("db", "orders"), 1L, 1, descriptor, 10L, 20L);

    Table table = FlussTable.fromTableInfo(tableInfo);

    assertArrayEquals(partitions, table.partitioning());
    assertEquals("orders", table.name());
    assertEquals("orders comment", table.comment());
    assertEquals("1d", table.properties().get("table.log.ttl"));
    assertEquals(Strategy.HASH, table.distribution().strategy());
    assertEquals(3, table.distribution().number());
    assertEquals(0, table.distribution().expressions().length);
    assertEquals(Instant.ofEpochMilli(10L), table.auditInfo().createTime());
    assertEquals(Instant.ofEpochMilli(20L), table.auditInfo().lastModifiedTime());
  }

  @Test
  void testToGravitinoTableReportsPrimaryKeyIndexAndUnknownId() {
    TableDescriptor descriptor =
        FlussTable.toTableDescriptor(
            columns(),
            null,
            Map.of(),
            new Transform[0],
            Distributions.hash(1, NamedReference.field("site_id")),
            null,
            new Index[] {Indexes.primary("pk", new String[][] {{"event_day"}, {"site_id"}})});
    TableInfo tableInfo =
        TableInfo.of(
            TablePath.of("db", "orders"), TableInfo.UNKNOWN_TABLE_ID, 1, descriptor, 10L, 20L);

    Table table = FlussTable.fromTableInfo(tableInfo);

    assertEquals(1, table.index().length);
    assertEquals("pk", table.index()[0].name());
    assertTrue(table.properties().containsKey(ID_KEY));
  }

  private static Column[] columns() {
    return new Column[] {
      Column.of("event_day", Types.StringType.get(), "event day", false, false, null),
      Column.of("region", Types.StringType.get(), "region", false, false, null),
      Column.of("site_id", Types.IntegerType.get(), "site", false, false, null),
      Column.of("pv", Types.LongType.get())
    };
  }
}
