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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.hive.HiveStorageConstants;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.Order;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.SkewedInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.ThrottlingException;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

class TestGlueCatalogTableOperations {

  private GlueCatalogOperations ops;
  private GlueClient mockClient;

  @BeforeEach
  void setup() {
    mockClient = mock(GlueClient.class);
    ops = new GlueCatalogOperations();
    ops.glueClient = mockClient;
    ops.warehouseLocation = "s3://test-bucket/warehouse";
    stubDatabaseLocation(null);
    useFakeGlueTable(null);
  }

  /**
   * Makes the mock behave like Glue for one table: creates and updates are visible to later reads,
   * which the catalog uses to build its responses.
   */
  private void useFakeGlueTable(Table initial) {
    AtomicReference<Table> stored = new AtomicReference<>(initial);
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenAnswer(
            inv -> {
              if (stored.get() == null) {
                throw EntityNotFoundException.builder().message("not found").build();
              }
              return GetTableResponse.builder().table(stored.get()).build();
            });
    when(mockClient.createTable(any(CreateTableRequest.class)))
        .thenAnswer(
            inv -> {
              stored.set(toTable(((CreateTableRequest) inv.getArgument(0)).tableInput()));
              return CreateTableResponse.builder().build();
            });
    when(mockClient.updateTable(any(UpdateTableRequest.class)))
        .thenAnswer(
            inv -> {
              stored.set(toTable(((UpdateTableRequest) inv.getArgument(0)).tableInput()));
              return UpdateTableResponse.builder().build();
            });
  }

  private static Table toTable(TableInput in) {
    return Table.builder()
        .name(in.name())
        .description(in.description())
        .owner(in.owner())
        .retention(in.retention())
        .storageDescriptor(in.storageDescriptor())
        .partitionKeys(in.partitionKeys())
        .tableType(in.tableType())
        .parameters(in.parameters())
        .createTime(Instant.now())
        .build();
  }

  /** Stubs the database lookup used to resolve table locations. */
  private void stubDatabaseLocation(String locationUri) {
    Database database = Database.builder().name("mydb").locationUri(locationUri).build();
    when(mockClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(GetDatabaseResponse.builder().database(database).build());
  }

  // -------------------------------------------------------------------------
  // listTables
  // -------------------------------------------------------------------------

  @Test
  void testListTablesPaginated() {
    Namespace ns = Namespace.of("metalake", "catalog", "mydb");
    Table t1 = Table.builder().name("t1").build();
    Table t2 = Table.builder().name("t2").build();
    Table t3 = Table.builder().name("t3").build();

    when(mockClient.getTables(any(GetTablesRequest.class)))
        .thenReturn(GetTablesResponse.builder().tableList(t1, t2).nextToken("tok").build())
        .thenReturn(GetTablesResponse.builder().tableList(t3).nextToken(null).build());

    NameIdentifier[] result = ops.listTables(ns);

    assertEquals(3, result.length);
    assertEquals("t1", result[0].name());
    assertEquals("t3", result[2].name());
  }

  @Test
  void testListTablesSchemaNotFound() {
    Namespace ns = Namespace.of("metalake", "catalog", "missing");
    when(mockClient.getTables(any(GetTablesRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertThrows(NoSuchSchemaException.class, () -> ops.listTables(ns));
  }

  @Test
  void testListTablesFormatFilter() {
    ops.tableFormatFilter = Set.of("iceberg");
    Namespace ns = Namespace.of("metalake", "catalog", "mydb");

    Table icebergTable =
        Table.builder()
            .name("ice_tbl")
            .parameters(Map.of(GlueConstants.TABLE_TYPE_PARAM, "ICEBERG"))
            .build();
    Table hiveTable = Table.builder().name("hive_tbl").parameters(Collections.emptyMap()).build();

    when(mockClient.getTables(any(GetTablesRequest.class)))
        .thenReturn(
            GetTablesResponse.builder().tableList(icebergTable, hiveTable).nextToken(null).build());

    NameIdentifier[] result = ops.listTables(ns);

    assertEquals(1, result.length);
    assertEquals("ice_tbl", result[0].name());
  }

  @Test
  void testFormatFilterHidesTableFromLoadAlterAndDrop() {
    ops.tableFormatFilter = Set.of("iceberg");
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "hive_tbl");
    Table hiveTable =
        Table.builder()
            .name("hive_tbl")
            .parameters(Collections.emptyMap())
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(hiveTable).build());

    assertThrows(NoSuchTableException.class, () -> ops.loadTable(ident));
    assertThrows(
        NoSuchTableException.class, () -> ops.alterTable(ident, TableChange.setProperty("k", "v")));
    assertFalse(ops.dropTable(ident));
    verify(mockClient, never()).updateTable(any(UpdateTableRequest.class));
    verify(mockClient, never()).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  void testFormatFilterRefusesCreateOfHiddenFormat() {
    ops.tableFormatFilter = Set.of("iceberg");
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "hive_tbl");
    Column[] columns = {
      GlueColumn.builder().withName("id").withType(Types.LongType.get()).withNullable(true).build()
    };

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ops.createTable(
                    ident,
                    columns,
                    null,
                    Collections.emptyMap(),
                    Transforms.EMPTY_TRANSFORM,
                    Distributions.NONE,
                    SortOrders.NONE,
                    Indexes.EMPTY_INDEXES));
    assertTrue(ex.getMessage().contains(GlueConstants.TABLE_FORMAT_FILTER), ex.getMessage());
    verify(mockClient, never()).createTable(any(CreateTableRequest.class));
  }

  @Test
  void testFormatFilterKeepsVisibleFormatUsable() {
    ops.tableFormatFilter = Set.of("hive");
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "hive_tbl");
    // table-format is not stored: a Glue parameter left by an older version does not change the
    // format, which comes from table_type.
    Table hiveTable =
        Table.builder()
            .name("hive_tbl")
            .parameters(Map.of(GlueConstants.TABLE_FORMAT, "ICEBERG"))
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(hiveTable).build());

    GlueTable loaded = ops.loadTable(ident);
    assertEquals("hive_tbl", loaded.name());
    assertEquals("HIVE", loaded.properties().get(GlueConstants.TABLE_FORMAT));
  }

  @Test
  void testCreateRefusesUnknownTableFormatAndDoesNotStoreIt() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.createTable(
                NameIdentifier.of("metalake", "catalog", "mydb", "t"),
                new Column[] {
                  GlueColumn.builder().withName("id").withType(Types.LongType.get()).build()
                },
                null,
                Map.of(GlueConstants.TABLE_FORMAT, "delta"),
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                SortOrders.NONE,
                Indexes.EMPTY_INDEXES));

    useFakeGlueTable(null);
    ops.createTable(
        NameIdentifier.of("metalake", "catalog", "mydb", "t"),
        new Column[] {GlueColumn.builder().withName("id").withType(Types.LongType.get()).build()},
        null,
        Map.of(GlueConstants.TABLE_FORMAT, "hive"),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);
    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(mockClient).createTable(captor.capture());
    assertFalse(
        captor.getValue().tableInput().parameters().containsKey(GlueConstants.TABLE_FORMAT));
  }

  @Test
  void testFormatFilterCreateUsesDefaultTableFormat() {
    ops.tableFormatFilter = Set.of("hive");
    ops.defaultTableFormat = "iceberg";
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "hive_tbl");
    Column[] columns = {
      GlueColumn.builder().withName("id").withType(Types.LongType.get()).withNullable(true).build()
    };

    // Without table-format the default (iceberg) applies, which the filter hides.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.createTable(
                ident,
                columns,
                null,
                Collections.emptyMap(),
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                SortOrders.NONE,
                Indexes.EMPTY_INDEXES));

    ops.createTable(
        ident,
        columns,
        null,
        Map.of(GlueConstants.TABLE_FORMAT, "hive", GlueConstants.LOCATION, "s3://bucket/path"),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);
    verify(mockClient).createTable(any(CreateTableRequest.class));
  }

  @Test
  void testFormatFilterAllExposesEveryFormat() {
    ops.tableFormatFilter = Set.of("all", "iceberg");
    Namespace ns = Namespace.of("metalake", "catalog", "mydb");
    Table hiveTable = Table.builder().name("hive_tbl").parameters(Collections.emptyMap()).build();
    when(mockClient.getTables(any(GetTablesRequest.class)))
        .thenReturn(GetTablesResponse.builder().tableList(hiveTable).nextToken(null).build());

    assertEquals(1, ops.listTables(ns).length);
  }

  // -------------------------------------------------------------------------
  // loadTable
  // -------------------------------------------------------------------------

  @Test
  void testLoadTableSuccess() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "mytable");
    Table glueTable =
        Table.builder()
            .name("mytable")
            .description("desc")
            .storageDescriptor(
                StorageDescriptor.builder()
                    .columns(
                        software.amazon.awssdk.services.glue.model.Column.builder()
                            .name("id")
                            .type("bigint")
                            .build())
                    .build())
            .createTime(Instant.now())
            .build();
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(glueTable).build());

    GlueTable result = ops.loadTable(ident);

    assertEquals("mytable", result.name());
    assertEquals("desc", result.comment());
    assertEquals(1, result.columns().length);
    assertEquals("id", result.columns()[0].name());
  }

  @Test
  void testLoadTableNotFound() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "missing");
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertThrows(NoSuchTableException.class, () -> ops.loadTable(ident));
  }

  // -------------------------------------------------------------------------
  // createTable
  // -------------------------------------------------------------------------

  @Test
  void testCreateTableSuccess() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "mytable");
    Column[] columns = {
      GlueColumn.builder().withName("id").withType(Types.LongType.get()).withNullable(true).build(),
      GlueColumn.builder()
          .withName("name")
          .withType(Types.StringType.get())
          .withNullable(true)
          .build()
    };

    GlueTable result =
        ops.createTable(
            ident,
            columns,
            "my comment",
            Map.of(GlueConstants.LOCATION, "s3://bucket/path", GlueConstants.FORMAT, "parquet"),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            SortOrders.NONE,
            Indexes.EMPTY_INDEXES);

    verify(mockClient).createTable(any(CreateTableRequest.class));
    assertEquals("mytable", result.name());
    assertEquals("my comment", result.comment());
    assertEquals(2, result.columns().length);
  }

  @Test
  void testCreateTableAlreadyExists() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "mytable");
    when(mockClient.createTable(any(CreateTableRequest.class)))
        .thenThrow(AlreadyExistsException.builder().message("exists").build());

    assertThrows(
        TableAlreadyExistsException.class,
        () ->
            ops.createTable(
                ident,
                new Column[0],
                null,
                Map.of(GlueConstants.FORMAT, "parquet"),
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                SortOrders.NONE,
                Indexes.EMPTY_INDEXES));
  }

  @Test
  void testCreateTableIndexesRejected() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "mytable");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.createTable(
                ident,
                new Column[0],
                null,
                Collections.emptyMap(),
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                SortOrders.NONE,
                new Index[] {mock(Index.class)}));
  }

  @Test
  void testCreateTableStorageDescriptorProperties() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "mytable");
    Map<String, String> props =
        Map.of(
            GlueConstants.LOCATION,
            "s3://my-bucket/path",
            GlueConstants.INPUT_FORMAT_CLASS,
            HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS,
            GlueConstants.OUTPUT_FORMAT,
            HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS,
            GlueConstants.SERDE_LIB,
            HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS);

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);

    ops.createTable(
        ident,
        new Column[0],
        "comment",
        props,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    verify(mockClient).createTable(captor.capture());
    CreateTableRequest req = captor.getValue();
    assertEquals("EXTERNAL_TABLE", req.tableInput().tableType());
    assertEquals("s3://my-bucket/path", req.tableInput().storageDescriptor().location());
    assertFalse(req.tableInput().parameters().containsKey(GlueConstants.LOCATION));
  }

  @Test
  void testCreateTableLocationFromDatabaseLocationUri() {
    stubDatabaseLocation("s3://test-bucket/gravprobe");
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "mytable");

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);

    ops.createTable(
        ident,
        new Column[0],
        "comment",
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    verify(mockClient).createTable(captor.capture());
    assertEquals(
        "s3://test-bucket/gravprobe/mytable",
        captor.getValue().tableInput().storageDescriptor().location());
  }

  @Test
  void testCreateTableLocationFromDatabaseLocationUriWithTrailingSlashes() {
    stubDatabaseLocation("s3://test-bucket/gravprobe///");
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "mytable");

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);

    ops.createTable(
        ident,
        new Column[0],
        "comment",
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    verify(mockClient).createTable(captor.capture());
    assertEquals(
        "s3://test-bucket/gravprobe/mytable",
        captor.getValue().tableInput().storageDescriptor().location());
  }

  @Test
  void testCreateTableLocationWhenWarehouseEqualsDatabaseLocation() {
    ops.warehouseLocation = "s3://test-bucket/gravprobe";
    stubDatabaseLocation("s3://test-bucket/gravprobe");
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "mytable");

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);

    ops.createTable(
        ident,
        new Column[0],
        "comment",
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    verify(mockClient).createTable(captor.capture());
    assertEquals(
        "s3://test-bucket/gravprobe/mytable",
        captor.getValue().tableInput().storageDescriptor().location());
  }

  @Test
  void testCreateTableLocationFallsBackToWarehouse() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "mytable");

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);

    ops.createTable(
        ident,
        new Column[0],
        "comment",
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    verify(mockClient).createTable(captor.capture());
    assertEquals(
        "s3://test-bucket/warehouse/mydb/mytable",
        captor.getValue().tableInput().storageDescriptor().location());
  }

  @Test
  void testCreateTableLocationFromWarehouseWithTrailingSlashes() {
    ops.warehouseLocation = "s3://test-bucket/warehouse///";
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "mytable");

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);

    ops.createTable(
        ident,
        new Column[0],
        "comment",
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    verify(mockClient).createTable(captor.capture());
    assertEquals(
        "s3://test-bucket/warehouse/mydb/mytable",
        captor.getValue().tableInput().storageDescriptor().location());
  }

  @Test
  void testCreateTableExplicitLocationWinsOverDatabaseLocationUri() {
    stubDatabaseLocation("s3://test-bucket/gravprobe");
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "mytable");

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);

    ops.createTable(
        ident,
        new Column[0],
        "comment",
        Map.of(GlueConstants.LOCATION, "s3://my-bucket/path"),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    verify(mockClient).createTable(captor.capture());
    assertEquals(
        "s3://my-bucket/path", captor.getValue().tableInput().storageDescriptor().location());
  }

  @Test
  void testCreateTableSchemaNotFoundWhileResolvingLocation() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "missing", "mytable");
    when(mockClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertThrows(
        NoSuchSchemaException.class,
        () ->
            ops.createTable(
                ident,
                new Column[0],
                "comment",
                Collections.emptyMap(),
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                SortOrders.NONE,
                Indexes.EMPTY_INDEXES));
  }

  // -------------------------------------------------------------------------
  // alterTable
  // -------------------------------------------------------------------------

  @Test
  void testAlterTableRenameAndComment() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "old");
    Table glueTable =
        Table.builder()
            .name("old")
            .description("old comment")
            .parameters(Map.of(GlueConstants.FORMAT, "parquet"))
            .storageDescriptor(StorageDescriptor.builder().build())
            .createTime(Instant.now())
            .build();
    useFakeGlueTable(glueTable);

    GlueTable result = ops.alterTable(ident, TableChange.updateComment("new comment"));

    assertEquals("new comment", result.comment());
  }

  @Test
  void testAlterTableKeepsExistingLocation() {
    stubDatabaseLocation("s3://test-bucket/gravprobe");
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "t");
    Table glueTable =
        Table.builder()
            .name("t")
            .storageDescriptor(
                StorageDescriptor.builder().location("s3://other-bucket/existing/t").build())
            .createTime(Instant.now())
            .build();
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(glueTable).build());
    when(mockClient.updateTable(any(UpdateTableRequest.class)))
        .thenReturn(UpdateTableResponse.builder().build());

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    ops.alterTable(ident, TableChange.updateComment("new comment"));

    verify(mockClient).updateTable(captor.capture());
    assertEquals(
        "s3://other-bucket/existing/t",
        captor.getValue().tableInput().storageDescriptor().location());
    verify(mockClient, never()).getDatabase(any(GetDatabaseRequest.class));
  }

  @Test
  void testAlterTableSetProperty() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "t");
    Table glueTable =
        Table.builder()
            .name("t")
            .parameters(Map.of("existing", "v1", GlueConstants.FORMAT, "parquet"))
            .storageDescriptor(StorageDescriptor.builder().build())
            .createTime(Instant.now())
            .build();
    useFakeGlueTable(glueTable);

    GlueTable result = ops.alterTable(ident, TableChange.setProperty("newKey", "newVal"));

    assertEquals("newVal", result.properties().get("newKey"));
    assertEquals("v1", result.properties().get("existing"));
  }

  @Test
  void testAlterTableAddColumn() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "t");
    Table glueTable =
        Table.builder()
            .name("t")
            .parameters(Map.of(GlueConstants.FORMAT, "parquet"))
            .storageDescriptor(
                StorageDescriptor.builder()
                    .columns(
                        List.of(
                            software.amazon.awssdk.services.glue.model.Column.builder()
                                .name("id")
                                .type("bigint")
                                .build()))
                    .build())
            .createTime(Instant.now())
            .build();
    useFakeGlueTable(glueTable);

    GlueTable result =
        ops.alterTable(
            ident, TableChange.addColumn(new String[] {"email"}, Types.StringType.get()));

    assertEquals(2, result.columns().length);
    assertEquals("email", result.columns()[1].name());
  }

  @Test
  void testAlterTableNotFound() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "missing");
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertThrows(
        NoSuchTableException.class, () -> ops.alterTable(ident, TableChange.updateComment("x")));
  }

  // -------------------------------------------------------------------------
  // dropTable
  // -------------------------------------------------------------------------

  @Test
  void testDropTableSuccess() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "t");
    stubGetTable(Table.builder().name("t").tableType(GlueConstants.EXTERNAL_TABLE_TYPE).build());

    boolean result = ops.dropTable(ident);

    verify(mockClient).deleteTable(any(DeleteTableRequest.class));
    assertTrue(result);
  }

  @Test
  void testDropTableNotFound() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "missing");
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertFalse(ops.dropTable(ident));
  }

  /** The table is dropped by another client between the type check and the delete. */
  @Test
  void testDropTableVanishesBeforeDelete() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "missing");
    stubGetTable(
        Table.builder().name("missing").tableType(GlueConstants.EXTERNAL_TABLE_TYPE).build());
    when(mockClient.deleteTable(any(DeleteTableRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertFalse(ops.dropTable(ident));
  }

  @Test
  void testDropTableWithCatalogId() {
    ops.catalogId = "123456789012";
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "t");
    stubGetTable(Table.builder().name("t").tableType(GlueConstants.EXTERNAL_TABLE_TYPE).build());
    ArgumentCaptor<DeleteTableRequest> captor = ArgumentCaptor.forClass(DeleteTableRequest.class);

    ops.dropTable(ident);

    verify(mockClient).deleteTable(captor.capture());
    assertEquals("123456789012", captor.getValue().catalogId());
    assertEquals("mydb", captor.getValue().databaseName());
    assertEquals("t", captor.getValue().name());
  }

  // -------------------------------------------------------------------------
  // views are not tables
  // -------------------------------------------------------------------------

  @Test
  void testListTablesExcludesViews() {
    Namespace ns = Namespace.of("metalake", "catalog", "mydb");
    Table table = Table.builder().name("iceberg_test").build();

    when(mockClient.getTables(any(GetTablesRequest.class)))
        .thenReturn(
            GetTablesResponse.builder()
                .tableList(table, viewObject("iceberg_view"))
                .nextToken(null)
                .build());

    NameIdentifier[] result = ops.listTables(ns);

    assertEquals(1, result.length);
    assertEquals("iceberg_test", result[0].name());
  }

  @Test
  void testLoadTableRejectsView() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "iceberg_view");
    stubGetTable(viewObject("iceberg_view"));

    NoSuchTableException e = assertThrows(NoSuchTableException.class, () -> ops.loadTable(ident));
    assertTrue(e.getMessage().contains("it is a view, not a table"));
  }

  @Test
  void testAlterTableRejectsView() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "iceberg_view");
    stubGetTable(viewObject("iceberg_view"));

    assertThrows(
        NoSuchTableException.class, () -> ops.alterTable(ident, TableChange.updateComment("x")));
  }

  @Test
  void testDropTableRejectsView() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "iceberg_view");
    stubGetTable(viewObject("iceberg_view"));

    assertThrows(NoSuchTableException.class, () -> ops.dropTable(ident));
    verify(mockClient, never()).deleteTable(any(DeleteTableRequest.class));
  }

  /** Glue stores tableType as a free-form string; casing must not decide whether it is a view. */
  @Test
  void testViewDetectionIsCaseInsensitive() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "v");
    stubGetTable(Table.builder().name("v").tableType("virtual_view").build());

    assertThrows(NoSuchTableException.class, () -> ops.loadTable(ident));
  }

  private void stubGetTable(Table table) {
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(table).build());
  }

  private static Table viewObject(String name) {
    return Table.builder()
        .name(name)
        .tableType(GlueConstants.VIRTUAL_VIEW_TABLE_TYPE)
        .viewOriginalText("/* Presto View: abc */")
        .parameters(Map.of("presto_view", "true"))
        .build();
  }

  // -------------------------------------------------------------------------
  // Hive-format create validation
  // -------------------------------------------------------------------------

  private static final NameIdentifier HIVE_IDENT =
      NameIdentifier.of("metalake", "catalog", "mydb", "hive_tbl");

  private static Column col(String name, Type type) {
    return GlueColumn.builder().withName(name).withType(type).withNullable(true).build();
  }

  private GlueTable createHive(
      Column[] columns,
      Map<String, String> props,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders) {
    return ops.createTable(
        HIVE_IDENT,
        columns,
        null,
        props,
        partitions,
        distribution,
        sortOrders,
        Indexes.EMPTY_INDEXES);
  }

  private void assertCreateRefused(
      String expectedText,
      Column[] columns,
      Map<String, String> props,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders) {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> createHive(columns, props, partitions, distribution, sortOrders));
    assertTrue(e.getMessage().contains(expectedText), e.getMessage());
    verify(mockClient, never()).createTable(any(CreateTableRequest.class));
  }

  private static final Column[] ABC = {
    col("a", Types.StringType.get()),
    col("b", Types.IntegerType.get()),
    col("c", Types.DateType.get())
  };

  @Test
  void testCreateRefusesPartitionColumnsThatAreNotTrailing() {
    assertCreateRefused(
        "must be the last columns",
        ABC,
        Collections.emptyMap(),
        new Transform[] {Transforms.identity("a")},
        Distributions.NONE,
        SortOrders.NONE);
    assertCreateRefused(
        "must be the last columns",
        ABC,
        Collections.emptyMap(),
        new Transform[] {Transforms.identity("c"), Transforms.identity("b")},
        Distributions.NONE,
        SortOrders.NONE);
  }

  @Test
  void testCreateStoresTrailingPartitionColumns() {
    GlueTable created =
        createHive(
            ABC,
            Collections.emptyMap(),
            new Transform[] {Transforms.identity("b"), Transforms.identity("c")},
            Distributions.NONE,
            SortOrders.NONE);

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(mockClient).createTable(captor.capture());
    TableInput input = captor.getValue().tableInput();
    assertEquals(List.of("b", "c"), input.partitionKeys().stream().map(c -> c.name()).toList());
    assertEquals(2, created.partitioning().length);
  }

  @Test
  void testCreateRefusesComplexPartitionColumn() {
    Column[] columns = {
      col("id", Types.LongType.get()),
      col("m", Types.MapType.valueNullable(Types.StringType.get(), Types.IntegerType.get()))
    };
    assertCreateRefused(
        "primitive type",
        columns,
        Collections.emptyMap(),
        new Transform[] {Transforms.identity("m")},
        Distributions.NONE,
        SortOrders.NONE);
  }

  @Test
  void testCreateRefusesDuplicateAndAutoIncrementColumns() {
    assertCreateRefused(
        "Duplicate column name",
        new Column[] {col("abc", Types.IntegerType.get()), col("Abc", Types.StringType.get())},
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE);
    Column autoInc =
        GlueColumn.builder()
            .withName("id")
            .withType(Types.LongType.get())
            .withNullable(true)
            .withAutoIncrement(true)
            .build();
    assertCreateRefused(
        "auto-increment",
        new Column[] {autoInc},
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE);
  }

  @Test
  void testCreateRefusesUnsupportedDistributionAndSortOrder() {
    assertCreateRefused(
        "hash distribution",
        ABC,
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.of(Strategy.RANGE, 4, NamedReference.field("a")),
        SortOrders.NONE);
    assertCreateRefused(
        "at least 1",
        ABC,
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.hash(0, NamedReference.field("a")),
        SortOrders.NONE);
    assertCreateRefused(
        "plain columns",
        ABC,
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.hash(4, FunctionExpression.of("abs", NamedReference.field("b"))),
        SortOrders.NONE);
    assertCreateRefused(
        "nulls_first",
        ABC,
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[] {
          SortOrders.of(
              NamedReference.field("a"), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST)
        });
  }

  @Test
  void testCreateDefaultsToTextStorage() {
    createHive(
        ABC,
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE);

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(mockClient).createTable(captor.capture());
    StorageDescriptor sd = captor.getValue().tableInput().storageDescriptor();
    assertEquals(HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS, sd.inputFormat());
    assertEquals(HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS, sd.outputFormat());
    assertEquals(
        HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS, sd.serdeInfo().serializationLibrary());
  }

  @Test
  void testCreateRefusesConflictingOrPartialStorageClasses() {
    assertCreateRefused(
        "conflicts",
        ABC,
        Map.of(
            GlueConstants.FORMAT,
            "csv",
            GlueConstants.INPUT_FORMAT_CLASS,
            HiveStorageConstants.ORC_INPUT_FORMAT_CLASS),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE);
    assertCreateRefused(
        "Set all of",
        ABC,
        Map.of(GlueConstants.INPUT_FORMAT_CLASS, HiveStorageConstants.ORC_INPUT_FORMAT_CLASS),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE);
    assertCreateRefused(
        "Unsupported table format",
        ABC,
        Map.of(
            GlueConstants.FORMAT,
            "garbage",
            GlueConstants.INPUT_FORMAT_CLASS,
            "a",
            GlueConstants.OUTPUT_FORMAT,
            "b",
            GlueConstants.SERDE_LIB,
            "c"),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE);
  }

  @Test
  void testCreateTreatsBlankLocationAsAbsentAndReturnsStoredTable() {
    GlueTable created =
        createHive(
            ABC,
            Map.of(GlueConstants.LOCATION, "  "),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            SortOrders.NONE);

    assertEquals(
        "s3://test-bucket/warehouse/mydb/hive_tbl",
        created.properties().get(GlueConstants.LOCATION));
    assertEquals(
        HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS,
        created.properties().get(GlueConstants.INPUT_FORMAT_CLASS));
  }

  @Test
  void testCreateUnsupportedTypeNamesColumn() {
    assertCreateRefused(
        "Column f",
        new Column[] {col("f", Types.FixedType.of(16))},
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE);
  }

  @Test
  void testLoadRefusesTableWithDuplicateColumns() {
    useFakeGlueTable(
        Table.builder()
            .name("dup")
            .storageDescriptor(
                StorageDescriptor.builder()
                    .columns(glueCol("id", "int"), glueCol("id", "string"))
                    .build())
            .build());

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> ops.loadTable(NameIdentifier.of("metalake", "catalog", "mydb", "dup")));
    assertTrue(e.getMessage().contains("more than one column named id"), e.getMessage());
  }

  // -------------------------------------------------------------------------
  // Hive-format alter
  // -------------------------------------------------------------------------

  private static final NameIdentifier ALTER_IDENT =
      NameIdentifier.of("metalake", "catalog", "mydb", "native");

  private static software.amazon.awssdk.services.glue.model.Column glueCol(
      String name, String type) {
    return software.amazon.awssdk.services.glue.model.Column.builder()
        .name(name)
        .type(type)
        .build();
  }

  /** A table as a crawler or another engine leaves it, with fields Gravitino does not model. */
  private static Table nativeTable() {
    return Table.builder()
        .name("native")
        .owner("crawler")
        .retention(7)
        .tableType("MANAGED_TABLE")
        .versionId("42")
        .parameters(Map.of("classification", "csv"))
        .partitionKeys(glueCol("dt", "string"))
        .storageDescriptor(
            StorageDescriptor.builder()
                .columns(
                    glueCol("id", "int").toBuilder().parameters(Map.of("stat", "1")).build(),
                    glueCol("s", "struct<Outer:int>"),
                    glueCol("k", "string"))
                .location("s3://bucket/native")
                .inputFormat(HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS)
                .outputFormat(HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS)
                .serdeInfo(
                    SerDeInfo.builder()
                        .serializationLibrary(HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS)
                        .build())
                .compressed(true)
                .storedAsSubDirectories(true)
                .numberOfBuckets(4)
                .bucketColumns("k")
                .sortColumns(Order.builder().column("k").sortOrder(1).build())
                .parameters(Map.of("recordCount", "10"))
                .skewedInfo(SkewedInfo.builder().skewedColumnNames("k").build())
                .build())
        .build();
  }

  @Test
  void testCreateHiveTableRefusesIcebergTableType() {
    assertCreateRefused(
        "is not allowed on a Hive-format table",
        ABC,
        Map.of(GlueConstants.TABLE_TYPE_PARAM, "ICEBERG"),
        new Transform[0],
        Distributions.NONE,
        SortOrders.NONE);
  }

  @Test
  void testCreateReturnsSubmittedDefinitionWhenReadBackFails() {
    when(mockClient.createTable(any(CreateTableRequest.class)))
        .thenReturn(CreateTableResponse.builder().build());
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenThrow(
            ThrottlingException.builder()
                .awsErrorDetails(
                    AwsErrorDetails.builder()
                        .errorCode("ThrottlingException")
                        .errorMessage("Rate exceeded")
                        .build())
                .build());

    GlueTable table =
        createHive(ABC, Map.of(), new Transform[0], Distributions.NONE, SortOrders.NONE);

    assertEquals("hive_tbl", table.name());
    assertEquals(3, table.columns().length);
    assertEquals(
        HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS,
        table.properties().get(GlueConstants.INPUT_FORMAT_CLASS));
  }

  @Test
  void testCreateTreatsBlankStorageClassesAsUnset() {
    useFakeGlueTable(null);
    GlueTable table =
        createHive(
            ABC,
            Map.of(
                GlueConstants.INPUT_FORMAT_CLASS, " ",
                GlueConstants.OUTPUT_FORMAT, "",
                GlueConstants.SERDE_LIB, " "),
            new Transform[0],
            Distributions.NONE,
            SortOrders.NONE);
    assertEquals(
        HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS,
        table.properties().get(GlueConstants.INPUT_FORMAT_CLASS));
  }

  @Test
  void testCreateRefusesMismatchedBuiltInFormatClasses() {
    assertCreateRefused(
        "belong to different storage formats",
        ABC,
        Map.of(
            GlueConstants.INPUT_FORMAT_CLASS, HiveStorageConstants.ORC_INPUT_FORMAT_CLASS,
            GlueConstants.OUTPUT_FORMAT, HiveStorageConstants.PARQUET_OUTPUT_FORMAT_CLASS,
            GlueConstants.SERDE_LIB, HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS),
        new Transform[0],
        Distributions.NONE,
        SortOrders.NONE);

    // A custom SerDe over text files is a common combination.
    useFakeGlueTable(null);
    GlueTable table =
        createHive(
            ABC,
            Map.of(
                GlueConstants.INPUT_FORMAT_CLASS, HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS,
                GlueConstants.OUTPUT_FORMAT, HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS,
                GlueConstants.SERDE_LIB, "org.openx.data.jsonserde.JsonSerDe"),
            new Transform[0],
            Distributions.NONE,
            SortOrders.NONE);
    assertEquals(
        "org.openx.data.jsonserde.JsonSerDe", table.properties().get(GlueConstants.SERDE_LIB));
  }

  @Test
  void testAlterRenamesSkewedColumns() {
    UpdateTableRequest req = alterAndCapture(TableChange.renameColumn(new String[] {"k"}, "k2"));
    assertEquals(
        List.of("k2"), req.tableInput().storageDescriptor().skewedInfo().skewedColumnNames());
  }

  @Test
  void testAlterRefusesDeletingSkewedColumn() {
    Table table = nativeTable();
    useFakeGlueTable(
        table.toBuilder()
            .storageDescriptor(
                table.storageDescriptor().toBuilder()
                    .skewedInfo(SkewedInfo.builder().skewedColumnNames("id").build())
                    .build())
            .build());
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ops.alterTable(ALTER_IDENT, TableChange.deleteColumn(new String[] {"id"}, false)));
    assertTrue(e.getMessage().contains("skewed"), e.getMessage());
  }

  @Test
  void testAlterRemovingStoragePropertyRemovesParameterOfSameName() {
    Table table = nativeTable();
    useFakeGlueTable(
        table.toBuilder()
            .parameters(Map.of("classification", "csv", GlueConstants.SERDE_NAME, "shadow"))
            .storageDescriptor(
                table.storageDescriptor().toBuilder()
                    .serdeInfo(table.storageDescriptor().serdeInfo().toBuilder().name("s").build())
                    .build())
            .build());
    ops.alterTable(ALTER_IDENT, TableChange.removeProperty(GlueConstants.SERDE_NAME));

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(mockClient).updateTable(captor.capture());
    TableInput in = captor.getValue().tableInput();
    assertFalse(in.parameters().containsKey(GlueConstants.SERDE_NAME));
    assertNull(in.storageDescriptor().serdeInfo().name());
  }

  @Test
  void testAlterRefusesChangingTableFormat() {
    useFakeGlueTable(nativeTable());
    for (TableChange change :
        new TableChange[] {
          TableChange.setProperty(GlueConstants.TABLE_FORMAT, "iceberg"),
          TableChange.setProperty(GlueConstants.TABLE_TYPE_PARAM, "ICEBERG"),
          TableChange.removeProperty(GlueConstants.TABLE_FORMAT)
        }) {
      assertThrows(IllegalArgumentException.class, () -> ops.alterTable(ALTER_IDENT, change));
    }
    verify(mockClient, never()).updateTable(any(UpdateTableRequest.class));
  }

  private UpdateTableRequest alterAndCapture(TableChange... changes) {
    useFakeGlueTable(nativeTable());
    ops.alterTable(ALTER_IDENT, changes);
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(mockClient).updateTable(captor.capture());
    return captor.getValue();
  }

  @Test
  void testAlterKeepsFieldsGravitinoDoesNotModel() {
    UpdateTableRequest req = alterAndCapture(TableChange.updateComment("new"));

    assertEquals("42", req.versionId());
    TableInput in = req.tableInput();
    assertEquals("new", in.description());
    assertEquals("crawler", in.owner());
    assertEquals(7, in.retention());
    assertEquals("MANAGED_TABLE", in.tableType());
    StorageDescriptor sd = in.storageDescriptor();
    assertEquals(Map.of("stat", "1"), sd.columns().get(0).parameters());
    assertEquals("struct<Outer:int>", sd.columns().get(1).type());
    assertTrue(sd.compressed());
    assertTrue(sd.storedAsSubDirectories());
    assertEquals(4, sd.numberOfBuckets());
    assertEquals(Map.of("recordCount", "10"), sd.parameters());
    assertEquals(List.of("k"), sd.skewedInfo().skewedColumnNames());
  }

  @Test
  void testAlterChangesLocationAndSerdeProperties() {
    UpdateTableRequest req =
        alterAndCapture(
            TableChange.setProperty(GlueConstants.LOCATION, "s3://bucket/moved"),
            TableChange.setProperty(GlueConstants.SERDE_PARAMETER_PREFIX + "field.delim", ","),
            TableChange.setProperty(GlueConstants.SERDE_NAME, "my_serde"));

    StorageDescriptor sd = req.tableInput().storageDescriptor();
    assertEquals("s3://bucket/moved", sd.location());
    assertEquals(",", sd.serdeInfo().parameters().get("field.delim"));
    assertEquals("my_serde", sd.serdeInfo().name());
    assertEquals(
        HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS, sd.serdeInfo().serializationLibrary());
    assertFalse(req.tableInput().parameters().containsKey(GlueConstants.LOCATION));
  }

  @Test
  void testAlterRefusesChangingStorageFormat() {
    useFakeGlueTable(nativeTable());
    assertThrows(
        IllegalArgumentException.class,
        () -> ops.alterTable(ALTER_IDENT, TableChange.setProperty(GlueConstants.FORMAT, "orc")));
    verify(mockClient, never()).updateTable(any(UpdateTableRequest.class));
  }

  @Test
  void testAlterTableWithoutStorageDescriptorKeepsIt() {
    useFakeGlueTable(Table.builder().name("native").partitionKeys(glueCol("dt", "string")).build());

    ops.alterTable(ALTER_IDENT, TableChange.updateComment("c"));

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(mockClient).updateTable(captor.capture());
    assertNull(captor.getValue().tableInput().storageDescriptor());
  }

  @Test
  void testAlterRefusesNestedAndMissingColumns() {
    useFakeGlueTable(nativeTable());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTable(
                ALTER_IDENT,
                TableChange.updateColumnType(new String[] {"s", "Outer"}, Types.LongType.get())));
    assertThrows(
        IllegalArgumentException.class,
        () -> ops.alterTable(ALTER_IDENT, TableChange.deleteColumn(new String[] {"nope"}, false)));
    assertThrows(
        IllegalArgumentException.class,
        () -> ops.alterTable(ALTER_IDENT, TableChange.renameColumn(new String[] {"nope"}, "x")));

    // Deleting a missing column with ifExists is a no-op and writes nothing.
    ops.alterTable(ALTER_IDENT, TableChange.deleteColumn(new String[] {"nope"}, true));
    verify(mockClient, never()).updateTable(any(UpdateTableRequest.class));
  }

  @Test
  void testCreateHiveTableFoldsColumnNames() {
    useFakeGlueTable(null);
    Column[] columns = {col("UserId", Types.LongType.get()), col("Region", Types.StringType.get())};
    createHive(
        columns,
        Collections.emptyMap(),
        new Transform[] {Transforms.identity("Region")},
        Distributions.of(Strategy.HASH, 4, NamedReference.field("UserId")),
        new SortOrder[] {SortOrders.of(NamedReference.field("UserId"), SortDirection.ASCENDING)});

    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(mockClient).createTable(captor.capture());
    TableInput input = captor.getValue().tableInput();
    assertEquals("userid", input.storageDescriptor().columns().get(0).name());
    assertEquals("region", input.partitionKeys().get(0).name());
    assertEquals(List.of("userid"), input.storageDescriptor().bucketColumns());
    assertEquals("userid", input.storageDescriptor().sortColumns().get(0).column());
  }

  @Test
  void testAlterFoldsNewHiveColumnNames() {
    UpdateTableRequest req =
        alterAndCapture(
            TableChange.renameColumn(new String[] {"k"}, "KeyCol"),
            TableChange.addColumn(new String[] {"NewCol"}, Types.StringType.get()));

    StorageDescriptor sd = req.tableInput().storageDescriptor();
    assertEquals(
        List.of("id", "s", "keycol", "newcol"), sd.columns().stream().map(c -> c.name()).toList());
    assertEquals(List.of("keycol"), sd.bucketColumns());
    assertEquals("keycol", sd.sortColumns().get(0).column());
  }

  @Test
  void testAlterReturnsSubmittedDefinitionWhenReadBackFails() {
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(nativeTable()).build())
        .thenThrow(ThrottlingException.builder().message("slow down").build());

    GlueTable table = ops.alterTable(ALTER_IDENT, TableChange.setProperty("k1", "v1"));

    verify(mockClient).updateTable(any(UpdateTableRequest.class));
    assertEquals("v1", table.properties().get("k1"));
    assertEquals("id", table.columns()[0].name());
  }

  @Test
  void testSetStoragePropertyRemovesTableParameterOfSameName() {
    useFakeGlueTable(
        nativeTable().toBuilder().parameters(Map.of(GlueConstants.SERDE_NAME, "old")).build());

    ops.alterTable(ALTER_IDENT, TableChange.setProperty(GlueConstants.SERDE_NAME, "new"));

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(mockClient).updateTable(captor.capture());
    TableInput input = captor.getValue().tableInput();
    assertEquals("new", input.storageDescriptor().serdeInfo().name());
    assertFalse(input.parameters().containsKey(GlueConstants.SERDE_NAME));
  }

  @Test
  void testCreateRefusesSerDeOfAnotherBuiltInFormat() {
    assertCreateRefused(
        "belong to different storage formats",
        ABC,
        Map.of(
            GlueConstants.INPUT_FORMAT_CLASS, HiveStorageConstants.ORC_INPUT_FORMAT_CLASS,
            GlueConstants.OUTPUT_FORMAT, HiveStorageConstants.ORC_OUTPUT_FORMAT_CLASS,
            GlueConstants.SERDE_LIB, HiveStorageConstants.PARQUET_SERDE_CLASS),
        new Transform[0],
        Distributions.NONE,
        SortOrders.NONE);
    assertCreateRefused(
        "belong to different storage formats",
        ABC,
        Map.of(
            GlueConstants.INPUT_FORMAT_CLASS, HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS,
            GlueConstants.OUTPUT_FORMAT, HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS,
            GlueConstants.SERDE_LIB, HiveStorageConstants.ORC_SERDE_CLASS),
        new Transform[0],
        Distributions.NONE,
        SortOrders.NONE);

    // A custom SerDe is taken as given, even over a format with its own SerDe.
    useFakeGlueTable(null);
    createHive(
        ABC,
        Map.of(
            GlueConstants.INPUT_FORMAT_CLASS, HiveStorageConstants.ORC_INPUT_FORMAT_CLASS,
            GlueConstants.OUTPUT_FORMAT, HiveStorageConstants.ORC_OUTPUT_FORMAT_CLASS,
            GlueConstants.SERDE_LIB, "com.example.CustomSerDe"),
        new Transform[0],
        Distributions.NONE,
        SortOrders.NONE);
    verify(mockClient).createTable(any(CreateTableRequest.class));
  }

  @Test
  void testAlterMatchesColumnNamesWithoutCase() {
    UpdateTableRequest req =
        alterAndCapture(TableChange.updateColumnComment(new String[] {"ID"}, "the id"));
    assertEquals("the id", req.tableInput().storageDescriptor().columns().get(0).comment());
  }

  @Test
  void testAlterAddColumnHonorsPosition() {
    UpdateTableRequest req =
        alterAndCapture(
            TableChange.addColumn(
                new String[] {"first_col"},
                Types.StringType.get(),
                null,
                TableChange.ColumnPosition.first()),
            TableChange.addColumn(
                new String[] {"after_id"},
                Types.StringType.get(),
                null,
                TableChange.ColumnPosition.after("id")));

    assertEquals(
        List.of("first_col", "id", "after_id", "s", "k"),
        req.tableInput().storageDescriptor().columns().stream().map(c -> c.name()).toList());
  }

  @Test
  void testAlterRefusesBadAddColumns() {
    useFakeGlueTable(nativeTable());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTable(
                ALTER_IDENT,
                TableChange.addColumn(
                    new String[] {"x"},
                    Types.StringType.get(),
                    null,
                    TableChange.ColumnPosition.after("nope"))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTable(
                ALTER_IDENT,
                TableChange.addColumn(
                    new String[] {"x"},
                    Types.LongType.get(),
                    null,
                    TableChange.ColumnPosition.defaultPos(),
                    true,
                    true)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTable(
                ALTER_IDENT, TableChange.addColumn(new String[] {"ID"}, Types.StringType.get())));
    verify(mockClient, never()).updateTable(any(UpdateTableRequest.class));
  }

  @Test
  void testAlterUpdatesColumnPosition() {
    UpdateTableRequest req =
        alterAndCapture(
            TableChange.updateColumnPosition(
                new String[] {"k"}, TableChange.ColumnPosition.first()));
    assertEquals(
        List.of("k", "id", "s"),
        req.tableInput().storageDescriptor().columns().stream().map(c -> c.name()).toList());
  }

  @Test
  void testAlterRenameBucketColumnUpdatesBucketAndSortColumns() {
    UpdateTableRequest req = alterAndCapture(TableChange.renameColumn(new String[] {"k"}, "k2"));
    StorageDescriptor sd = req.tableInput().storageDescriptor();
    assertEquals(List.of("k2"), sd.bucketColumns());
    assertEquals("k2", sd.sortColumns().get(0).column());
  }

  @Test
  void testAlterRefusesDeletingBucketColumn() {
    useFakeGlueTable(nativeTable());
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> ops.alterTable(ALTER_IDENT, TableChange.deleteColumn(new String[] {"k"}, false)));
    assertTrue(e.getMessage().contains("Cannot delete column k"), e.getMessage());
  }

  @Test
  void testAlterConcurrentModificationIsRetryable() {
    useFakeGlueTable(nativeTable());
    when(mockClient.updateTable(any(UpdateTableRequest.class)))
        .thenThrow(
            ConcurrentModificationException.builder()
                .message("Update table failed due to concurrent modifications")
                .build());

    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () -> ops.alterTable(ALTER_IDENT, TableChange.setProperty("k", "v")));
    assertTrue(e.getMessage().contains("was changed concurrently"), e.getMessage());
  }
}
