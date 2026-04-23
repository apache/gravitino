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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

class TestGlueCatalogOperations {

  private GlueCatalogOperations ops;
  private GlueClient mockClient;

  @BeforeEach
  void setup() {
    mockClient = mock(GlueClient.class);
    ops = new GlueCatalogOperations();
    ops.glueClient = mockClient;
  }

  // -------------------------------------------------------------------------
  // listSchemas
  // -------------------------------------------------------------------------

  @Test
  void testListSchemas() {
    Namespace ns = Namespace.of("metalake", "catalog");
    Database db1 = Database.builder().name("db1").build();
    Database db2 = Database.builder().name("db2").build();
    Database db3 = Database.builder().name("db3").build();
    Database db4 = Database.builder().name("db4").build();

    when(mockClient.getDatabases(any(GetDatabasesRequest.class)))
        .thenReturn(
            GetDatabasesResponse.builder().databaseList(db1, db2).nextToken("token1").build())
        .thenReturn(GetDatabasesResponse.builder().databaseList(db3, db4).nextToken(null).build());

    NameIdentifier[] result = ops.listSchemas(ns);

    assertEquals(4, result.length);
    assertEquals("db1", result[0].name());
    assertEquals("db4", result[3].name());
  }

  @Test
  void testListSchemasReturnsEmptyArray() {
    Namespace ns = Namespace.of("metalake", "catalog");
    when(mockClient.getDatabases(any(GetDatabasesRequest.class)))
        .thenReturn(
            GetDatabasesResponse.builder()
                .databaseList(Collections.emptyList())
                .nextToken(null)
                .build());

    assertEquals(0, ops.listSchemas(ns).length);
  }

  // -------------------------------------------------------------------------
  // createSchema
  // -------------------------------------------------------------------------

  @Test
  void testCreateSchema() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb");
    Map<String, String> props = Map.of("k", "v");

    GlueSchema schema = ops.createSchema(ident, "my comment", props);

    verify(mockClient).createDatabase(any(CreateDatabaseRequest.class));
    assertEquals("mydb", schema.name());
    assertEquals("my comment", schema.comment());
    assertEquals(props, schema.properties());
  }

  @Test
  void testCreateSchemaAlreadyExists() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb");
    when(mockClient.createDatabase(any(CreateDatabaseRequest.class)))
        .thenThrow(AlreadyExistsException.builder().message("exists").build());

    assertThrows(
        SchemaAlreadyExistsException.class,
        () -> ops.createSchema(ident, null, Collections.emptyMap()));
  }

  // -------------------------------------------------------------------------
  // loadSchema
  // -------------------------------------------------------------------------

  @Test
  void testLoadSchema() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb");
    Database db =
        Database.builder()
            .name("mydb")
            .description("desc")
            .parameters(Map.of("k", "v"))
            .createTime(Instant.now())
            .build();
    when(mockClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(GetDatabaseResponse.builder().database(db).build());

    GlueSchema schema = ops.loadSchema(ident);

    assertEquals("mydb", schema.name());
    assertEquals("desc", schema.comment());
    assertEquals(Map.of("k", "v"), schema.properties());
  }

  @Test
  void testLoadSchemaNotFound() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "missing");
    when(mockClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertThrows(NoSuchSchemaException.class, () -> ops.loadSchema(ident));
  }

  // -------------------------------------------------------------------------
  // alterSchema
  // -------------------------------------------------------------------------

  @Test
  void testAlterSchemaSetProperty() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb");
    Database db =
        Database.builder()
            .name("mydb")
            .parameters(Map.of("existing", "val"))
            .createTime(Instant.now())
            .build();
    when(mockClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(GetDatabaseResponse.builder().database(db).build());
    when(mockClient.updateDatabase(any(UpdateDatabaseRequest.class)))
        .thenReturn(UpdateDatabaseResponse.builder().build());

    ArgumentCaptor<UpdateDatabaseRequest> captor =
        ArgumentCaptor.forClass(UpdateDatabaseRequest.class);

    GlueSchema result = ops.alterSchema(ident, SchemaChange.setProperty("newKey", "newVal"));

    verify(mockClient).updateDatabase(captor.capture());
    Map<String, String> sentParams = captor.getValue().databaseInput().parameters();
    assertEquals("val", sentParams.get("existing"));
    assertEquals("newVal", sentParams.get("newKey"));
    assertEquals("newVal", result.properties().get("newKey"));
  }

  @Test
  void testAlterSchemaRemoveProperty() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb");
    Database db =
        Database.builder()
            .name("mydb")
            .parameters(Map.of("toRemove", "v"))
            .createTime(Instant.now())
            .build();
    when(mockClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(GetDatabaseResponse.builder().database(db).build());
    when(mockClient.updateDatabase(any(UpdateDatabaseRequest.class)))
        .thenReturn(UpdateDatabaseResponse.builder().build());

    ArgumentCaptor<UpdateDatabaseRequest> captor =
        ArgumentCaptor.forClass(UpdateDatabaseRequest.class);

    ops.alterSchema(ident, SchemaChange.removeProperty("toRemove"));

    verify(mockClient).updateDatabase(captor.capture());
    assertFalse(captor.getValue().databaseInput().parameters().containsKey("toRemove"));
  }

  @Test
  void testAlterSchemaUnsupportedChange() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb");
    Database db =
        Database.builder().name("mydb").parameters(Map.of()).createTime(Instant.now()).build();
    when(mockClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(GetDatabaseResponse.builder().database(db).build());

    SchemaChange unsupported = mock(SchemaChange.class);

    assertThrows(IllegalArgumentException.class, () -> ops.alterSchema(ident, unsupported));
  }

  @Test
  void testAlterSchemaNotFound() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "missing");
    when(mockClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertThrows(
        NoSuchSchemaException.class,
        () -> ops.alterSchema(ident, SchemaChange.setProperty("k", "v")));
  }

  // -------------------------------------------------------------------------
  // dropSchema
  // -------------------------------------------------------------------------

  @Test
  void testDropSchema() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb");
    when(mockClient.getTables(any(GetTablesRequest.class)))
        .thenReturn(GetTablesResponse.builder().tableList(List.of()).build());

    boolean dropped = ops.dropSchema(ident, false);

    verify(mockClient).deleteDatabase(any(DeleteDatabaseRequest.class));
    assertTrue(dropped);
  }

  @Test
  void testDropSchemaNonEmpty() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb");
    Table t = Table.builder().name("t1").build();
    when(mockClient.getTables(any(GetTablesRequest.class)))
        .thenReturn(GetTablesResponse.builder().tableList(t).build());

    assertThrows(NonEmptySchemaException.class, () -> ops.dropSchema(ident, false));
    verify(mockClient, never()).deleteDatabase(any(DeleteDatabaseRequest.class));
  }

  @Test
  void testDropSchemaCascadeTrue() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb");

    boolean dropped = ops.dropSchema(ident, true);

    verify(mockClient, never()).getTables(any(GetTablesRequest.class));
    verify(mockClient).deleteDatabase(any(DeleteDatabaseRequest.class));
    assertTrue(dropped);
  }

  @Test
  void testDropSchemaNotFound() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "missing");
    when(mockClient.deleteDatabase(any(DeleteDatabaseRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertFalse(ops.dropSchema(ident, true));
  }

  @Test
  void testDropSchemaWithCatalogId() {
    ops.catalogId = "123456789012";
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb");
    when(mockClient.getTables(any(GetTablesRequest.class)))
        .thenReturn(GetTablesResponse.builder().tableList(List.of()).build());

    ArgumentCaptor<DeleteDatabaseRequest> captor =
        ArgumentCaptor.forClass(DeleteDatabaseRequest.class);

    ops.dropSchema(ident, false);

    verify(mockClient).deleteDatabase(captor.capture());
    assertEquals("123456789012", captor.getValue().catalogId());
  }

  // -------------------------------------------------------------------------
  // listTables
  // -------------------------------------------------------------------------

  @Test
  void testListTables() {
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
  void testListTablesWithFormatFilter() {
    ops.tableFormatFilter = java.util.Set.of("iceberg");
    Namespace ns = Namespace.of("metalake", "catalog", "mydb");

    Table icebergTable =
        Table.builder()
            .name("ice_tbl")
            .parameters(Map.of(GlueConstants.TABLE_FORMAT, "ICEBERG"))
            .build();
    Table hiveTable = Table.builder().name("hive_tbl").parameters(Collections.emptyMap()).build();

    when(mockClient.getTables(any(GetTablesRequest.class)))
        .thenReturn(
            GetTablesResponse.builder().tableList(icebergTable, hiveTable).nextToken(null).build());

    NameIdentifier[] result = ops.listTables(ns);

    assertEquals(1, result.length);
    assertEquals("ice_tbl", result[0].name());
  }

  // -------------------------------------------------------------------------
  // loadTable
  // -------------------------------------------------------------------------

  @Test
  void testLoadTable() {
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
  void testCreateTable() {
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
            Map.of(GlueConstants.LOCATION, "s3://bucket/path"),
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
                Collections.emptyMap(),
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                SortOrders.NONE,
                Indexes.EMPTY_INDEXES));
  }

  @Test
  void testCreateTableRejectsIndexes() {
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
                new org.apache.gravitino.rel.indexes.Index[] {
                  mock(org.apache.gravitino.rel.indexes.Index.class)
                }));
  }

  @Test
  void testCreateTableStorageDescriptorProperties() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "mytable");
    Map<String, String> props =
        Map.of(
            GlueConstants.LOCATION, "s3://my-bucket/path",
            GlueConstants.INPUT_FORMAT, "org.apache.hadoop.mapred.TextInputFormat",
            GlueConstants.OUTPUT_FORMAT,
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            GlueConstants.TABLE_TYPE, "EXTERNAL_TABLE");

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
    assertFalse(req.tableInput().parameters().containsKey(GlueConstants.TABLE_TYPE));
  }

  // -------------------------------------------------------------------------
  // alterTable
  // -------------------------------------------------------------------------

  @Test
  void testAlterTable() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "old");
    Table glueTable =
        Table.builder()
            .name("old")
            .description("old comment")
            .storageDescriptor(StorageDescriptor.builder().build())
            .createTime(Instant.now())
            .build();
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(glueTable).build());
    when(mockClient.updateTable(any(UpdateTableRequest.class)))
        .thenReturn(UpdateTableResponse.builder().build());

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);

    GlueTable result =
        ops.alterTable(ident, TableChange.rename("new"), TableChange.updateComment("new comment"));

    verify(mockClient).updateTable(captor.capture());
    assertEquals("new", captor.getValue().tableInput().name());
    assertEquals("new comment", result.comment());
  }

  @Test
  void testAlterTableSetProperty() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "t");
    Table glueTable =
        Table.builder()
            .name("t")
            .parameters(Map.of("existing", "v1"))
            .storageDescriptor(StorageDescriptor.builder().build())
            .createTime(Instant.now())
            .build();
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(glueTable).build());
    when(mockClient.updateTable(any(UpdateTableRequest.class)))
        .thenReturn(UpdateTableResponse.builder().build());

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
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(glueTable).build());
    when(mockClient.updateTable(any(UpdateTableRequest.class)))
        .thenReturn(UpdateTableResponse.builder().build());

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
  void testDropTable() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "t");

    assertTrue(ops.dropTable(ident));
    verify(mockClient).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  void testDropTableNotFound() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "missing");
    when(mockClient.deleteTable(any(DeleteTableRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertFalse(ops.dropTable(ident));
  }

  @Test
  void testDropTableWithCatalogId() {
    ops.catalogId = "123456789012";
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "mydb", "t");
    ArgumentCaptor<DeleteTableRequest> captor = ArgumentCaptor.forClass(DeleteTableRequest.class);

    ops.dropTable(ident);

    verify(mockClient).deleteTable(captor.capture());
    assertEquals("123456789012", captor.getValue().catalogId());
    assertEquals("mydb", captor.getValue().databaseName());
    assertEquals("t", captor.getValue().name());
  }
}
