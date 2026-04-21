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
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;

class TestGlueCatalogOperations {

  private GlueCatalogOperations ops;
  private GlueClient mockClient;

  @BeforeEach
  void setup() {
    mockClient = mock(GlueClient.class);
    ops = new GlueCatalogOperations();
    ops.glueClient = mockClient;
    // catalogId is null by default (caller's AWS account)
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

    NameIdentifier[] result = ops.listSchemas(ns);

    assertEquals(0, result.length);
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

    boolean dropped = ops.dropSchema(ident, true);

    assertFalse(dropped);
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
}
