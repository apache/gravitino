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
import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange.AddColumn;
import org.apache.fluss.metadata.TableChange.After;
import org.apache.fluss.metadata.TableChange.DropColumn;
import org.apache.fluss.metadata.TableChange.First;
import org.apache.fluss.metadata.TableChange.Last;
import org.apache.fluss.metadata.TableChange.ModifyColumn;
import org.apache.fluss.metadata.TableChange.RenameColumn;
import org.apache.fluss.metadata.TableChange.ResetOption;
import org.apache.fluss.metadata.TableChange.SetOption;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestFlussCatalogOperations {

  private static final NameIdentifier SCHEMA_IDENT = NameIdentifier.of("metalake", "catalog", "db");
  private static final NameIdentifier TABLE_IDENT =
      NameIdentifier.of("metalake", "catalog", "db", "orders");
  private static final TablePath TABLE_PATH = TablePath.of("db", "orders");

  @Test
  void testToFlussConfiguration() {
    Configuration configuration =
        FlussCatalogOperations.toFlussConfiguration(
            Map.of(
                FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS,
                "localhost:9123",
                CATALOG_BYPASS_PREFIX + "client.request.timeout",
                "10 s",
                ID_KEY,
                "catalog-id"),
            catalogInfo());

    assertEquals(List.of("localhost:9123"), configuration.get(ConfigOptions.BOOTSTRAP_SERVERS));
    assertEquals("10 s", configuration.toMap().get("client.request.timeout"));
    assertEquals(
        "gravitino-catalog-id-metalake.catalog", configuration.getString(ConfigOptions.CLIENT_ID));
  }

  @Test
  void testToFlussConfigurationRequiresBootstrapServers() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> FlussCatalogOperations.toFlussConfiguration(Map.of(), catalogInfo()));

    assertTrue(exception.getMessage().contains("Missing configuration: bootstrap.servers"));
  }

  @Test
  void testInitializeCreatesConnectionWithFlussConfiguration() throws Exception {
    Admin admin = mock(Admin.class);
    Connection connection = mock(Connection.class);
    AtomicReference<Configuration> capturedConfig = new AtomicReference<>();
    when(connection.getAdmin()).thenReturn(admin);
    when(admin.listDatabases()).thenReturn(CompletableFuture.completedFuture(List.of("db")));

    FlussCatalogOperations operations =
        new FlussCatalogOperations(
            config -> {
              capturedConfig.set(config);
              return connection;
            });
    operations.initialize(
        Map.of(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS, "localhost:9123"),
        catalogInfo(),
        null);

    assertEquals(
        List.of("localhost:9123"), capturedConfig.get().get(ConfigOptions.BOOTSTRAP_SERVERS));
    assertArrayEquals(
        new NameIdentifier[] {SCHEMA_IDENT},
        operations.listSchemas(Namespace.of("metalake", "catalog")));

    operations.close();
    verify(connection).close();
  }

  @Test
  void testInitializeMapsConnectionFailures() {
    FlussCatalogOperations operations =
        new FlussCatalogOperations(
            config -> {
              throw new IllegalStateException("auth failed");
            });

    ConnectionFailedException exception =
        assertThrows(
            ConnectionFailedException.class,
            () ->
                operations.initialize(
                    Map.of(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS, "localhost:9123"),
                    catalogInfo(),
                    null));

    assertTrue(exception.getMessage().contains("Failed to connect to Fluss cluster"));
  }

  @Test
  void testTestConnectionUsesInitializedOps() throws Exception {
    Admin admin = mock(Admin.class);
    Connection connection = mock(Connection.class);
    AtomicReference<Configuration> capturedConfig = new AtomicReference<>();
    when(connection.getAdmin()).thenReturn(admin);
    when(admin.listDatabases()).thenReturn(CompletableFuture.completedFuture(List.of("db")));

    FlussCatalogOperations operations =
        new FlussCatalogOperations(
            config -> {
              capturedConfig.set(config);
              return connection;
            });
    operations.initialize(
        Map.of(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS, "localhost:9123"),
        catalogInfo(),
        null);

    operations.testConnection(
        NameIdentifier.of("metalake", "catalog"),
        Catalog.Type.RELATIONAL,
        "fluss",
        null,
        Map.of(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS, "ignored:1"));

    assertEquals(
        List.of("localhost:9123"), capturedConfig.get().get(ConfigOptions.BOOTSTRAP_SERVERS));
    verify(admin).listDatabases();
    verify(connection, never()).close();

    operations.close();
    verify(connection).close();
  }

  @Test
  void testTestConnectionMapsFailures() {
    Admin admin = mock(Admin.class);
    Connection connection = mock(Connection.class);
    when(connection.getAdmin()).thenReturn(admin);
    when(admin.listDatabases()).thenReturn(failedFuture(new RuntimeException("network down")));

    FlussCatalogOperations operations = new FlussCatalogOperations(config -> connection);
    operations.initialize(
        Map.of(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS, "localhost:9123"), null, null);

    assertThrows(
        ConnectionFailedException.class,
        () ->
            operations.testConnection(
                NameIdentifier.of("metalake", "catalog"),
                Catalog.Type.RELATIONAL,
                "fluss",
                null,
                Map.of(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS, "localhost:9123")));
  }

  @Test
  void testTestConnectionRequiresInitialization() {
    FlussCatalogOperations operations =
        new FlussCatalogOperations(config -> mock(Connection.class));

    assertThrows(
        IllegalStateException.class,
        () ->
            operations.testConnection(
                NameIdentifier.of("metalake", "catalog"),
                Catalog.Type.RELATIONAL,
                "fluss",
                null,
                Map.of(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS, "localhost:9123")));
  }

  @Test
  void testSchemaCrudDelegatesToFlussAdmin() {
    Admin admin = mock(Admin.class);
    FlussCatalogOperations operations = operations(admin);
    when(admin.createDatabase(eq("db"), any(DatabaseDescriptor.class), eq(false)))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(admin.getDatabaseInfo("db"))
        .thenReturn(CompletableFuture.completedFuture(databaseInfo("db", "schema comment")));
    when(admin.listDatabases()).thenReturn(CompletableFuture.completedFuture(List.of("db", "db2")));
    when(admin.databaseExists("db")).thenReturn(CompletableFuture.completedFuture(true));
    when(admin.dropDatabase("db", false, false))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(admin.databaseExists("missing")).thenReturn(CompletableFuture.completedFuture(false));

    org.apache.gravitino.Schema schema =
        operations.createSchema(SCHEMA_IDENT, "schema comment", Map.of("owner", "fluss"));

    assertEquals("db", schema.name());
    assertEquals("schema comment", schema.comment());
    assertEquals("fluss", schema.properties().get("owner"));
    assertTrue(operations.schemaExists(SCHEMA_IDENT));
    assertArrayEquals(
        new NameIdentifier[] {SCHEMA_IDENT, NameIdentifier.of("metalake", "catalog", "db2")},
        operations.listSchemas(Namespace.of("metalake", "catalog")));
    assertEquals(schema.name(), operations.loadSchema(SCHEMA_IDENT).name());
    assertTrue(operations.dropSchema(SCHEMA_IDENT, false));
    assertFalse(operations.dropSchema(NameIdentifier.of("metalake", "catalog", "missing"), false));

    ArgumentCaptor<DatabaseDescriptor> descriptorCaptor =
        ArgumentCaptor.forClass(DatabaseDescriptor.class);
    verify(admin).createDatabase(eq("db"), descriptorCaptor.capture(), eq(false));
    assertEquals("schema comment", descriptorCaptor.getValue().getComment().orElse(null));
    assertEquals("fluss", descriptorCaptor.getValue().getCustomProperties().get("owner"));
    verify(admin).dropDatabase("db", false, false);
  }

  @Test
  void testTableCrudDelegatesToFlussAdmin() {
    Admin admin = mock(Admin.class);
    FlussCatalogOperations operations = operations(admin);
    when(admin.createTable(eq(TABLE_PATH), any(TableDescriptor.class), eq(false)))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(admin.getTableInfo(TABLE_PATH)).thenReturn(CompletableFuture.completedFuture(tableInfo()));
    when(admin.listTables("db")).thenReturn(CompletableFuture.completedFuture(List.of("orders")));
    when(admin.dropTable(TABLE_PATH, false)).thenReturn(CompletableFuture.completedFuture(null));

    Table table =
        operations.createTable(
            TABLE_IDENT,
            tableColumns(),
            "orders comment",
            Map.of("table.log.ttl", "1d"),
            new org.apache.gravitino.rel.expressions.transforms.Transform[] {
              Transforms.identity("event_day")
            },
            Distributions.hash(3, NamedReference.field("site_id")),
            SortOrders.NONE,
            new org.apache.gravitino.rel.indexes.Index[] {
              Indexes.primary("pk", new String[][] {{"event_day"}, {"site_id"}})
            });

    assertEquals("orders", table.name());
    assertEquals("orders comment", table.comment());
    assertArrayEquals(
        new NameIdentifier[] {TABLE_IDENT},
        operations.listTables(Namespace.of("metalake", "catalog", "db")));
    assertEquals(table.name(), operations.loadTable(TABLE_IDENT).name());
    assertTrue(operations.dropTable(TABLE_IDENT));

    ArgumentCaptor<TableDescriptor> descriptorCaptor =
        ArgumentCaptor.forClass(TableDescriptor.class);
    verify(admin).createTable(eq(TABLE_PATH), descriptorCaptor.capture(), eq(false));
    TableDescriptor descriptor = descriptorCaptor.getValue();
    assertEquals("orders comment", descriptor.getComment().orElse(null));
    assertEquals("1d", descriptor.getProperties().get("table.log.ttl"));
    assertEquals(List.of("event_day"), descriptor.getPartitionKeys());
    assertEquals(List.of("site_id"), descriptor.getBucketKeys());
  }

  @Test
  void testDropTableReturnsFalseWhenTableDoesNotExist() {
    Admin admin = mock(Admin.class);
    Connection connection = mock(Connection.class);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "db", "orders");
    TablePath tablePath = TablePath.of("db", "orders");
    when(connection.getAdmin()).thenReturn(admin);
    when(admin.dropTable(tablePath, false))
        .thenReturn(failedFuture(new TableNotExistException("missing")));
    FlussCatalogOperations operations = new FlussCatalogOperations(config -> connection);
    operations.initialize(
        Map.of(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS, "localhost:9092"), null, null);

    assertFalse(operations.dropTable(ident));
  }

  @Test
  void testToFlussTableChangesMapsSupportedChanges() {
    List<org.apache.fluss.metadata.TableChange> changes =
        FlussCatalogOperations.toFlussTableChanges(
            tableInfo(),
            TableChange.addColumn(
                new String[] {"country"},
                Types.StringType.get(),
                "country",
                TableChange.ColumnPosition.first()),
            TableChange.addColumn(
                new String[] {"city"},
                Types.StringType.get(),
                "city",
                TableChange.ColumnPosition.after("country")),
            TableChange.renameColumn(new String[] {"pv"}, "views"),
            TableChange.updateColumnType(new String[] {"views"}, Types.IntegerType.get()),
            TableChange.updateColumnComment(new String[] {"views"}, "views comment"),
            TableChange.updateColumnNullability(new String[] {"views"}, false),
            TableChange.updateColumnPosition(
                new String[] {"views"}, TableChange.ColumnPosition.after("city")),
            TableChange.deleteColumn(new String[] {"region"}, false),
            TableChange.setProperty("table.log.ttl", "1d"),
            TableChange.removeProperty("old.option"));

    assertEquals(10, changes.size());

    AddColumn country = assertInstanceOf(AddColumn.class, changes.get(0));
    assertEquals("country", country.getName());
    assertEquals(DataTypes.STRING(), country.getDataType());
    assertEquals("country", country.getComment());
    assertInstanceOf(First.class, country.getPosition());

    AddColumn city = assertInstanceOf(AddColumn.class, changes.get(1));
    assertEquals("city", city.getName());
    assertEquals("country", assertInstanceOf(After.class, city.getPosition()).columnName());

    RenameColumn renameColumn = assertInstanceOf(RenameColumn.class, changes.get(2));
    assertEquals("pv", renameColumn.getOldColumnName());
    assertEquals("views", renameColumn.getNewColumnName());

    ModifyColumn typeChange = assertInstanceOf(ModifyColumn.class, changes.get(3));
    assertEquals("views", typeChange.getName());
    assertEquals(DataTypes.INT(), typeChange.getDataType());
    assertEquals("page views", typeChange.getComment());
    assertNull(typeChange.getNewPosition());

    ModifyColumn commentChange = assertInstanceOf(ModifyColumn.class, changes.get(4));
    assertEquals(DataTypes.INT(), commentChange.getDataType());
    assertEquals("views comment", commentChange.getComment());
    assertNull(commentChange.getNewPosition());

    ModifyColumn nullabilityChange = assertInstanceOf(ModifyColumn.class, changes.get(5));
    assertFalse(nullabilityChange.getDataType().isNullable());
    assertEquals("views comment", nullabilityChange.getComment());
    assertNull(nullabilityChange.getNewPosition());

    ModifyColumn positionChange = assertInstanceOf(ModifyColumn.class, changes.get(6));
    assertFalse(positionChange.getDataType().isNullable());
    assertEquals("views comment", positionChange.getComment());
    assertEquals(
        "city", assertInstanceOf(After.class, positionChange.getNewPosition()).columnName());

    assertEquals("region", assertInstanceOf(DropColumn.class, changes.get(7)).getName());

    SetOption setOption = assertInstanceOf(SetOption.class, changes.get(8));
    assertEquals("table.log.ttl", setOption.getKey());
    assertEquals("1d", setOption.getValue());

    assertEquals("old.option", assertInstanceOf(ResetOption.class, changes.get(9)).getKey());
  }

  @Test
  void testDefaultAddColumnPositionMapsToFlussLastPosition() {
    List<org.apache.fluss.metadata.TableChange> changes =
        FlussCatalogOperations.toFlussTableChanges(
            tableInfo(), TableChange.addColumn(new String[] {"country"}, Types.StringType.get()));

    AddColumn addColumn = assertInstanceOf(AddColumn.class, changes.get(0));
    assertEquals("country", addColumn.getName());
    assertInstanceOf(Last.class, addColumn.getPosition());
  }

  @Test
  void testDeleteColumnIfExistsSkipsMissingColumn() {
    List<org.apache.fluss.metadata.TableChange> changes =
        FlussCatalogOperations.toFlussTableChanges(
            tableInfo(), TableChange.deleteColumn(new String[] {"missing"}, true));

    assertTrue(changes.isEmpty());
  }

  @Test
  void testRejectsUnsupportedColumnChanges() {
    UnsupportedOperationException defaultValueException =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                FlussCatalogOperations.toFlussTableChanges(
                    tableInfo(),
                    TableChange.updateColumnDefaultValue(
                        new String[] {"pv"}, Column.DEFAULT_VALUE_NOT_SET)));
    assertTrue(defaultValueException.getMessage().contains("column default values"));

    UnsupportedOperationException autoIncrementException =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                FlussCatalogOperations.toFlussTableChanges(
                    tableInfo(), TableChange.updateColumnAutoIncrement(new String[] {"pv"}, true)));
    assertTrue(autoIncrementException.getMessage().contains("auto-increment"));
  }

  @Test
  void testRejectsUnsupportedPositionsAndNestedColumns() {
    IllegalArgumentException positionException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                FlussCatalogOperations.toFlussTableChanges(
                    tableInfo(),
                    TableChange.updateColumnPosition(
                        new String[] {"pv"}, TableChange.ColumnPosition.after("missing"))));
    assertTrue(positionException.getMessage().contains("Column does not exist"));

    IllegalArgumentException nestedColumnException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                FlussCatalogOperations.toFlussTableChanges(
                    tableInfo(),
                    TableChange.addColumn(
                        new String[] {"nested", "field"}, Types.StringType.get())));
    assertTrue(nestedColumnException.getMessage().contains("top-level fields"));
  }

  @Test
  void testRejectsUnsupportedTableChanges() {
    UnsupportedOperationException tableCommentException =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                FlussCatalogOperations.toFlussTableChanges(
                    tableInfo(), TableChange.updateComment("new comment")));
    assertTrue(tableCommentException.getMessage().contains("table comment"));

    UnsupportedOperationException renameTableException =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                FlussCatalogOperations.toFlussTableChanges(
                    tableInfo(), TableChange.rename("new_table")));
    assertTrue(renameTableException.getMessage().contains("renaming tables"));
  }

  private static TableInfo tableInfo() {
    Schema schema =
        Schema.newBuilder()
            .fromColumns(
                List.of(
                    new Schema.Column("event_day", DataTypes.STRING().copy(false), "event day"),
                    new Schema.Column("region", DataTypes.STRING(), null),
                    new Schema.Column("pv", DataTypes.BIGINT(), "page views")))
            .build();
    TableDescriptor descriptor =
        TableDescriptor.builder()
            .schema(schema)
            .comment("orders comment")
            .partitionedBy(List.of("event_day"))
            .distributedBy(3, List.of("region"))
            .properties(Map.of("table.log.ttl", "1d"))
            .build();
    return TableInfo.of(TABLE_PATH, 1L, 3, descriptor, 10L, 20L);
  }

  private static FlussCatalogOperations operations(Admin admin) {
    Connection connection = mock(Connection.class);
    when(connection.getAdmin()).thenReturn(admin);
    FlussCatalogOperations operations = new FlussCatalogOperations(config -> connection);
    operations.initialize(
        Map.of(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS, "localhost:9123"), null, null);
    return operations;
  }

  private static DatabaseInfo databaseInfo(String name, String comment) {
    DatabaseDescriptor descriptor =
        DatabaseDescriptor.builder()
            .comment(comment)
            .customProperties(Map.of("owner", "fluss"))
            .build();
    return new DatabaseInfo(name, descriptor, 10L, 20L);
  }

  private static Column[] tableColumns() {
    return new Column[] {
      Column.of("event_day", Types.StringType.get(), "event day", false, false, null),
      Column.of("region", Types.StringType.get(), "region", false, false, null),
      Column.of("site_id", Types.IntegerType.get(), "site", false, false, null),
      Column.of("pv", Types.LongType.get())
    };
  }

  private static CatalogInfo catalogInfo() {
    return new CatalogInfo(
        42L,
        "catalog",
        Catalog.Type.RELATIONAL,
        "fluss",
        "catalog comment",
        Map.of(),
        null,
        Namespace.of("metalake"));
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable e) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }
}
