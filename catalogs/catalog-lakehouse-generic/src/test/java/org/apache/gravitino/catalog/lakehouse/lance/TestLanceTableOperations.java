/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.lakehouse.lance;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_CREATION_MODE;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_SCHEMA_REFRESH_MODE;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_STORAGE_OPTIONS_PREFIX;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_TABLE_DECLARED;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_TABLE_VERSION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.lance.Version;
import org.lance.index.IndexOptions;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class TestLanceTableOperations {

  @TempDir private java.nio.file.Path tempDir;

  private LanceTableOperations lanceTableOps;
  private EntityStore store;
  private ManagedSchemaOperations schemaOps;
  private IdGenerator idGenerator;

  @BeforeEach
  public void setUp() {
    store = mock(EntityStore.class);
    schemaOps = mock(ManagedSchemaOperations.class);
    idGenerator = mock(IdGenerator.class);
    lanceTableOps = spy(new LanceTableOperations(store, schemaOps, idGenerator));
  }

  @Test
  public void testCreationModeEnum() {
    // Test that CreationMode enum has expected values
    Assertions.assertEquals(3, LanceTableOperations.CreationMode.values().length);
    Assertions.assertNotNull(LanceTableOperations.CreationMode.valueOf("CREATE"));
    Assertions.assertNotNull(LanceTableOperations.CreationMode.valueOf("EXIST_OK"));
    Assertions.assertNotNull(LanceTableOperations.CreationMode.valueOf("OVERWRITE"));
  }

  @Test
  public void testCreateTableWithInvalidMode() {
    // Arrange
    NameIdentifier ident = NameIdentifier.of("catalog", "schema", "table");
    Column[] columns = new Column[] {Column.of("id", Types.IntegerType.get(), "id column")};
    String location = tempDir.resolve("table6").toString();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(Table.PROPERTY_LOCATION, location);
    properties.put(LANCE_CREATION_MODE, "INVALID_MODE");

    // Act & Assert
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            lanceTableOps.createTable(
                ident,
                columns,
                null,
                properties,
                new Transform[0],
                null,
                new SortOrder[0],
                new Index[0]));
  }

  @Test
  public void testLoadDeclaredTableSchemaFromLocation() throws Exception {
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("declared-table").toString();
    TableEntity tableEntity =
        tableEntity(
            ident,
            List.of(),
            Map.of(
                Table.PROPERTY_LOCATION,
                location,
                LANCE_TABLE_DECLARED,
                "true",
                LANCE_STORAGE_OPTIONS_PREFIX + "endpoint",
                "http://endpoint"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);
    when(idGenerator.nextId()).thenReturn(10L, 11L);
    when(store.update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<TableEntity, TableEntity> updater = invocation.getArgument(3);
              return updater.apply(tableEntity);
            });

    Dataset dataset = mock(Dataset.class);
    when(dataset.getSchema())
        .thenReturn(
            new Schema(
                List.of(
                    Field.nullable("id", new ArrowType.Int(32, true)),
                    Field.nullable("name", new ArrowType.Utf8()))));
    when(dataset.version()).thenReturn(8L);
    Mockito.doReturn(dataset)
        .when(lanceTableOps)
        .openDataset(location, Map.of("endpoint", "http://endpoint"));

    Table loadedTable =
        PrincipalUtils.doAs(new UserPrincipal("tester"), () -> lanceTableOps.loadTable(ident));

    Assertions.assertEquals(2, loadedTable.columns().length);
    Assertions.assertEquals("id", loadedTable.columns()[0].name());
    Assertions.assertEquals(Types.IntegerType.get(), loadedTable.columns()[0].dataType());
    Assertions.assertEquals("name", loadedTable.columns()[1].name());
    Assertions.assertEquals(Types.StringType.get(), loadedTable.columns()[1].dataType());
    Assertions.assertEquals("8", loadedTable.properties().get(LANCE_TABLE_VERSION));
    Assertions.assertFalse(loadedTable.properties().containsKey(LANCE_TABLE_DECLARED));
  }

  @Test
  public void testLoadTableWithStoredColumnsDoesNotReadLocation() throws Exception {
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("normal-table").toString();
    TableEntity tableEntity =
        tableEntity(
            ident,
            List.of(
                ColumnEntity.builder()
                    .withId(10L)
                    .withName("id")
                    .withDataType(Types.IntegerType.get())
                    .withPosition(0)
                    .withAuditInfo(AuditInfo.EMPTY)
                    .build()),
            Map.of(Table.PROPERTY_LOCATION, location));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);

    Table loadedTable = lanceTableOps.loadTable(ident);

    Assertions.assertEquals(1, loadedTable.columns().length);
    Assertions.assertEquals("id", loadedTable.columns()[0].name());
    verify(lanceTableOps, never()).openDataset(anyString(), any());
  }

  @Test
  public void testVersionCheckRefreshesSchemaFromLocation() throws Exception {
    lanceTableOps.setCatalogProperties(Map.of(LANCE_SCHEMA_REFRESH_MODE, "version-check"));
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("version-check-table").toString();
    TableEntity tableEntity =
        tableEntity(
            ident,
            List.of(
                ColumnEntity.builder()
                    .withId(10L)
                    .withName("old_col")
                    .withDataType(Types.StringType.get())
                    .withPosition(0)
                    .withAuditInfo(AuditInfo.EMPTY)
                    .build()),
            Map.of(Table.PROPERTY_LOCATION, location, LANCE_TABLE_VERSION, "8"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);
    when(idGenerator.nextId()).thenReturn(11L, 12L);
    when(store.update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<TableEntity, TableEntity> updater = invocation.getArgument(3);
              return updater.apply(tableEntity);
            });

    Dataset dataset = mock(Dataset.class);
    when(dataset.getSchema())
        .thenReturn(
            new Schema(
                List.of(
                    Field.nullable("id", new ArrowType.Int(32, true)),
                    Field.nullable("name", new ArrowType.Utf8()))));
    when(dataset.version()).thenReturn(9L);
    Mockito.doReturn(dataset).when(lanceTableOps).openDataset(location, Map.of());

    Table loadedTable =
        PrincipalUtils.doAs(new UserPrincipal("tester"), () -> lanceTableOps.loadTable(ident));

    Assertions.assertEquals(2, loadedTable.columns().length);
    Assertions.assertEquals("id", loadedTable.columns()[0].name());
    Assertions.assertEquals("name", loadedTable.columns()[1].name());
    Assertions.assertEquals("9", loadedTable.properties().get(LANCE_TABLE_VERSION));
  }

  @Test
  public void testVersionCheckSkipsRefreshWhenVersionIsCurrent() throws Exception {
    lanceTableOps.setCatalogProperties(Map.of(LANCE_SCHEMA_REFRESH_MODE, "version-check"));
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("current-version-table").toString();
    TableEntity tableEntity =
        tableEntity(
            ident,
            List.of(
                ColumnEntity.builder()
                    .withId(10L)
                    .withName("id")
                    .withDataType(Types.IntegerType.get())
                    .withPosition(0)
                    .withAuditInfo(AuditInfo.EMPTY)
                    .build()),
            Map.of(Table.PROPERTY_LOCATION, location, LANCE_TABLE_VERSION, "9"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);

    Dataset dataset = mock(Dataset.class);
    when(dataset.version()).thenReturn(9L);
    Mockito.doReturn(dataset).when(lanceTableOps).openDataset(location, Map.of());

    Table loadedTable = lanceTableOps.loadTable(ident);

    Assertions.assertEquals(1, loadedTable.columns().length);
    Assertions.assertEquals("id", loadedTable.columns()[0].name());
    verify(dataset, never()).getSchema();
    verify(store, never())
        .update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
  }

  @Test
  public void testHandleLanceTableChangeRespectsOrder() {
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(Map.of(Table.PROPERTY_LOCATION, "location"));

    Dataset dataset = mock(Dataset.class);
    Version version = mock(Version.class);
    when(dataset.getVersion()).thenReturn(version);
    when(version.getId()).thenReturn(7L);
    Mockito.doReturn(dataset).when(lanceTableOps).openDataset("location", Map.of());

    TableChange[] changes =
        new TableChange[] {
          TableChange.renameColumn(new String[] {"old"}, "renamed"),
          TableChange.addIndex(Index.IndexType.SCALAR, "idx_renamed", new String[][] {{"renamed"}}),
          TableChange.deleteColumn(new String[] {"renamed"}, false)
        };

    long returnedVersion = lanceTableOps.handleLanceTableChange(table, changes);
    Assertions.assertEquals(7L, returnedVersion);

    InOrder inOrder = Mockito.inOrder(dataset);
    inOrder.verify(dataset).alterColumns(anyList());
    inOrder.verify(dataset).createIndex(any(IndexOptions.class));
    inOrder.verify(dataset).dropColumns(anyList());
    inOrder.verify(dataset).getVersion();
  }

  @Test
  public void testHandleLanceTableChangeUsesCatalogStorageOptions() {
    lanceTableOps.setCatalogProperties(
        Map.of(
            LANCE_STORAGE_OPTIONS_PREFIX + "endpoint", "http://catalog-endpoint",
            LANCE_STORAGE_OPTIONS_PREFIX + "secret_access_key", "catalog-secret"));

    Table table = mock(Table.class);
    when(table.properties())
        .thenReturn(
            Map.of(
                Table.PROPERTY_LOCATION,
                "location",
                LANCE_STORAGE_OPTIONS_PREFIX + "access_key_id",
                "table-key"));

    Dataset dataset = mock(Dataset.class);
    Version version = mock(Version.class);
    when(dataset.getVersion()).thenReturn(version);
    when(version.getId()).thenReturn(9L);
    Mockito.doReturn(dataset)
        .when(lanceTableOps)
        .openDataset(
            "location",
            Map.of(
                "endpoint",
                "http://catalog-endpoint",
                "secret_access_key",
                "catalog-secret",
                "access_key_id",
                "table-key"));

    TableChange[] changes =
        new TableChange[] {TableChange.deleteColumn(new String[] {"col1"}, false)};
    long returnedVersion = lanceTableOps.handleLanceTableChange(table, changes);

    Assertions.assertEquals(9L, returnedVersion);
    Mockito.verify(dataset).dropColumns(anyList());
    Mockito.verify(dataset).getVersion();
  }

  private static TableEntity tableEntity(
      NameIdentifier ident, List<ColumnEntity> columns, Map<String, String> properties) {
    return TableEntity.builder()
        .withId(1L)
        .withName(ident.name())
        .withNamespace(ident.namespace())
        .withComment("comment")
        .withColumns(columns)
        .withProperties(properties)
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.EPOCH).build())
        .build();
  }
}
