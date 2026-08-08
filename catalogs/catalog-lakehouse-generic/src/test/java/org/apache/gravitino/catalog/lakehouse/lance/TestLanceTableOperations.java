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
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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
  public void testVariantTypeIsRejectedBeforeOverwriteMutation() {
    NameIdentifier ident = NameIdentifier.of("catalog", "schema", "table");
    Column[] columns = {Column.of("payload", Types.VariantType.get(), "variant")};
    Map<String, String> properties =
        Map.of(
            Table.PROPERTY_LOCATION,
            tempDir.resolve("variant-overwrite").toString(),
            LANCE_CREATION_MODE,
            "OVERWRITE");

    IllegalArgumentException exception =
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

    Assertions.assertTrue(exception.getMessage().contains("exact native representation"));
    verify(lanceTableOps, never()).purgeTable(ident);
    verify(lanceTableOps, never()).dropTable(ident);
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

  /**
   * Reproduces the concurrent repair-on-load race seen in {@code LanceSparkRESTServiceIT}. When two
   * loads repair the same table at once, the optimistic-locked {@code store.update} of the slower
   * one matches zero rows and {@code TableMetaService} surfaces it as {@code IOException("Failed to
   * update the entity")}. Before the CAS retry, {@code repairTableMetadata} rethrew it as a fatal
   * {@code RuntimeException} (HTTP 500) instead of tolerating the concurrent update. This test
   * asserts that the lost race is benign and load returns a usable table.
   */
  @Test
  public void testLoadTableSurvivesConcurrentRepairVersionRace() throws Exception {
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("concurrent-repair-table").toString();
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
    // The winner of the race already repaired the table to the dataset schema and version.
    TableEntity alreadyRepairedTableEntity =
        tableEntity(
            ident,
            List.of(
                ColumnEntity.builder()
                    .withId(11L)
                    .withName("id")
                    .withDataType(Types.IntegerType.get())
                    .withPosition(0)
                    .withAuditInfo(AuditInfo.EMPTY)
                    .build(),
                ColumnEntity.builder()
                    .withId(12L)
                    .withName("name")
                    .withDataType(Types.StringType.get())
                    .withPosition(1)
                    .withAuditInfo(AuditInfo.EMPTY)
                    .build()),
            Map.of(Table.PROPERTY_LOCATION, location, LANCE_TABLE_VERSION, "8"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);
    when(idGenerator.nextId()).thenReturn(10L, 11L);

    // First repair attempt loses the optimistic-lock CAS (a concurrent load already bumped the
    // version): TableMetaService surfaces exactly this IOException. The retry re-reads the winner's
    // already-repaired entity, against which the idempotent updater succeeds.
    when(store.update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any()))
        .thenThrow(new IOException("Failed to update the entity: " + ident))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<TableEntity, TableEntity> updater = invocation.getArgument(3);
              return updater.apply(alreadyRepairedTableEntity);
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

    // A lost repair race must not fail the load: the bounded CAS retry recovers and returns the
    // repaired table instead of surfacing the first conflict as RuntimeException("Failed to repair
    // table").
    Table loadedTable =
        Assertions.assertDoesNotThrow(
            () ->
                PrincipalUtils.doAs(
                    new UserPrincipal("tester"), () -> lanceTableOps.loadTable(ident)));
    Assertions.assertEquals(2, loadedTable.columns().length);
    Assertions.assertEquals("id", loadedTable.columns()[0].name());
    Assertions.assertEquals("name", loadedTable.columns()[1].name());
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
  public void testVersionCheckRefreshIsIdempotentWhenCurrentEntityWasAlreadyRepaired()
      throws Exception {
    lanceTableOps.setCatalogProperties(Map.of(LANCE_SCHEMA_REFRESH_MODE, "version-check"));
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("concurrent-version-check-table").toString();
    TableEntity staleTableEntity =
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
    TableEntity alreadyRepairedTableEntity =
        tableEntity(
            ident,
            List.of(
                ColumnEntity.builder()
                    .withId(11L)
                    .withName("id")
                    .withDataType(Types.IntegerType.get())
                    .withPosition(0)
                    .withAuditInfo(AuditInfo.EMPTY)
                    .build(),
                ColumnEntity.builder()
                    .withId(12L)
                    .withName("name")
                    .withDataType(Types.StringType.get())
                    .withPosition(1)
                    .withAuditInfo(AuditInfo.EMPTY)
                    .build()),
            Map.of(Table.PROPERTY_LOCATION, location, LANCE_TABLE_VERSION, "9"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(staleTableEntity);
    when(store.update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<TableEntity, TableEntity> updater = invocation.getArgument(3);
              return updater.apply(alreadyRepairedTableEntity);
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
  public void testAlterTablePersistsUpdatedLanceVersion() throws Exception {
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("alter-table").toString();
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
            Map.of(Table.PROPERTY_LOCATION, location, LANCE_TABLE_VERSION, "8"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);
    AtomicReference<TableEntity> storedTable = new AtomicReference<>();
    when(store.update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<TableEntity, TableEntity> updater = invocation.getArgument(3);
              TableEntity updated = updater.apply(tableEntity);
              storedTable.set(updated);
              return updated;
            });

    Dataset dataset = mock(Dataset.class);
    Version version = mock(Version.class);
    when(dataset.getVersion()).thenReturn(version);
    when(version.getId()).thenReturn(9L);
    Mockito.doReturn(dataset).when(lanceTableOps).openDataset(location, Map.of());

    Table alteredTable =
        PrincipalUtils.doAs(
            new UserPrincipal("tester"),
            () ->
                lanceTableOps.alterTable(
                    ident, TableChange.deleteColumn(new String[] {"id"}, false)));

    Assertions.assertEquals("9", alteredTable.properties().get(LANCE_TABLE_VERSION));
    Assertions.assertEquals("9", storedTable.get().properties().get(LANCE_TABLE_VERSION));
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

  @Test
  public void testVersionCheckSkipsSchemaReadForEmptySchemaWhenVersionUnchanged() throws Exception {
    lanceTableOps.setCatalogProperties(Map.of(LANCE_SCHEMA_REFRESH_MODE, "version-check"));
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("empty-version-check").toString();
    // Table has empty columns but a known stored version (confirmed-empty state)
    TableEntity tableEntity =
        tableEntity(
            ident, List.of(), Map.of(Table.PROPERTY_LOCATION, location, LANCE_TABLE_VERSION, "5"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);

    Dataset dataset = mock(Dataset.class);
    when(dataset.version()).thenReturn(5L);
    Mockito.doReturn(dataset).when(lanceTableOps).openDataset(location, Map.of());

    Table loaded = lanceTableOps.loadTable(ident);

    Assertions.assertEquals(0, loaded.columns().length);
    // version unchanged — schema read must be skipped even though columns are empty
    verify(dataset, never()).getSchema();
    verify(store, never())
        .update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
  }

  @Test
  public void testEmptyDatasetRecordsVersionOnFirstLoad() throws Exception {
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("empty-dataset-first").toString();
    TableEntity tableEntity =
        tableEntity(ident, List.of(), Map.of(Table.PROPERTY_LOCATION, location));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);
    when(store.update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<TableEntity, TableEntity> updater = invocation.getArgument(3);
              return updater.apply(tableEntity);
            });

    Dataset dataset = mock(Dataset.class);
    when(dataset.getSchema()).thenReturn(new Schema(List.of()));
    when(dataset.version()).thenReturn(3L);
    Mockito.doReturn(dataset).when(lanceTableOps).openDataset(location, Map.of());

    Table loaded =
        PrincipalUtils.doAs(new UserPrincipal("tester"), () -> lanceTableOps.loadTable(ident));

    Assertions.assertEquals(0, loaded.columns().length);
    Assertions.assertEquals("3", loaded.properties().get(LANCE_TABLE_VERSION));
    verify(store).update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
  }

  @Test
  public void testEmptyDatasetSkipsOpenWhenVersionAlreadyRecorded() throws Exception {
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("empty-dataset-second").toString();
    // Simulate the table after first-load recorded lance.version=3 but columns still empty
    TableEntity tableEntity =
        tableEntity(
            ident, List.of(), Map.of(Table.PROPERTY_LOCATION, location, LANCE_TABLE_VERSION, "3"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);

    Table loaded = lanceTableOps.loadTable(ident);

    Assertions.assertEquals(0, loaded.columns().length);
    verify(lanceTableOps, never()).openDataset(anyString(), any());
    verify(store, never())
        .update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
  }

  // ---------------------------------------------------------------------------
  //  VERSION_CHECK: dataset becomes empty while stored columns are non-empty
  // ---------------------------------------------------------------------------

  @Test
  public void testVersionCheckClearsStaleColumnsWhenDatasetBecomesEmpty() throws Exception {
    // Regression test for: VERSION_CHECK + stored columns non-empty + dataset schema becomes empty.
    // recordCheckedEmptyVersion must clear the stale stored columns (not preserve them via
    // current.columns()), otherwise the version sentinel locks in permanently stale metadata.
    lanceTableOps.setCatalogProperties(Map.of(LANCE_SCHEMA_REFRESH_MODE, "version-check"));
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("stale-columns-empty-dataset").toString();
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
                    .build(),
                ColumnEntity.builder()
                    .withId(11L)
                    .withName("name")
                    .withDataType(Types.StringType.get())
                    .withPosition(1)
                    .withAuditInfo(AuditInfo.EMPTY)
                    .build()),
            Map.of(Table.PROPERTY_LOCATION, location, LANCE_TABLE_VERSION, "8"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);
    when(store.update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<TableEntity, TableEntity> updater = invocation.getArgument(3);
              return updater.apply(tableEntity);
            });

    Dataset dataset = mock(Dataset.class);
    when(dataset.getSchema()).thenReturn(new Schema(List.of()));
    when(dataset.version()).thenReturn(9L);
    Mockito.doReturn(dataset).when(lanceTableOps).openDataset(location, Map.of());

    Table loaded =
        PrincipalUtils.doAs(new UserPrincipal("tester"), () -> lanceTableOps.loadTable(ident));

    // Stale columns must be cleared; storing [id, name] here would permanently lock them in.
    Assertions.assertEquals(0, loaded.columns().length);
    // The new dataset version must be persisted so future VERSION_CHECK loads see the match.
    Assertions.assertEquals("9", loaded.properties().get(LANCE_TABLE_VERSION));
  }

  @Test
  public void testVersionCheckStaleColumnsAreNotReturnedOnSubsequentLoad() throws Exception {
    // After the fix: once stale columns are cleared and version=9 is recorded, the next loadTable
    // must early-return with empty columns (not re-open the dataset and not return stale data).
    lanceTableOps.setCatalogProperties(Map.of(LANCE_SCHEMA_REFRESH_MODE, "version-check"));
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("subsequent-empty-load").toString();
    // Simulate the store state after the first load cleared stale columns.
    TableEntity tableEntity =
        tableEntity(
            ident, List.of(), Map.of(Table.PROPERTY_LOCATION, location, LANCE_TABLE_VERSION, "9"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);

    Dataset dataset = mock(Dataset.class);
    when(dataset.version()).thenReturn(9L);
    Mockito.doReturn(dataset).when(lanceTableOps).openDataset(location, Map.of());

    Table loaded = lanceTableOps.loadTable(ident);

    Assertions.assertEquals(0, loaded.columns().length);
    // Version matched: schema read must be skipped entirely.
    verify(dataset, never()).getSchema();
    verify(store, never())
        .update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
  }

  // ---------------------------------------------------------------------------
  //  VERSION_CHECK: empty stored columns
  // ---------------------------------------------------------------------------

  @Test
  public void testVersionCheckFirstLoadEmptyStoredColumnsEmptyDataset() throws Exception {
    // VERSION_CHECK + no stored version + empty stored columns + dataset is also empty.
    // Should open dataset, read schema (empty), and record version without creating any columns.
    lanceTableOps.setCatalogProperties(Map.of(LANCE_SCHEMA_REFRESH_MODE, "version-check"));
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("vc-empty-first-load").toString();
    TableEntity tableEntity =
        tableEntity(ident, List.of(), Map.of(Table.PROPERTY_LOCATION, location));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);
    when(store.update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<TableEntity, TableEntity> updater = invocation.getArgument(3);
              return updater.apply(tableEntity);
            });

    Dataset dataset = mock(Dataset.class);
    when(dataset.getSchema()).thenReturn(new Schema(List.of()));
    when(dataset.version()).thenReturn(5L);
    Mockito.doReturn(dataset).when(lanceTableOps).openDataset(location, Map.of());

    Table loaded =
        PrincipalUtils.doAs(new UserPrincipal("tester"), () -> lanceTableOps.loadTable(ident));

    Assertions.assertEquals(0, loaded.columns().length);
    Assertions.assertEquals("5", loaded.properties().get(LANCE_TABLE_VERSION));
    verify(store).update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
  }

  @Test
  public void testVersionCheckEmptyStoredColumnsVersionBumpedDatasetStillEmpty() throws Exception {
    // VERSION_CHECK + stored empty columns with version=5 + dataset bumped to version=6 (still
    // empty). Should detect version change, read schema (empty), and update version to 6.
    lanceTableOps.setCatalogProperties(Map.of(LANCE_SCHEMA_REFRESH_MODE, "version-check"));
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("vc-empty-version-bump").toString();
    TableEntity tableEntity =
        tableEntity(
            ident, List.of(), Map.of(Table.PROPERTY_LOCATION, location, LANCE_TABLE_VERSION, "5"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);
    when(store.update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<TableEntity, TableEntity> updater = invocation.getArgument(3);
              return updater.apply(tableEntity);
            });

    Dataset dataset = mock(Dataset.class);
    when(dataset.getSchema()).thenReturn(new Schema(List.of()));
    when(dataset.version()).thenReturn(6L);
    Mockito.doReturn(dataset).when(lanceTableOps).openDataset(location, Map.of());

    Table loaded =
        PrincipalUtils.doAs(new UserPrincipal("tester"), () -> lanceTableOps.loadTable(ident));

    Assertions.assertEquals(0, loaded.columns().length);
    Assertions.assertEquals("6", loaded.properties().get(LANCE_TABLE_VERSION));
  }

  // ---------------------------------------------------------------------------
  //  DECLARED_AND_EMPTY: non-declared empty stored columns with real dataset schema
  // ---------------------------------------------------------------------------

  @Test
  public void testDeclaredAndEmptyRepairsNonDeclaredEmptyStoredColumnsFromRealDataset()
      throws Exception {
    // DECLARED_AND_EMPTY + non-declared table + empty stored columns (no version) + dataset has a
    // real schema. loadTable should open the dataset, read the schema, and persist the columns.
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("dae-empty-stored-real-schema").toString();
    TableEntity tableEntity =
        tableEntity(ident, List.of(), Map.of(Table.PROPERTY_LOCATION, location));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);
    when(idGenerator.nextId()).thenReturn(20L, 21L);
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
                    Field.nullable("col_a", new ArrowType.Int(64, true)),
                    Field.nullable("col_b", new ArrowType.Bool()))));
    when(dataset.version()).thenReturn(7L);
    Mockito.doReturn(dataset).when(lanceTableOps).openDataset(location, Map.of());

    Table loaded =
        PrincipalUtils.doAs(new UserPrincipal("tester"), () -> lanceTableOps.loadTable(ident));

    Assertions.assertEquals(2, loaded.columns().length);
    Assertions.assertEquals("col_a", loaded.columns()[0].name());
    Assertions.assertEquals("col_b", loaded.columns()[1].name());
    Assertions.assertEquals("7", loaded.properties().get(LANCE_TABLE_VERSION));
  }

  // ---------------------------------------------------------------------------
  //  Edge cases: no location, dataset open failure
  // ---------------------------------------------------------------------------

  @Test
  public void testLoadTableWithNoLocationReturnsStoredMetadata() throws Exception {
    // A table without a PROPERTY_LOCATION must return stored metadata immediately without
    // attempting to open any dataset, even in VERSION_CHECK mode.
    lanceTableOps.setCatalogProperties(Map.of(LANCE_SCHEMA_REFRESH_MODE, "version-check"));
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    // Declared table with empty columns and no location triggers the schema-refresh branch.
    TableEntity tableEntity = tableEntity(ident, List.of(), Map.of(LANCE_TABLE_DECLARED, "true"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);

    Table loaded = lanceTableOps.loadTable(ident);

    Assertions.assertEquals(0, loaded.columns().length);
    verify(lanceTableOps, never()).openDataset(anyString(), any());
    verify(store, never())
        .update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
  }

  @Test
  public void testLoadTableFallsBackToStoredMetadataWhenDatasetOpenFails() throws Exception {
    // If the Lance dataset cannot be opened (e.g. storage not accessible), loadTable must return
    // the stored metadata rather than propagating the exception.
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("broken-dataset").toString();
    TableEntity tableEntity =
        tableEntity(ident, List.of(), Map.of(Table.PROPERTY_LOCATION, location));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);
    Mockito.doThrow(new RuntimeException("storage unavailable"))
        .when(lanceTableOps)
        .openDataset(eq(location), any());

    Table loaded = lanceTableOps.loadTable(ident);

    Assertions.assertEquals(0, loaded.columns().length);
    verify(store, never())
        .update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
  }

  // ---------------------------------------------------------------------------
  //  Declared table: dataset open but no recordCheckedEmptyVersion
  // ---------------------------------------------------------------------------

  @Test
  public void testDeclaredTableWithEmptyDatasetDoesNotRecordVersion() throws Exception {
    // Declared tables use lance.declared=true as the "not yet written" signal. When the dataset is
    // empty, the caller should NOT record the checked version (that would advance the version
    // sentinel while the declared flag is still present). The returned table must stay unchanged.
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("declared-empty-dataset").toString();
    TableEntity tableEntity =
        tableEntity(
            ident,
            List.of(),
            Map.of(Table.PROPERTY_LOCATION, location, LANCE_TABLE_DECLARED, "true"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);

    Dataset dataset = mock(Dataset.class);
    when(dataset.getSchema()).thenReturn(new Schema(List.of()));
    when(dataset.version()).thenReturn(3L);
    Mockito.doReturn(dataset).when(lanceTableOps).openDataset(location, Map.of());

    Table loaded =
        PrincipalUtils.doAs(new UserPrincipal("tester"), () -> lanceTableOps.loadTable(ident));

    // lance.declared is still present (no schema written yet), no version recorded.
    Assertions.assertTrue(
        Boolean.parseBoolean(loaded.properties().get(LANCE_TABLE_DECLARED)),
        "lance.declared must still be set");
    Assertions.assertNull(
        loaded.properties().get(LANCE_TABLE_VERSION), "lance.version must not be recorded");
    verify(store, never())
        .update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
  }

  // ---------------------------------------------------------------------------
  //  VERSION_CHECK: declared table always refreshes even when version matches
  // ---------------------------------------------------------------------------

  @Test
  public void testVersionCheckDeclaredTableAlwaysRefreshesDespiteVersionMatch() throws Exception {
    // In VERSION_CHECK mode the early-return is gated on !declaredOnly, so declared tables must
    // always open the dataset and repair their schema regardless of the stored lance.version.
    lanceTableOps.setCatalogProperties(Map.of(LANCE_SCHEMA_REFRESH_MODE, "version-check"));
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    String location = tempDir.resolve("vc-declared-version-match").toString();
    // Stored version already matches the dataset version.
    TableEntity tableEntity =
        tableEntity(
            ident,
            List.of(),
            Map.of(
                Table.PROPERTY_LOCATION,
                location,
                LANCE_TABLE_DECLARED,
                "true",
                LANCE_TABLE_VERSION,
                "9"));
    when(store.get(eq(ident), eq(Entity.EntityType.TABLE), eq(TableEntity.class)))
        .thenReturn(tableEntity);
    when(idGenerator.nextId()).thenReturn(30L);
    when(store.update(eq(ident), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<TableEntity, TableEntity> updater = invocation.getArgument(3);
              return updater.apply(tableEntity);
            });

    Dataset dataset = mock(Dataset.class);
    when(dataset.getSchema())
        .thenReturn(new Schema(List.of(Field.nullable("id", new ArrowType.Int(32, true)))));
    when(dataset.version()).thenReturn(9L);
    Mockito.doReturn(dataset).when(lanceTableOps).openDataset(location, Map.of());

    Table loaded =
        PrincipalUtils.doAs(new UserPrincipal("tester"), () -> lanceTableOps.loadTable(ident));

    // Schema must be read and persisted — declared tables bypass the version early-return.
    Assertions.assertEquals(1, loaded.columns().length);
    Assertions.assertEquals("id", loaded.columns()[0].name());
    Assertions.assertFalse(
        loaded.properties().containsKey(LANCE_TABLE_DECLARED),
        "lance.declared must be removed after schema is written");
    verify(dataset).getSchema();
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
