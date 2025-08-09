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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.apache.gravitino.Entity.EntityType.SCHEMA;
import static org.apache.gravitino.Entity.EntityType.TABLE;
import static org.apache.gravitino.StringIdentifier.ID_KEY;
import static org.apache.gravitino.TestBasePropertiesMetadata.COMMENT_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.TestColumn;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.connector.TestCatalogOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestTableOperationDispatcher extends TestOperationDispatcher {
  static TableOperationDispatcher tableOperationDispatcher;
  static SchemaOperationDispatcher schemaOperationDispatcher;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);
    tableOperationDispatcher =
        new TableOperationDispatcher(catalogManager, entityStore, idGenerator);

    Config config = mock(Config.class);
    doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "schemaDispatcher", schemaOperationDispatcher, true);
  }

  @Test
  public void testCreateAndListTables() throws IOException {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema41");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent1 = NameIdentifier.of(tableNs, "table1");
    Column[] columns =
        new Column[] {
          TestColumn.builder()
              .withName("col1")
              .withPosition(0)
              .withType(Types.StringType.get())
              .build(),
          TestColumn.builder()
              .withName("col2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .build()
        };

    Table table1 =
        tableOperationDispatcher.createTable(
            tableIdent1, columns, "comment", props, new Transform[0]);
    Assertions.assertEquals("table1", table1.name());
    Assertions.assertEquals("comment", table1.comment());
    testProperties(props, table1.properties());
    Assertions.assertEquals(0, table1.partitioning().length);
    Assertions.assertArrayEquals(columns, table1.columns());

    // Test required table properties exception
    Map<String, String> illegalTableProperties =
        new HashMap<String, String>() {
          {
            put("k2", "v2");
          }
        };
    testPropertyException(
        () ->
            tableOperationDispatcher.createTable(
                tableIdent1, columns, "comment", illegalTableProperties, new Transform[0]),
        "Properties or property prefixes are required and must be set");

    // Test reserved table properties exception
    illegalTableProperties.put(COMMENT_KEY, "table comment");
    illegalTableProperties.put(ID_KEY, "gravitino.v1.uidfdsafdsa");
    testPropertyException(
        () ->
            tableOperationDispatcher.createTable(
                tableIdent1, columns, "comment", illegalTableProperties, new Transform[0]),
        "Properties or property prefixes are reserved and cannot be set",
        "comment",
        "gravitino.identifier");

    // Check if the Table entity is stored in the EntityStore
    TableEntity tableEntity = entityStore.get(tableIdent1, TABLE, TableEntity.class);
    Assertions.assertNotNull(tableEntity);
    Assertions.assertEquals("table1", tableEntity.name());

    Assertions.assertFalse(table1.properties().containsKey(ID_KEY));

    Optional<NameIdentifier> ident1 =
        Arrays.stream(tableOperationDispatcher.listTables(tableNs))
            .filter(s -> s.name().equals("table1"))
            .findFirst();
    Assertions.assertTrue(ident1.isPresent());

    // Test when the entity store failed to put the table entity
    doThrow(new IOException()).when(entityStore).put(any(), anyBoolean());
    NameIdentifier tableIdent2 = NameIdentifier.of(tableNs, "table2");
    Table table2 =
        tableOperationDispatcher.createTable(
            tableIdent2, columns, "comment", props, new Transform[0]);

    // Check if the created Schema's field values are correct
    Assertions.assertEquals("table2", table2.name());
    Assertions.assertEquals("comment", table2.comment());
    testProperties(props, table2.properties());

    // Check if the Table entity is stored in the EntityStore
    Assertions.assertFalse(entityStore.exists(tableIdent2, TABLE));
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> entityStore.get(tableIdent2, TABLE, TableEntity.class));

    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", table2.auditInfo().creator());
  }

  @Test
  public void testCreateAndLoadTable() throws IOException {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema51");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent1 = NameIdentifier.of(tableNs, "table11");
    Column[] columns =
        new Column[] {
          TestColumn.builder()
              .withName("col1")
              .withPosition(0)
              .withType(Types.StringType.get())
              .build(),
          TestColumn.builder()
              .withName("col2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .build()
        };

    Table table1 =
        tableOperationDispatcher.createTable(
            tableIdent1, columns, "comment", props, new Transform[0]);
    Table loadedTable1 = tableOperationDispatcher.loadTable(tableIdent1);
    Assertions.assertEquals(table1.name(), loadedTable1.name());
    Assertions.assertEquals(table1.comment(), loadedTable1.comment());
    testProperties(table1.properties(), loadedTable1.properties());
    Assertions.assertEquals(0, loadedTable1.partitioning().length);
    Assertions.assertArrayEquals(table1.columns(), loadedTable1.columns());
    // Audit info is gotten from the entity store
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, loadedTable1.auditInfo().creator());

    // Case 2: Test if the table entity is not found in the entity store
    reset(entityStore);
    entityStore.delete(tableIdent1, TABLE);
    entityStore.delete(NameIdentifier.of(tableNs.levels()), SCHEMA);
    doThrow(new NoSuchEntityException(""))
        .when(entityStore)
        .get(any(), eq(Entity.EntityType.TABLE), any());
    Table loadedTable2 = tableOperationDispatcher.loadTable(tableIdent1);
    // Succeed to import the topic entity
    Assertions.assertTrue(entityStore.exists(NameIdentifier.of(tableNs.levels()), SCHEMA));
    Assertions.assertTrue(entityStore.exists(tableIdent1, TABLE));
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedTable2.auditInfo().creator());

    // Case 3: Test if the entity store is failed to get the table entity
    reset(entityStore);
    entityStore.delete(tableIdent1, TABLE);
    entityStore.delete(NameIdentifier.of(tableNs.levels()), SCHEMA);
    doThrow(new IOException()).when(entityStore).get(any(), eq(Entity.EntityType.TABLE), any());
    Table loadedTable3 = tableOperationDispatcher.loadTable(tableIdent1);
    // Succeed to import the topic entity
    Assertions.assertTrue(entityStore.exists(NameIdentifier.of(tableNs.levels()), SCHEMA));
    Assertions.assertTrue(entityStore.exists(tableIdent1, TABLE));
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedTable3.auditInfo().creator());

    // Case 4: Test if the table entity is not matched
    reset(entityStore);
    TableEntity tableEntity =
        TableEntity.builder()
            .withId(1L)
            .withName("table11")
            .withNamespace(tableNs)
            .withAuditInfo(
                AuditInfo.builder().withCreator("gravitino").withCreateTime(Instant.now()).build())
            .build();
    doReturn(tableEntity).when(entityStore).get(any(), eq(Entity.EntityType.TABLE), any());
    Table loadedTable4 = tableOperationDispatcher.loadTable(tableIdent1);
    // Succeed to import the topic entity
    reset(entityStore);
    TableEntity tableImportedEntity = entityStore.get(tableIdent1, TABLE, TableEntity.class);
    Assertions.assertEquals("test", tableImportedEntity.auditInfo().creator());
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedTable4.auditInfo().creator());

    // Case 5: Test loading table with duplicate columns
    NameIdentifier tableIdentDup = NameIdentifier.of(tableNs, "table_dup_cols");
    Column[] dupColumns =
        new Column[] {
          TestColumn.builder()
              .withName("col1")
              .withPosition(0)
              .withType(Types.StringType.get())
              .build(),
          TestColumn.builder()
              .withName("col2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .build(),
          TestColumn.builder()
              .withName("col2") // Duplicate column
              .withPosition(2)
              .withType(Types.IntegerType.get())
              .build()
        };

    TestCatalog testCatalog =
        (TestCatalog) catalogManager.loadCatalog(NameIdentifier.of(metalake, catalog));
    TestCatalogOperations catalogOps = (TestCatalogOperations) testCatalog.ops();

    // Create table with duplicate columns directly in catalog
    catalogOps.createTable(
        tableIdentDup,
        dupColumns,
        "table with dup cols",
        props,
        new Transform[0],
        null,
        null,
        null);

    // Load the table - should not throw exception due to duplicate columns
    Table loadedTableDup = tableOperationDispatcher.loadTable(tableIdentDup);
    Assertions.assertNotNull(loadedTableDup);

    // Verify the table entity in store has correct columns (duplicates handled)
    TableEntity tableEntityDup = entityStore.get(tableIdentDup, TABLE, TableEntity.class);
    // Should have only 2 unique columns (col1 and col2)
    Assertions.assertEquals(2, tableEntityDup.columns().size());
    Set<String> uniqueColNames =
        tableEntityDup.columns().stream().map(ColumnEntity::name).collect(Collectors.toSet());
    Assertions.assertEquals(2, uniqueColNames.size());
    Assertions.assertTrue(uniqueColNames.contains("col1"));
    Assertions.assertTrue(uniqueColNames.contains("col2"));
  }

  @Test
  public void testCreateAndAlterTable() throws IOException {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema61");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent = NameIdentifier.of(tableNs, "table21");
    Column[] columns =
        new Column[] {
          TestColumn.builder()
              .withName("col1")
              .withPosition(0)
              .withType(Types.StringType.get())
              .build(),
          TestColumn.builder()
              .withName("col2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .build()
        };

    Table table =
        tableOperationDispatcher.createTable(
            tableIdent, columns, "comment", props, new Transform[0]);

    // Test immutable table properties
    TableChange[] illegalChange =
        new TableChange[] {TableChange.setProperty(COMMENT_KEY, "new comment")};
    testPropertyException(
        () -> tableOperationDispatcher.alterTable(tableIdent, illegalChange),
        "Property comment is immutable or reserved, cannot be set");

    TableChange[] changes =
        new TableChange[] {TableChange.setProperty("k3", "v3"), TableChange.removeProperty("k1")};

    Table alteredTable = tableOperationDispatcher.alterTable(tableIdent, changes);
    Assertions.assertEquals(table.name(), alteredTable.name());
    Assertions.assertEquals(table.comment(), alteredTable.comment());
    Map<String, String> expectedProps = ImmutableMap.of("k2", "v2", "k3", "v3");
    testProperties(expectedProps, alteredTable.properties());
    // Audit info is gotten from gravitino entity store
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredTable.auditInfo().creator());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredTable.auditInfo().lastModifier());

    // Case 2: Test if the table entity is not found in the entity store
    reset(entityStore);
    doThrow(new NoSuchEntityException("")).when(entityStore).update(any(), any(), any(), any());
    Table alteredTable2 = tableOperationDispatcher.alterTable(tableIdent, changes);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", alteredTable2.auditInfo().creator());
    Assertions.assertEquals("test", alteredTable2.auditInfo().lastModifier());

    // Case 3: Test if the entity store is failed to update the table entity
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).update(any(), any(), any(), any());
    Table alteredTable3 = tableOperationDispatcher.alterTable(tableIdent, changes);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", alteredTable3.auditInfo().creator());
    Assertions.assertEquals("test", alteredTable3.auditInfo().lastModifier());

    // Case 4: Test if the table entity is not matched
    reset(entityStore);
    TableEntity unmatchedEntity =
        TableEntity.builder()
            .withId(1L)
            .withName("table21")
            .withNamespace(tableNs)
            .withAuditInfo(
                AuditInfo.builder().withCreator("gravitino").withCreateTime(Instant.now()).build())
            .build();
    doReturn(unmatchedEntity).when(entityStore).update(any(), any(), any(), any());
    Table alteredTable4 = tableOperationDispatcher.alterTable(tableIdent, changes);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", alteredTable4.auditInfo().creator());
    Assertions.assertEquals("test", alteredTable4.auditInfo().lastModifier());
  }

  @Test
  public void testCreateAndDropTable() throws IOException {
    NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, "schema71", "table31");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Column[] columns =
        new Column[] {
          TestColumn.builder()
              .withName("col1")
              .withPosition(0)
              .withType(Types.StringType.get())
              .build(),
          TestColumn.builder()
              .withName("col2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .build()
        };

    schemaOperationDispatcher.createSchema(
        NameIdentifier.of(tableIdent.namespace().levels()), "comment", props);

    tableOperationDispatcher.createTable(tableIdent, columns, "comment", props, new Transform[0]);

    boolean dropped = tableOperationDispatcher.dropTable(tableIdent);
    Assertions.assertTrue(dropped);
    Assertions.assertFalse(tableOperationDispatcher.dropTable(tableIdent));

    // Test if the entity store is failed to drop the table entity
    tableOperationDispatcher.createTable(tableIdent, columns, "comment", props, new Transform[0]);
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).delete(any(), any(), anyBoolean());
    Assertions.assertThrows(
        RuntimeException.class, () -> tableOperationDispatcher.dropTable(tableIdent));
  }

  @Test
  public void testCreateTableNeedImportingSchema() throws IOException {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema181");
    NameIdentifier tableIdent = NameIdentifier.of(tableNs, "topic81");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    TestCatalog testCatalog =
        (TestCatalog) catalogManager.loadCatalog(NameIdentifier.of(metalake, catalog));
    TestCatalogOperations testCatalogOperations = (TestCatalogOperations) testCatalog.ops();
    testCatalogOperations.createSchema(
        NameIdentifier.of(tableNs.levels()), "", Collections.emptyMap());
    Column[] columns =
        new Column[] {
          TestColumn.builder()
              .withName("col1")
              .withPosition(0)
              .withType(Types.StringType.get())
              .build(),
          TestColumn.builder()
              .withName("col2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .build()
        };
    tableOperationDispatcher.createTable(tableIdent, columns, "comment", props);
    Assertions.assertTrue(entityStore.exists(NameIdentifier.of(tableNs.levels()), SCHEMA));
    Assertions.assertTrue(entityStore.exists(tableIdent, TABLE));
  }

  @Test
  public void testCreateAndLoadTableWithColumn() throws IOException {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema91");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent = NameIdentifier.of(tableNs, "table41");
    Column[] columns =
        new Column[] {
          TestColumn.builder()
              .withName("col1")
              .withPosition(0)
              .withType(Types.StringType.get())
              .withComment("comment1")
              .withNullable(true)
              .withAutoIncrement(true)
              .withDefaultValue(Literals.stringLiteral("1"))
              .build(),
          TestColumn.builder()
              .withName("col2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .withComment("comment2")
              .withNullable(false)
              .withAutoIncrement(false)
              .withDefaultValue(Literals.stringLiteral("2"))
              .build()
        };

    Table table1 =
        tableOperationDispatcher.createTable(
            tableIdent, columns, "comment", props, new Transform[0]);

    Table loadedTable1 = tableOperationDispatcher.loadTable(tableIdent);
    Assertions.assertEquals(table1.name(), loadedTable1.name());
    Assertions.assertEquals(table1.comment(), loadedTable1.comment());
    testProperties(table1.properties(), loadedTable1.properties());
    testColumns(columns, loadedTable1.columns());

    // The columns from table and table entity should be the same after creating.
    TableEntity tableEntity = entityStore.get(tableIdent, TABLE, TableEntity.class);
    Assertions.assertNotNull(tableEntity);
    Assertions.assertEquals("table41", tableEntity.name());
    testColumnAndColumnEntities(columns, tableEntity.columns());

    // Test if the column from table is not matched with the column from table entity
    TestCatalog testCatalog =
        (TestCatalog) catalogManager.loadCatalog(NameIdentifier.of(metalake, catalog));
    TestCatalogOperations testCatalogOperations = (TestCatalogOperations) testCatalog.ops();

    // 1. Update the existing column
    Table alteredTable2 =
        testCatalogOperations.alterTable(
            tableIdent, TableChange.renameColumn(new String[] {"col1"}, "col3"));
    Table loadedTable2 = tableOperationDispatcher.loadTable(tableIdent);
    testColumns(alteredTable2.columns(), loadedTable2.columns());

    // columns in table entity should be updated to match the columns in table
    TableEntity tableEntity2 = entityStore.get(tableIdent, TABLE, TableEntity.class);
    testColumnAndColumnEntities(alteredTable2.columns(), tableEntity2.columns());

    // 2. Add a new column
    Table alteredTable3 =
        testCatalogOperations.alterTable(
            tableIdent,
            TableChange.addColumn(
                new String[] {"col4"},
                Types.StringType.get(),
                "comment4",
                TableChange.ColumnPosition.first(),
                true,
                true,
                Literals.stringLiteral("4")));

    Table loadedTable3 = tableOperationDispatcher.loadTable(tableIdent);
    testColumns(alteredTable3.columns(), loadedTable3.columns());

    TableEntity tableEntity3 = entityStore.get(tableIdent, TABLE, TableEntity.class);
    testColumnAndColumnEntities(alteredTable3.columns(), tableEntity3.columns());

    // 3. Drop a column
    Table alteredTable4 =
        testCatalogOperations.alterTable(
            tableIdent, TableChange.deleteColumn(new String[] {"col2"}, true));
    Table loadedTable4 = tableOperationDispatcher.loadTable(tableIdent);
    testColumns(alteredTable4.columns(), loadedTable4.columns());

    TableEntity tableEntity4 = entityStore.get(tableIdent, TABLE, TableEntity.class);
    testColumnAndColumnEntities(alteredTable4.columns(), tableEntity4.columns());

    // No column for the table
    Table alteredTable5 =
        testCatalogOperations.alterTable(
            tableIdent,
            TableChange.deleteColumn(new String[] {"col3"}, true),
            TableChange.deleteColumn(new String[] {"col4"}, true));
    Table loadedTable5 = tableOperationDispatcher.loadTable(tableIdent);
    Assertions.assertEquals(0, alteredTable5.columns().length);
    Assertions.assertEquals(0, loadedTable5.columns().length);

    TableEntity tableEntity5 = entityStore.get(tableIdent, TABLE, TableEntity.class);
    Assertions.assertEquals(0, tableEntity5.columns().size());

    // Re-add columns to the table
    Table alteredTable6 =
        testCatalogOperations.alterTable(
            tableIdent,
            TableChange.addColumn(
                new String[] {"col5"},
                Types.StringType.get(),
                "comment5",
                TableChange.ColumnPosition.first(),
                true,
                true,
                Literals.stringLiteral("5")),
            TableChange.addColumn(
                new String[] {"col6"},
                Types.StringType.get(),
                "comment6",
                TableChange.ColumnPosition.first(),
                false,
                false,
                Literals.stringLiteral("2")));
    Table loadedTable6 = tableOperationDispatcher.loadTable(tableIdent);
    testColumns(alteredTable6.columns(), loadedTable6.columns());

    TableEntity tableEntity6 = entityStore.get(tableIdent, TABLE, TableEntity.class);
    testColumnAndColumnEntities(alteredTable6.columns(), tableEntity6.columns());
  }

  @Test
  public void testCreateAndAlterTableWithColumn() throws IOException {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema101");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent = NameIdentifier.of(tableNs, "table51");
    Column[] columns =
        new Column[] {
          TestColumn.builder()
              .withName("col1")
              .withPosition(0)
              .withType(Types.StringType.get())
              .withComment("comment1")
              .withNullable(true)
              .withAutoIncrement(true)
              .withDefaultValue(Literals.stringLiteral("1"))
              .build(),
          TestColumn.builder()
              .withName("col2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .withComment("comment2")
              .withNullable(false)
              .withAutoIncrement(false)
              .withDefaultValue(Literals.stringLiteral("2"))
              .build()
        };

    Table table1 =
        tableOperationDispatcher.createTable(
            tableIdent, columns, "comment", props, new Transform[0]);
    testColumns(columns, table1.columns());

    // 1. Rename the column
    Table alteredTable1 =
        tableOperationDispatcher.alterTable(
            tableIdent, TableChange.renameColumn(new String[] {"col1"}, "col3"));
    Column[] expectedColumns =
        new Column[] {
          TestColumn.builder()
              .withName("col3")
              .withPosition(0)
              .withType(Types.StringType.get())
              .withComment("comment1")
              .withNullable(true)
              .withAutoIncrement(true)
              .withDefaultValue(Literals.stringLiteral("1"))
              .build(),
          TestColumn.builder()
              .withName("col2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .withComment("comment2")
              .withNullable(false)
              .withAutoIncrement(false)
              .withDefaultValue(Literals.stringLiteral("2"))
              .build()
        };
    testColumns(expectedColumns, alteredTable1.columns());

    TableEntity tableEntity1 = entityStore.get(tableIdent, TABLE, TableEntity.class);
    testColumnAndColumnEntities(expectedColumns, tableEntity1.columns());

    // 2. Add a new column
    Table alteredTable2 =
        tableOperationDispatcher.alterTable(
            tableIdent,
            TableChange.addColumn(
                new String[] {"col4"},
                Types.StringType.get(),
                "comment4",
                TableChange.ColumnPosition.first(),
                true,
                true,
                Literals.stringLiteral("4")));
    Column[] expectedColumns2 =
        new Column[] {
          TestColumn.builder()
              .withName("col4")
              .withPosition(0)
              .withType(Types.StringType.get())
              .withComment("comment4")
              .withNullable(true)
              .withAutoIncrement(true)
              .withDefaultValue(Literals.stringLiteral("4"))
              .build(),
          TestColumn.builder()
              .withName("col3")
              .withPosition(1)
              .withType(Types.StringType.get())
              .withComment("comment1")
              .withNullable(true)
              .withAutoIncrement(true)
              .withDefaultValue(Literals.stringLiteral("1"))
              .build(),
          TestColumn.builder()
              .withName("col2")
              .withPosition(2)
              .withType(Types.StringType.get())
              .withComment("comment2")
              .withNullable(false)
              .withAutoIncrement(false)
              .withDefaultValue(Literals.stringLiteral("2"))
              .build()
        };

    testColumns(expectedColumns2, alteredTable2.columns());

    TableEntity tableEntity2 = entityStore.get(tableIdent, TABLE, TableEntity.class);
    testColumnAndColumnEntities(expectedColumns2, tableEntity2.columns());

    // 3. Drop a column
    Table alteredTable3 =
        tableOperationDispatcher.alterTable(
            tableIdent,
            TableChange.deleteColumn(new String[] {"col2"}, true),
            TableChange.deleteColumn(new String[] {"col3"}, true));
    Column[] expectedColumns3 =
        new Column[] {
          TestColumn.builder()
              .withName("col4")
              .withPosition(0)
              .withType(Types.StringType.get())
              .withComment("comment4")
              .withNullable(true)
              .withAutoIncrement(true)
              .withDefaultValue(Literals.stringLiteral("4"))
              .build()
        };
    testColumns(expectedColumns3, alteredTable3.columns());

    TableEntity tableEntity3 = entityStore.get(tableIdent, TABLE, TableEntity.class);
    testColumnAndColumnEntities(expectedColumns3, tableEntity3.columns());

    // 4. Update column default value
    Table alteredTable4 =
        tableOperationDispatcher.alterTable(
            tableIdent,
            TableChange.updateColumnDefaultValue(
                new String[] {"col4"}, Literals.stringLiteral("5")));

    Column[] expectedColumns4 =
        new Column[] {
          TestColumn.builder()
              .withName("col4")
              .withPosition(0)
              .withType(Types.StringType.get())
              .withComment("comment4")
              .withNullable(true)
              .withAutoIncrement(true)
              .withDefaultValue(Literals.stringLiteral("5"))
              .build()
        };
    testColumns(expectedColumns4, alteredTable4.columns());

    TableEntity tableEntity4 = entityStore.get(tableIdent, TABLE, TableEntity.class);
    testColumnAndColumnEntities(expectedColumns4, tableEntity4.columns());

    // 5. Update column type
    Table alteredTable5 =
        tableOperationDispatcher.alterTable(
            tableIdent,
            TableChange.updateColumnType(new String[] {"col4"}, Types.IntegerType.get()));

    Column[] expectedColumns5 =
        new Column[] {
          TestColumn.builder()
              .withName("col4")
              .withPosition(0)
              .withType(Types.IntegerType.get())
              .withComment("comment4")
              .withNullable(true)
              .withAutoIncrement(true)
              .withDefaultValue(Literals.stringLiteral("5"))
              .build()
        };

    testColumns(expectedColumns5, alteredTable5.columns());

    TableEntity tableEntity5 = entityStore.get(tableIdent, TABLE, TableEntity.class);
    testColumnAndColumnEntities(expectedColumns5, tableEntity5.columns());

    // 6. Update column comment
    Table alteredTable6 =
        tableOperationDispatcher.alterTable(
            tableIdent, TableChange.updateColumnComment(new String[] {"col4"}, "new comment"));

    Column[] expectedColumns6 =
        new Column[] {
          TestColumn.builder()
              .withName("col4")
              .withPosition(0)
              .withType(Types.IntegerType.get())
              .withComment("new comment")
              .withNullable(true)
              .withAutoIncrement(true)
              .withDefaultValue(Literals.stringLiteral("5"))
              .build()
        };

    testColumns(expectedColumns6, alteredTable6.columns());

    TableEntity tableEntity6 = entityStore.get(tableIdent, TABLE, TableEntity.class);
    testColumnAndColumnEntities(expectedColumns6, tableEntity6.columns());

    // 7. Update column nullable
    Table alteredTable7 =
        tableOperationDispatcher.alterTable(
            tableIdent, TableChange.updateColumnNullability(new String[] {"col4"}, false));

    Column[] expectedColumns7 =
        new Column[] {
          TestColumn.builder()
              .withName("col4")
              .withPosition(0)
              .withType(Types.IntegerType.get())
              .withComment("new comment")
              .withNullable(false)
              .withAutoIncrement(true)
              .withDefaultValue(Literals.stringLiteral("5"))
              .build()
        };

    testColumns(expectedColumns7, alteredTable7.columns());

    TableEntity tableEntity7 = entityStore.get(tableIdent, TABLE, TableEntity.class);
    testColumnAndColumnEntities(expectedColumns7, tableEntity7.columns());

    // 8. Update column auto increment
    Table alteredTable8 =
        tableOperationDispatcher.alterTable(
            tableIdent, TableChange.updateColumnAutoIncrement(new String[] {"col4"}, false));

    Column[] expectedColumns8 =
        new Column[] {
          TestColumn.builder()
              .withName("col4")
              .withPosition(0)
              .withType(Types.IntegerType.get())
              .withComment("new comment")
              .withNullable(false)
              .withAutoIncrement(false)
              .withDefaultValue(Literals.stringLiteral("5"))
              .build()
        };

    testColumns(expectedColumns8, alteredTable8.columns());

    TableEntity tableEntity8 = entityStore.get(tableIdent, TABLE, TableEntity.class);
    testColumnAndColumnEntities(expectedColumns8, tableEntity8.columns());
  }

  @Test
  public void testCreateAndDropTableWithColumn() throws IOException {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema111");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent = NameIdentifier.of(tableNs, "table61");
    Column[] columns =
        new Column[] {
          TestColumn.builder()
              .withName("col1")
              .withPosition(0)
              .withType(Types.StringType.get())
              .withComment("comment1")
              .withNullable(true)
              .withAutoIncrement(true)
              .withDefaultValue(Literals.stringLiteral("1"))
              .build(),
          TestColumn.builder()
              .withName("col2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .withComment("comment2")
              .withNullable(false)
              .withAutoIncrement(false)
              .withDefaultValue(Literals.stringLiteral("2"))
              .build()
        };

    Table table1 =
        tableOperationDispatcher.createTable(
            tableIdent, columns, "comment", props, new Transform[0]);
    testColumns(columns, table1.columns());

    // Delete table
    boolean dropped = tableOperationDispatcher.dropTable(tableIdent);
    Assertions.assertTrue(dropped);
    Assertions.assertFalse(entityStore.exists(tableIdent, TABLE));
  }

  private static void testColumns(Column[] expectedColumns, Column[] actualColumns) {
    Map<String, Column> expectedColumnMap =
        expectedColumns == null
            ? Collections.emptyMap()
            : Arrays.stream(expectedColumns)
                .collect(Collectors.toMap(c -> c.name().toLowerCase(), Function.identity()));
    Map<String, Column> actualColumnMap =
        actualColumns == null
            ? Collections.emptyMap()
            : Arrays.stream(actualColumns)
                .collect(Collectors.toMap(Column::name, Function.identity()));

    Assertions.assertEquals(expectedColumnMap.size(), actualColumnMap.size());
    expectedColumnMap.forEach(
        (name, expectedColumn) -> {
          TestColumn actualColumn = (TestColumn) actualColumnMap.get(name);
          TestColumn e = (TestColumn) expectedColumn;
          Assertions.assertNotNull(actualColumn);
          Assertions.assertEquals(e.name().toLowerCase(), actualColumn.name());
          Assertions.assertEquals(e.position(), actualColumn.position());
          Assertions.assertEquals(e.dataType(), actualColumn.dataType());
          Assertions.assertEquals(e.comment(), actualColumn.comment());
          Assertions.assertEquals(e.nullable(), actualColumn.nullable());
          Assertions.assertEquals(e.autoIncrement(), actualColumn.autoIncrement());
          Assertions.assertEquals(e.defaultValue(), actualColumn.defaultValue());
        });
  }

  private static void testColumnAndColumnEntities(
      Column[] expectedColumns, List<ColumnEntity> ColumnEntities) {
    Map<String, Column> expectedColumnMap =
        expectedColumns == null
            ? Collections.emptyMap()
            : Arrays.stream(expectedColumns)
                .collect(Collectors.toMap(Column::name, Function.identity()));
    Map<String, ColumnEntity> actualColumnMap =
        ColumnEntities == null
            ? Collections.emptyMap()
            : ColumnEntities.stream()
                .collect(Collectors.toMap(ColumnEntity::name, Function.identity()));

    Assertions.assertEquals(expectedColumnMap.size(), actualColumnMap.size());
    expectedColumnMap.forEach(
        (name, expectedColumn) -> {
          ColumnEntity actualColumn = actualColumnMap.get(name);
          TestColumn e = (TestColumn) expectedColumn;
          Assertions.assertNotNull(actualColumn);
          Assertions.assertEquals(e.name(), actualColumn.name());
          Assertions.assertEquals(e.position(), actualColumn.position());
          Assertions.assertEquals(e.dataType(), actualColumn.dataType());
          Assertions.assertEquals(e.comment(), actualColumn.comment());
          Assertions.assertEquals(e.nullable(), actualColumn.nullable());
          Assertions.assertEquals(e.autoIncrement(), actualColumn.autoIncrement());
          Assertions.assertEquals(e.defaultValue(), actualColumn.defaultValue());
        });
  }

  public static TableOperationDispatcher getTableOperationDispatcher() {
    return tableOperationDispatcher;
  }

  public static SchemaOperationDispatcher getSchemaOperationDispatcher() {
    return schemaOperationDispatcher;
  }
}
