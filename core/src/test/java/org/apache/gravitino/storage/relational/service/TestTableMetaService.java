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
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.function.Executable;

public class TestTableMetaService extends TestJDBCBackend {
  private final String metalakeName = "metalake_for_table_test";
  private final String catalogName = "catalog_for_table_test";
  private final String schemaName = "schema_for_table_test";

  private long maxEntityChangeId() {
    return SessionUtils.doWithCommitAndFetchResult(
        EntityChangeLogMapper.class, EntityChangeLogMapper::selectMaxChangeId);
  }

  private List<EntityChangeRecord> listEntityChanges(long lastConsumedId) {
    return SessionUtils.doWithCommitAndFetchResult(
        EntityChangeLogMapper.class, mapper -> mapper.selectEntityChanges(lastConsumedId, 100));
  }

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table",
            AUDIT_INFO);
    TableEntity tableCopy =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table",
            AUDIT_INFO);
    backend.insert(table, false);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(tableCopy, false));
  }

  @TestTemplate
  public void testInsertWaitsForConcurrentSchemaDelete() throws Exception {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema = createAndInsertSchema(metalakeName, catalogName, schemaName);
    SchemaPO observedSchemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(schema.id()));
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table_racing_schema_delete",
            AUDIT_INFO);

    Throwable insertFailure =
        runWhileSchemaDeleteUncommitted(
            observedSchemaPO, () -> TableMetaService.getInstance().insertTable(table, false));

    Assertions.assertInstanceOf(NoSuchEntityException.class, insertFailure);
    Assertions.assertTrue(
        SessionUtils.getWithoutCommit(
                TableMetaMapper.class, mapper -> mapper.listTablePOsByTableIds(List.of(table.id())))
            .isEmpty());
  }

  @TestTemplate
  public void testInsertRollsBackAllRowsWhenColumnWriteFails() throws IOException {
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);
    Namespace tableNamespace = NamespaceUtil.ofTable(metalakeName, catalogName, schemaName);
    ColumnEntity column = column("column", Types.IntegerType.get());
    TableEntity invalidTable =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table_insert_rollback")
            .withNamespace(tableNamespace)
            // The duplicate ID violates the column-version unique key. The failure happens after
            // table_meta and table_version_info have already been written in this transaction.
            .withColumns(List.of(column, column))
            .withAuditInfo(AUDIT_INFO)
            .build();

    assertThrows(
        RuntimeException.class,
        () -> TableMetaService.getInstance().insertTable(invalidTable, false));
    assertThrows(
        NoSuchEntityException.class,
        () -> TableMetaService.getInstance().getTableByIdentifier(invalidTable.nameIdentifier()));

    // Reusing the same table ID, name, and column proves that the failed attempt left no metadata,
    // version, or column row behind.
    TableEntity validTable =
        TableEntity.builder()
            .withId(invalidTable.id())
            .withName(invalidTable.name())
            .withNamespace(tableNamespace)
            .withColumns(List.of(column))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableMetaService.getInstance().insertTable(validTable, false);
    TableEntity inserted =
        TableMetaService.getInstance().getTableByIdentifier(validTable.nameIdentifier());
    Assertions.assertEquals(validTable.id(), inserted.id());
    Assertions.assertEquals(1, inserted.columns().size());
  }

  @TestTemplate
  public void testOverwriteRollsBackExistingTableAndThenSucceeds() throws IOException {
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);
    Namespace tableNamespace = NamespaceUtil.ofTable(metalakeName, catalogName, schemaName);
    ColumnEntity originalColumn = column("original_column", Types.IntegerType.get());
    TableEntity original =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table_overwrite_rollback")
            .withNamespace(tableNamespace)
            .withColumns(List.of(originalColumn))
            .withComment("original")
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableMetaService.getInstance().insertTable(original, false);

    ColumnEntity replacementColumn = column("replacement_column", Types.StringType.get());
    TableEntity invalidReplacement =
        TableEntity.builder()
            .withId(original.id())
            .withName(original.name())
            .withNamespace(tableNamespace)
            // Overwrite deletes the old columns before inserting these. Repeating the same ID
            // makes the final step fail, which must restore both the table and its old column.
            .withColumns(List.of(replacementColumn, replacementColumn))
            .withComment("must roll back")
            .withAuditInfo(AUDIT_INFO)
            .build();
    assertThrows(
        RuntimeException.class,
        () -> TableMetaService.getInstance().insertTable(invalidReplacement, true));

    TableEntity afterFailure =
        TableMetaService.getInstance().getTableByIdentifier(original.nameIdentifier());
    Assertions.assertEquals("original", afterFailure.comment());
    Assertions.assertEquals(List.of(originalColumn), afterFailure.columns());

    TableEntity validReplacement =
        TableEntity.builder()
            .withId(original.id())
            .withName(original.name())
            .withNamespace(tableNamespace)
            .withColumns(List.of(replacementColumn))
            .withComment("replaced")
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableMetaService.getInstance().insertTable(validReplacement, true);

    TableEntity afterSuccess =
        TableMetaService.getInstance().getTableByIdentifier(original.nameIdentifier());
    Assertions.assertEquals("replaced", afterSuccess.comment());
    Assertions.assertEquals(List.of(replacementColumn), afterSuccess.columns());
  }

  @TestTemplate
  public void testOverwriteAdvancesVersionAndRejectsStaleUpdate() throws IOException {
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);
    TableEntity original =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table_overwrite_occ",
            AUDIT_INFO);
    TableMetaService.getInstance().insertTable(original, false);
    TablePO beforeOverwrite = getTablePO(original.id());
    TableEntity replacement = copyTableWithComment(original, "overwrite winner");

    assertThrows(
        OptimisticLockException.class,
        () ->
            updateTableUnchecked(
                original.nameIdentifier(),
                current -> {
                  insertTableUnchecked(replacement, true);
                  return copyTableWithComment(current, "stale update");
                }));

    TableEntity winner =
        TableMetaService.getInstance().getTableByIdentifier(original.nameIdentifier());
    TablePO afterOverwrite = getTablePO(original.id());
    Assertions.assertEquals("overwrite winner", winner.comment());
    Assertions.assertEquals(
        beforeOverwrite.getCurrentVersion() + 1, afterOverwrite.getCurrentVersion());
    Assertions.assertEquals(afterOverwrite.getCurrentVersion(), afterOverwrite.getLastVersion());
  }

  @TestTemplate
  public void testUpdateAlreadyExistsException() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table",
            AUDIT_INFO);
    TableEntity tableCopy =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table1",
            AUDIT_INFO);
    backend.insert(table, false);
    backend.insert(tableCopy, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.update(
                tableCopy.nameIdentifier(),
                Entity.EntityType.TABLE,
                e ->
                    createTableEntity(tableCopy.id(), tableCopy.namespace(), "table", AUDIT_INFO)));
  }

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, AUDIT_INFO);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            catalogName,
            AUDIT_INFO);
    backend.insert(catalog, false);
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            schemaName,
            AUDIT_INFO);
    backend.insert(schema, false);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table",
            AUDIT_INFO);
    backend.insert(table, false);

    List<TableEntity> tables = backend.list(table.namespace(), Entity.EntityType.TABLE, true);
    assertTrue(tables.contains(table));

    // meta data soft delete
    backend.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, true);
    assertFalse(backend.exists(table.nameIdentifier(), Entity.EntityType.TABLE));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(table.id(), Entity.EntityType.TABLE));

    // meta data hard delete
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.hardDeleteLegacyData(entityType, Instant.now().toEpochMilli() + 1000);
    }
    assertFalse(legacyRecordExistsInDB(table.id(), Entity.EntityType.TABLE));
  }

  @TestTemplate
  public void testUpdateTable() throws IOException {
    String catalogName = "catalog1";
    String schemaName = "schema1";
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);

    ColumnEntity column1 =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column1")
            .withPosition(0)
            .withComment("comment1")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withDefaultValue(Literals.integerLiteral(1))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableEntity createdTable =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table1")
            .withNamespace(Namespace.of(metalakeName, catalogName, schemaName))
            .withColumns(List.of(column1))
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(createdTable, false);

    // test update table without changing schema name
    long maxIdBeforeRename = maxEntityChangeId();
    TableEntity updatedTable =
        TableEntity.builder()
            .withId(createdTable.id())
            .withName("table2")
            .withNamespace(createdTable.namespace())
            .withColumns(createdTable.columns())
            .withAuditInfo(AUDIT_INFO)
            .build();
    Function<TableEntity, TableEntity> updater = oldTable -> updatedTable;
    backend.update(createdTable.nameIdentifier(), Entity.EntityType.TABLE, updater);

    TableEntity retrievedTable =
        TableMetaService.getInstance().getTableByIdentifier(updatedTable.nameIdentifier());
    Assertions.assertEquals(updatedTable.id(), retrievedTable.id());
    Assertions.assertEquals(updatedTable.name(), retrievedTable.name());
    Assertions.assertEquals(updatedTable.namespace(), retrievedTable.namespace());
    Assertions.assertEquals(updatedTable.auditInfo(), retrievedTable.auditInfo());
    compareTwoColumns(updatedTable.columns(), retrievedTable.columns());
    compareTwoColumns(updatedTable.columns(), retrievedTable.columns());
    Assertions.assertTrue(
        listEntityChanges(maxIdBeforeRename).stream()
            .anyMatch(
                record ->
                    record.getMetalakeName().equals(metalakeName)
                        && record.getEntityType().equals(Entity.EntityType.TABLE.name())
                        && record
                            .getFullName()
                            .equals(
                                NameIdentifierUtil.ofTable(
                                        metalakeName, catalogName, schemaName, "table1")
                                    .toString())
                        && record.getOperateType() == OperateType.ALTER));

    // test update table with changing schema name to a non-existing schema
    String newSchemaName = "schema2";
    TableEntity updatedTable2 =
        TableEntity.builder()
            .withId(updatedTable.id())
            .withName("table3")
            .withNamespace(Namespace.of(metalakeName, catalogName, newSchemaName))
            .withColumns(updatedTable.columns())
            .withAuditInfo(AUDIT_INFO)
            .build();
    Function<TableEntity, TableEntity> updater2 = oldTable -> updatedTable2;
    Exception e =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () -> backend.update(updatedTable.nameIdentifier(), Entity.EntityType.TABLE, updater2));
    Assertions.assertTrue(e.getMessage().contains(newSchemaName));

    // test update table with changing schema name to an existing schema
    SchemaEntity newSchema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalogName),
            newSchemaName,
            AUDIT_INFO);
    backend.insert(newSchema, false);

    long maxIdBeforeSchemaMove = maxEntityChangeId();
    TableEntity movedTable =
        TableEntity.builder()
            .withId(updatedTable.id())
            .withName(updatedTable.name())
            .withNamespace(Namespace.of(metalakeName, catalogName, newSchemaName))
            .withColumns(updatedTable.columns())
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.update(updatedTable.nameIdentifier(), Entity.EntityType.TABLE, oldTable -> movedTable);
    Assertions.assertTrue(
        listEntityChanges(maxIdBeforeSchemaMove).stream()
            .anyMatch(
                record ->
                    record.getMetalakeName().equals(metalakeName)
                        && record.getEntityType().equals(Entity.EntityType.TABLE.name())
                        && record
                            .getFullName()
                            .equals(
                                NameIdentifierUtil.ofTable(
                                        metalakeName, catalogName, schemaName, "table2")
                                    .toString())
                        && record.getOperateType() == OperateType.ALTER));

    backend.update(movedTable.nameIdentifier(), Entity.EntityType.TABLE, updater2);

    TableEntity retrievedTable2 =
        TableMetaService.getInstance().getTableByIdentifier(updatedTable2.nameIdentifier());
    Assertions.assertEquals(updatedTable2.id(), retrievedTable2.id());
    Assertions.assertEquals(updatedTable2.name(), retrievedTable2.name());
    Assertions.assertEquals(updatedTable2.namespace(), retrievedTable2.namespace());
    Assertions.assertEquals(updatedTable2.auditInfo(), retrievedTable2.auditInfo());
    compareTwoColumns(updatedTable2.columns(), retrievedTable2.columns());

    long maxIdBeforeDelete = maxEntityChangeId();
    Assertions.assertTrue(
        backend.delete(updatedTable2.nameIdentifier(), Entity.EntityType.TABLE, false));
    Assertions.assertTrue(
        listEntityChanges(maxIdBeforeDelete).stream()
            .anyMatch(
                record ->
                    record.getMetalakeName().equals(metalakeName)
                        && record.getEntityType().equals(Entity.EntityType.TABLE.name())
                        && record
                            .getFullName()
                            .equals(
                                NameIdentifierUtil.ofTable(
                                        metalakeName, catalogName, newSchemaName, "table3")
                                    .toString())
                        && record.getOperateType() == OperateType.DROP));
  }

  @TestTemplate
  public void testAlterReportsOptimisticLockConflictAndKeepsWinnerVersion() throws IOException {
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table_alter_conflict",
            AUDIT_INFO);
    backend.insert(table, false);
    TablePO initialPO = getTablePO(table.id());

    assertThrows(
        OptimisticLockException.class,
        () ->
            TableMetaService.getInstance()
                .updateTable(
                    table.nameIdentifier(),
                    entity -> {
                      TableEntity current = (TableEntity) entity;
                      // The updater runs before the outer CAS. Commit another update here so the
                      // outer write continues with a stale current_version and must lose the CAS.
                      updateTableUnchecked(
                          table.nameIdentifier(),
                          competing -> copyTableWithComment(competing, "competing update"));
                      return copyTableWithComment(current, "requested update");
                    }));

    TableEntity current =
        TableMetaService.getInstance().getTableByIdentifier(table.nameIdentifier());
    Assertions.assertEquals("competing update", current.comment());
    TablePO currentPO = getTablePO(table.id());
    Assertions.assertEquals(
        initialPO.getCurrentVersion() + 1, currentPO.getCurrentVersion().longValue());
    Assertions.assertEquals(currentPO.getCurrentVersion(), currentPO.getLastVersion());
  }

  @TestTemplate
  public void testAlterReportsNoSuchWhenDeletedConcurrently() throws IOException {
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table_alter_deleted",
            AUDIT_INFO);
    backend.insert(table, false);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            TableMetaService.getInstance()
                .updateTable(
                    table.nameIdentifier(),
                    entity -> {
                      TableMetaService.getInstance().deleteTable(table.nameIdentifier());
                      return copyTableWithComment((TableEntity) entity, "requested update");
                    }));
    assertThrows(
        NoSuchEntityException.class,
        () -> TableMetaService.getInstance().getTableByIdentifier(table.nameIdentifier()));
  }

  @TestTemplate
  public void testAlterReportsNoSuchWhenRenamedConcurrently() throws IOException {
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table_alter_renamed",
            AUDIT_INFO);
    backend.insert(table, false);
    NameIdentifier renamedIdentifier =
        NameIdentifier.of(table.namespace(), "table_alter_renamed_winner");

    assertStaleAlterReportsNoSuch(
        table,
        competing ->
            copyTable(competing, renamedIdentifier.name(), competing.namespace(), "renamed winner"),
        renamedIdentifier,
        "renamed winner");
  }

  @TestTemplate
  public void testAlterReportsNoSuchWhenMovedConcurrently() throws IOException {
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);
    String newSchemaName = "schema_for_concurrent_move";
    createAndInsertSchema(metalakeName, catalogName, newSchemaName);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table_alter_moved",
            AUDIT_INFO);
    backend.insert(table, false);
    Namespace movedNamespace = NamespaceUtil.ofTable(metalakeName, catalogName, newSchemaName);
    NameIdentifier movedIdentifier = NameIdentifier.of(movedNamespace, table.name());

    assertStaleAlterReportsNoSuch(
        table,
        competing -> copyTable(competing, competing.name(), movedNamespace, "moved winner"),
        movedIdentifier,
        "moved winner");
  }

  @TestTemplate
  public void testDeleteRejectsStaleVersion() throws IOException {
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);
    ColumnEntity column = column("column_that_must_survive", Types.IntegerType.get());
    TableEntity table =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table_stale_delete")
            .withNamespace(NamespaceUtil.ofTable(metalakeName, catalogName, schemaName))
            .withColumns(List.of(column))
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(table, false);
    TablePO stalePO = getTablePO(table.id());

    TableMetaService.getInstance()
        .updateTable(
            table.nameIdentifier(),
            entity -> copyTableWithComment((TableEntity) entity, "winning update"));

    // The stale drop still carries the original version. It must not delete the newer table.
    assertThrows(
        OptimisticLockException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () ->
                    TableMetaService.getInstance()
                        .deleteTableWithVersion(table.nameIdentifier(), stalePO)));
    TableEntity current =
        TableMetaService.getInstance().getTableByIdentifier(table.nameIdentifier());
    Assertions.assertEquals("winning update", current.comment());
    Assertions.assertEquals(1, current.columns().size());
    Assertions.assertEquals(column.id(), current.columns().get(0).id());
  }

  @TestTemplate
  public void testDeleteReportsNoSuchWhenDeletedConcurrently() throws IOException {
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table_delete_deleted",
            AUDIT_INFO);
    backend.insert(table, false);
    TablePO stalePO = getTablePO(table.id());

    TableMetaService.getInstance().deleteTable(table.nameIdentifier());

    // The second delete still has the first delete's snapshot. Since the table is now gone rather
    // than merely newer, the result must be "not found", not an OCC conflict.
    assertThrows(
        NoSuchEntityException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () ->
                    TableMetaService.getInstance()
                        .deleteTableWithVersion(table.nameIdentifier(), stalePO)));
  }

  @TestTemplate
  public void testUpdateRollsBackMetadataAndVersionWhenColumnWriteFails() throws IOException {
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table_update_rollback",
            AUDIT_INFO);
    backend.insert(table, false);
    TablePO initialPO = getTablePO(table.id());

    ColumnEntity duplicateColumn = column("duplicate_id", Types.IntegerType.get());
    // The duplicate column ID fails after the table and version rows are updated. The assertions
    // below verify that the outer transaction rolls both earlier writes back.
    assertThrows(
        IllegalStateException.class,
        () ->
            TableMetaService.getInstance()
                .updateTable(
                    table.nameIdentifier(),
                    entity -> {
                      TableEntity current = (TableEntity) entity;
                      return copyTableWithColumns(
                          current, List.of(duplicateColumn, duplicateColumn), "must roll back");
                    }));

    TableEntity current =
        TableMetaService.getInstance().getTableByIdentifier(table.nameIdentifier());
    Assertions.assertNull(current.comment());
    Assertions.assertTrue(current.columns().isEmpty());
    TablePO currentPO = getTablePO(table.id());
    Assertions.assertEquals(initialPO.getCurrentVersion(), currentPO.getCurrentVersion());
    Assertions.assertEquals(initialPO.getLastVersion(), currentPO.getLastVersion());
  }

  @TestTemplate
  public void testMoveTableWaitsForConcurrentTargetSchemaDelete() throws Exception {
    String sourceSchemaName = "source_schema";
    String targetSchemaName = "target_schema";
    createParentEntities(metalakeName, catalogName, sourceSchemaName, AUDIT_INFO);
    SchemaEntity targetSchema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            targetSchemaName,
            AUDIT_INFO);
    backend.insert(targetSchema, false);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, sourceSchemaName),
            "moving_table",
            AUDIT_INFO);
    backend.insert(table, false);

    SchemaPO observedTargetSchema =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(targetSchema.id()));
    TableEntity movedTable =
        TableEntity.builder()
            .withId(table.id())
            .withName(table.name())
            .withNamespace(NamespaceUtil.ofTable(metalakeName, catalogName, targetSchemaName))
            .withColumns(table.columns())
            .withAuditInfo(table.auditInfo())
            .build();
    // Resolving the target ID happens before the table transaction. The move must then wait on the
    // target schema row instead of writing below a schema whose delete is about to commit.
    Throwable moveFailure =
        runWhileSchemaDeleteUncommitted(
            observedTargetSchema,
            () ->
                backend.update(
                    table.nameIdentifier(), Entity.EntityType.TABLE, ignored -> movedTable));

    Assertions.assertInstanceOf(NoSuchEntityException.class, moveFailure);

    TableEntity unchanged =
        TableMetaService.getInstance().getTableByIdentifier(table.nameIdentifier());
    Assertions.assertEquals(table.namespace(), unchanged.namespace());
    assertFalse(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, targetSchemaName, table.name()),
            Entity.EntityType.TABLE));
  }

  @TestTemplate
  public void testBatchGetTableByIdentifierIncludesVersionInfoFields() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);

    Map<String, String> tableProps =
        ImmutableMap.of(Table.PROPERTY_TABLE_FORMAT, "delta", "location", "s3://bucket/path");
    Distribution distribution = Distributions.of(Strategy.HASH, 4, NamedReference.field("col1"));
    SortOrder[] sortOrders =
        new SortOrder[] {SortOrders.of(NamedReference.field("col1"), SortDirection.ASCENDING)};
    Transform[] partitioning = new Transform[] {Transforms.identity("col2")};
    Index[] indexes = new Index[] {Indexes.primary("pk", new String[][] {{"col1"}})};

    TableEntity table =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("delta_table")
            .withNamespace(NamespaceUtil.ofTable(metalakeName, catalogName, schemaName))
            .withProperties(tableProps)
            .withComment("test table comment")
            .withDistribution(distribution)
            .withSortOrders(sortOrders)
            .withPartitioning(partitioning)
            .withIndexes(indexes)
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableMetaService.getInstance().insertTable(table, false);

    NameIdentifier tableIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "delta_table");
    List<TableEntity> results =
        TableMetaService.getInstance().batchGetTableByIdentifier(List.of(tableIdent));

    Assertions.assertEquals(1, results.size());
    TableEntity result = results.get(0);

    // Verify properties (including format) are returned
    Assertions.assertNotNull(result.properties());
    Assertions.assertEquals("delta", result.properties().get(Table.PROPERTY_TABLE_FORMAT));
    Assertions.assertEquals("s3://bucket/path", result.properties().get("location"));

    // Verify comment is returned
    Assertions.assertEquals("test table comment", result.comment());

    // Verify distribution is returned
    Assertions.assertNotNull(result.distribution());
    Assertions.assertEquals(distribution, result.distribution());

    // Verify sort orders are returned
    Assertions.assertNotNull(result.sortOrders());
    Assertions.assertArrayEquals(sortOrders, result.sortOrders());

    // Verify partitioning is returned — compare field references since serialization may change
    // the concrete implementation class (e.g., IdentityTransform -> IdentityPartitioningDTO)
    Assertions.assertNotNull(result.partitioning());
    Assertions.assertEquals(partitioning.length, result.partitioning().length);
    Assertions.assertEquals(
        ((NamedReference.FieldReference) partitioning[0].references()[0]).fieldName()[0],
        ((NamedReference.FieldReference) result.partitioning()[0].references()[0]).fieldName()[0]);

    // Verify indexes are returned
    Assertions.assertNotNull(result.indexes());
    Assertions.assertArrayEquals(indexes, result.indexes());
  }

  @TestTemplate
  public void testBatchGetTableByIdentifierDoesNotIncludeColumns() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);

    ColumnEntity column = column("col1", Types.IntegerType.get());
    TableEntity table =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table_with_columns")
            .withNamespace(NamespaceUtil.ofTable(metalakeName, catalogName, schemaName))
            .withColumns(List.of(column))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableMetaService.getInstance().insertTable(table, false);

    // Verify getTableByIdentifier (single-get path) returns columns
    TableEntity singleGetResult =
        TableMetaService.getInstance()
            .getTableByIdentifier(
                NameIdentifier.of(metalakeName, catalogName, schemaName, "table_with_columns"));
    Assertions.assertEquals(1, singleGetResult.columns().size());

    // batchGetTableByIdentifier does not fetch columns (separate table_column_meta table)
    NameIdentifier tableIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "table_with_columns");
    List<TableEntity> results =
        TableMetaService.getInstance().batchGetTableByIdentifier(List.of(tableIdent));

    Assertions.assertEquals(1, results.size());
    Assertions.assertTrue(
        results.get(0).columns().isEmpty(),
        "batchGetTableByIdentifier does not fetch columns from table_column_meta");
  }

  private void compareTwoColumns(
      List<ColumnEntity> expectedColumns, List<ColumnEntity> actualColumns) {
    Assertions.assertEquals(expectedColumns.size(), actualColumns.size());
    Map<String, ColumnEntity> expectedColumnsMap =
        expectedColumns.stream().collect(Collectors.toMap(ColumnEntity::name, Function.identity()));
    actualColumns.forEach(
        column -> {
          ColumnEntity expectedColumn = expectedColumnsMap.get(column.name());
          Assertions.assertNotNull(expectedColumn);
          Assertions.assertEquals(expectedColumn.id(), column.id());
          Assertions.assertEquals(expectedColumn.name(), column.name());
          Assertions.assertEquals(expectedColumn.position(), column.position());
          Assertions.assertEquals(expectedColumn.comment(), column.comment());
          Assertions.assertEquals(expectedColumn.dataType(), column.dataType());
          Assertions.assertEquals(expectedColumn.nullable(), column.nullable());
          Assertions.assertEquals(expectedColumn.autoIncrement(), column.autoIncrement());
          Assertions.assertEquals(expectedColumn.defaultValue(), column.defaultValue());
          Assertions.assertEquals(expectedColumn.auditInfo(), column.auditInfo());
        });
  }

  private TablePO getTablePO(long tableId) {
    return SessionUtils.getWithoutCommit(
        TableMetaMapper.class, mapper -> mapper.listTablePOsByTableIds(List.of(tableId)).get(0));
  }

  /**
   * Holds an uncommitted delete of the given schema open, runs {@code victim} against it, and
   * returns what the victim threw once the delete commits, or null if it succeeded.
   *
   * <p>The helper also asserts the part both callers care about: while the delete is in flight the
   * victim must block on the schema row rather than slip past it.
   */
  private Throwable runWhileSchemaDeleteUncommitted(SchemaPO observedSchemaPO, Executable victim)
      throws Exception {
    CountDownLatch schemaDeleteLocked = new CountDownLatch(1);
    CountDownLatch allowDeleteCommit = new CountDownLatch(1);
    CountDownLatch victimStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> deleteResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () -> {
                      int deleted =
                          SessionUtils.getWithoutCommit(
                              SchemaMetaMapper.class,
                              mapper ->
                                  mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                                      observedSchemaPO.getSchemaId(),
                                      observedSchemaPO.getCurrentVersion()));
                      Assertions.assertEquals(1, deleted);
                      schemaDeleteLocked.countDown();
                      try {
                        assertTrue(allowDeleteCommit.await(30, TimeUnit.SECONDS));
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                      }
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });
    try {
      assertTrue(schemaDeleteLocked.await(30, TimeUnit.SECONDS));
      Future<Throwable> victimResult =
          executor.submit(
              () -> {
                victimStarted.countDown();
                try {
                  victim.execute();
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(victimStarted.await(30, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> victimResult.get(500, TimeUnit.MILLISECONDS));

      allowDeleteCommit.countDown();
      Assertions.assertNull(deleteResult.get(30, TimeUnit.SECONDS));
      return victimResult.get(30, TimeUnit.SECONDS);
    } finally {
      allowDeleteCommit.countDown();
      executor.shutdownNow();
    }
  }

  /**
   * Runs an alter that loses to a competing writer which renames or moves the table away, and
   * asserts the loser is told the table is gone while the winner's change survives.
   */
  private void assertStaleAlterReportsNoSuch(
      TableEntity table,
      Function<TableEntity, TableEntity> competingUpdate,
      NameIdentifier winnerIdentifier,
      String winnerComment) {
    assertThrows(
        NoSuchEntityException.class,
        () ->
            TableMetaService.getInstance()
                .updateTable(
                    table.nameIdentifier(),
                    entity -> {
                      TableEntity current = (TableEntity) entity;
                      updateTableUnchecked(table.nameIdentifier(), competingUpdate);
                      return copyTableWithComment(current, "stale update");
                    }));

    assertThrows(
        NoSuchEntityException.class,
        () -> TableMetaService.getInstance().getTableByIdentifier(table.nameIdentifier()));
    Assertions.assertEquals(
        winnerComment,
        TableMetaService.getInstance().getTableByIdentifier(winnerIdentifier).comment());
  }

  private ColumnEntity column(String name, Type dataType) {
    return ColumnEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withPosition(0)
        .withDataType(dataType)
        .withNullable(true)
        .withAutoIncrement(false)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private TableEntity copyTableWithComment(TableEntity current, String comment) {
    return copyTable(current, current.name(), current.namespace(), comment);
  }

  private TableEntity copyTableWithColumns(
      TableEntity current, List<ColumnEntity> columns, String comment) {
    return copyTable(current, current.name(), current.namespace(), comment, columns);
  }

  private TableEntity copyTable(
      TableEntity current, String name, Namespace namespace, String comment) {
    return copyTable(current, name, namespace, comment, current.columns());
  }

  private TableEntity copyTable(
      TableEntity current,
      String name,
      Namespace namespace,
      String comment,
      List<ColumnEntity> columns) {
    return TableEntity.builder()
        .withId(current.id())
        .withName(name)
        .withNamespace(namespace)
        .withColumns(columns)
        .withProperties(current.properties())
        .withPartitioning(current.partitioning())
        .withSortOrders(current.sortOrders())
        .withDistribution(current.distribution())
        .withIndexes(current.indexes())
        .withComment(comment)
        .withAuditInfo(current.auditInfo())
        .build();
  }

  private void updateTableUnchecked(
      NameIdentifier identifier, Function<TableEntity, TableEntity> updater) {
    try {
      TableMetaService.getInstance().updateTable(identifier, updater);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void insertTableUnchecked(TableEntity table, boolean overwrite) {
    try {
      TableMetaService.getInstance().insertTable(table, overwrite);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
