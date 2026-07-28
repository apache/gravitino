/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.TableDeletionEntryPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Cross-backend tests for root-only table deletion transactions. */
public class TestTableDeletionService extends TestJDBCBackend {

  private static final String METALAKE = "deletion_metalake";
  private static final String CATALOG = "deletion_catalog";
  private static final String SCHEMA = "deletion_schema";
  private static final String TABLE = "orders";
  private static final long DELETED_AT = 1_784_800_000_000L;
  private static final long RETENTION_EXPIRES_AT = Long.MAX_VALUE;

  private NameIdentifier tableIdentifier;
  private TableEntity table;
  private TablePO liveTable;

  @BeforeEach
  public void createTable() throws IOException {
    createParentEntities(METALAKE, CATALOG, SCHEMA, AUDIT_INFO);
    Namespace namespace = NamespaceUtil.ofTable(METALAKE, CATALOG, SCHEMA);
    tableIdentifier = NameIdentifier.of(namespace, TABLE);
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("order_id")
            .withPosition(0)
            .withDataType(Types.LongType.get())
            .withNullable(false)
            .withAutoIncrement(false)
            .withAuditInfo(AUDIT_INFO)
            .build();
    table =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(TABLE)
            .withNamespace(namespace)
            .withColumns(List.of(column))
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(table, false);
    liveTable = loadLiveTable();
  }

  @TestTemplate
  public void testDeleteAndRestoreOnlyMutateTableRoot() throws IOException {
    List<Long> tableVersionState = childDeletedAt("table_version_info");
    List<Long> columnState = childDeletedAt("table_column_version_info");

    EntityDeletionPO deletion = newDeletion("D1");
    TableDeletionService.getInstance().delete(liveTable, DELETED_AT, deletion);

    TablePO retained = TableDeletionService.getInstance().getRetainedTable("D1");
    assertNotNull(retained);
    assertEquals(table.id(), retained.getTableId());
    assertEquals(DELETED_AT, retained.getDeletedAt());
    assertEquals("D1", retained.getDeletionId());
    assertNotNull(EntityDeletionService.getInstance().get("D1"));
    assertFalse(backend.exists(tableIdentifier, Entity.EntityType.TABLE));
    assertEquals(tableVersionState, childDeletedAt("table_version_info"));
    assertEquals(columnState, childDeletedAt("table_column_version_info"));

    TablePO restored = TableDeletionService.getInstance().restore("D1");

    assertEquals(table.id(), restored.getTableId());
    assertTrue(backend.exists(tableIdentifier, Entity.EntityType.TABLE));
    assertNull(TableDeletionService.getInstance().getRetainedTable("D1"));
    assertNull(EntityDeletionService.getInstance().get("D1"));
    assertEquals(tableVersionState, childDeletedAt("table_version_info"));
    assertEquals(columnState, childDeletedAt("table_column_version_info"));
  }

  @TestTemplate
  public void testDeleteUsesTheLockedCurrentVersion() throws IOException {
    EntityDeletionPO deletion = newDeletion("D-current");
    TablePO staleTable = copyWithVersion(liveTable, liveTable.getCurrentVersion() + 1);

    TableDeletionService.getInstance().delete(staleTable, DELETED_AT, deletion);

    assertNotNull(EntityDeletionService.getInstance().get("D-current"));
    TablePO retained = TableDeletionService.getInstance().getRetainedTable("D-current");
    assertNotNull(retained);
    assertEquals(liveTable.getCurrentVersion(), retained.getCurrentVersion());
    assertFalse(backend.exists(tableIdentifier, Entity.EntityType.TABLE));
  }

  @TestTemplate
  public void testRetainedTableReadsJoinTheRootAndAction() throws IOException {
    TableDeletionService.getInstance().delete(liveTable, DELETED_AT, newDeletion("D-joined"));
    long schemaId = liveTable.getSchemaId();

    TableDeletionEntryPO entry =
        TableDeletionService.getInstance().getRetainedTableDeletion(schemaId, TABLE);
    assertNotNull(entry);
    assertEquals(table.id(), entry.getTable().getTableId());
    assertEquals(TABLE, entry.getTable().getTableName());
    assertEquals("D-joined", entry.getTable().getDeletionId());
    assertEquals("D-joined", entry.getDeletion().getDeletionId());
    assertEquals("DELETED", entry.getDeletion().getState());
    assertEquals(RETENTION_EXPIRES_AT, entry.getDeletion().getRetentionExpiresAt());
    assertEquals(
        List.of("D-joined"),
        TableDeletionService.getInstance().listRetainedTableDeletions(schemaId).stream()
            .map(value -> value.getDeletion().getDeletionId())
            .toList());
    assertNull(TableDeletionService.getInstance().getRetainedTableDeletion(schemaId, "missing"));
  }

  @TestTemplate
  public void testDeleteDoesNotLeaveAnActionWhenLockedRootIsNotLive() throws IOException {
    TableDeletionService.getInstance().delete(liveTable, DELETED_AT, newDeletion("D1"));

    assertThrows(
        IllegalStateException.class,
        () ->
            TableDeletionService.getInstance()
                .delete(liveTable, DELETED_AT + 1, newDeletion("D-rollback")));

    assertNull(EntityDeletionService.getInstance().get("D-rollback"));
    assertNull(TableDeletionService.getInstance().getRetainedTable("D-rollback"));
    assertNotNull(EntityDeletionService.getInstance().get("D1"));
    assertFalse(backend.exists(tableIdentifier, Entity.EntityType.TABLE));
  }

  @TestTemplate
  public void testDeleteJoinsOuterTransactionRollback() throws IOException {
    assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () ->
                    TableDeletionService.getInstance()
                        .delete(liveTable, DELETED_AT, newDeletion("D-rollback")),
                () -> {
                  throw new IllegalStateException("force outer transaction rollback");
                }));

    assertNull(EntityDeletionService.getInstance().get("D-rollback"));
    assertNull(TableDeletionService.getInstance().getRetainedTable("D-rollback"));
    assertTrue(backend.exists(tableIdentifier, Entity.EntityType.TABLE));
  }

  @TestTemplate
  public void testRestoreRejectsExpiredAndPurgeOwnedActions() throws IOException {
    TableDeletionService.getInstance().delete(liveTable, DELETED_AT, newDeletion("D1"));

    updateDeletion("UPDATE entity_deletion SET retention_expires_at = 1 WHERE deletion_id = 'D1'");
    assertRestoreRejected("D1");

    updateDeletion(
        "UPDATE entity_deletion SET retention_expires_at = "
            + RETENTION_EXPIRES_AT
            + ", purge_job_id = 'job-1' WHERE deletion_id = 'D1'");
    assertRestoreRejected("D1");

    updateDeletion(
        "UPDATE entity_deletion SET purge_job_id = NULL, state = 'PURGING'"
            + " WHERE deletion_id = 'D1'");
    assertRestoreRejected("D1");
  }

  @TestTemplate
  public void testOldDeletionCannotRestoreLaterGeneration() throws IOException {
    TableDeletionService.getInstance().delete(liveTable, DELETED_AT, newDeletion("D1"));
    TableDeletionService.getInstance().restore("D1");

    liveTable = loadLiveTable();
    TableDeletionService.getInstance().delete(liveTable, DELETED_AT + 1, newDeletion("D2"));

    assertThrows(
        IllegalStateException.class, () -> TableDeletionService.getInstance().restore("D1"));
    TablePO retained = TableDeletionService.getInstance().getRetainedTable("D2");
    assertNotNull(retained);
    assertEquals(table.id(), retained.getTableId());
    assertEquals("D2", retained.getDeletionId());
    assertNotNull(EntityDeletionService.getInstance().get("D2"));
    assertFalse(backend.exists(tableIdentifier, Entity.EntityType.TABLE));
  }

  @TestTemplate
  public void testLegacyMetadataGcDoesNotDeleteActiveDeletion() {
    TableDeletionService.getInstance().delete(liveTable, DELETED_AT, newDeletion("D1"));

    assertEquals(
        0, TableMetaService.getInstance().deleteTableMetasByLegacyTimeline(DELETED_AT + 1, 100));
    assertTrue(legacyRecordExistsInDB(table.id(), Entity.EntityType.TABLE));
    assertNotNull(TableDeletionService.getInstance().getRetainedTable("D1"));
    assertNotNull(EntityDeletionService.getInstance().get("D1"));
  }

  private TablePO loadLiveTable() {
    long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    return SessionUtils.doWithCommitAndFetchResult(
        TableMetaMapper.class, mapper -> mapper.selectTableMetaBySchemaIdAndName(schemaId, TABLE));
  }

  private void assertRestoreRejected(String deletionId) throws IOException {
    assertThrows(
        IllegalStateException.class, () -> TableDeletionService.getInstance().restore(deletionId));
    assertNotNull(TableDeletionService.getInstance().getRetainedTable(deletionId));
    assertNotNull(EntityDeletionService.getInstance().get(deletionId));
    assertFalse(backend.exists(tableIdentifier, Entity.EntityType.TABLE));
  }

  private List<Long> childDeletedAt(String tableName) {
    List<Long> deletedAt = new ArrayList<>();
    String sql = "SELECT deleted_at FROM " + tableName + " WHERE table_id = ? ORDER BY deleted_at";
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setLong(1, table.id());
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          deletedAt.add(resultSet.getLong("deleted_at"));
        }
      }
      return deletedAt;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to inspect table-owned metadata", e);
    }
  }

  private void updateDeletion(String sql) {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      assertEquals(1, statement.executeUpdate());
    } catch (SQLException e) {
      throw new RuntimeException("Failed to update deletion action", e);
    }
  }

  private static EntityDeletionPO newDeletion(String deletionId) {
    return EntityDeletionPO.builder()
        .deletionId(deletionId)
        .state("DELETED")
        .retentionExpiresAt(RETENTION_EXPIRES_AT)
        .build();
  }

  private static TablePO copyWithVersion(TablePO source, long version) {
    return TablePO.builder()
        .withTableId(source.getTableId())
        .withTableName(source.getTableName())
        .withMetalakeId(source.getMetalakeId())
        .withCatalogId(source.getCatalogId())
        .withSchemaId(source.getSchemaId())
        .withAuditInfo(source.getAuditInfo())
        .withCurrentVersion(version)
        .withLastVersion(source.getLastVersion())
        .withDeletedAt(source.getDeletedAt())
        .withDeletionId(source.getDeletionId())
        .build();
  }
}
