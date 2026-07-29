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
  private static final String PURGE_JOB_ID = "101";
  private static final long DELETED_AT = 1_784_800_000_000L;
  private static final long RETENTION_EXPIRES_AT = Long.MAX_VALUE;
  private static final long OTHER_OBJECT_ID = 91_099L;
  private static final long TAG_ID = 91_001L;
  private static final long POLICY_ID = 91_002L;
  private static final long ROLE_ID = 91_003L;

  private NameIdentifier tableIdentifier;
  private TableEntity table;
  private TablePO liveTable;
  private long columnId;

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
    columnId = column.id();
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

  @TestTemplate
  public void testFinalizePurgeDeletesOnlyTheOwnedGeneration() throws IOException {
    TableDeletionService.getInstance().delete(liveTable, DELETED_AT, newDeletion("D1"));
    markPurging("D1", PURGE_JOB_ID);
    insertPurgeFixtures("D1", Long.parseLong(PURGE_JOB_ID));

    assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () ->
                    TableDeletionService.getInstance()
                        .finalizePurge(table.id(), "D1", PURGE_JOB_ID),
                () -> {
                  throw new IllegalStateException("force outer transaction rollback");
                }));

    assertPurgeTargetPresent("D1");

    TableDeletionService.getInstance().finalizePurge(table.id(), "D1", PURGE_JOB_ID);

    assertEquals(0, rowCount("SELECT COUNT(*) FROM table_meta WHERE table_id = ?", table.id()));
    assertEquals(0, rowCount("SELECT COUNT(*) FROM entity_deletion WHERE deletion_id = ?", "D1"));
    assertEquals(
        0, rowCount("SELECT COUNT(*) FROM table_version_info WHERE table_id = ?", table.id()));
    assertEquals(
        0,
        rowCount("SELECT COUNT(*) FROM table_column_version_info WHERE table_id = ?", table.id()));
    assertEquals(
        0,
        rowCount("SELECT COUNT(*) FROM partition_statistic_meta WHERE table_id = ?", table.id()));
    assertOwnedRelations(0);

    assertEquals(
        1,
        rowCount("SELECT COUNT(*) FROM owner_meta WHERE metadata_object_id = ?", OTHER_OBJECT_ID));
    assertEquals(
        1,
        rowCount(
            "SELECT COUNT(*) FROM tag_relation_meta WHERE metadata_object_id = ?",
            OTHER_OBJECT_ID));
    assertEquals(
        1,
        rowCount(
            "SELECT COUNT(*) FROM policy_relation_meta WHERE metadata_object_id = ?",
            OTHER_OBJECT_ID));
    assertEquals(
        1,
        rowCount(
            "SELECT COUNT(*) FROM statistic_meta WHERE metadata_object_id = ?", OTHER_OBJECT_ID));
    assertEquals(
        1,
        rowCount(
            "SELECT COUNT(*) FROM role_meta_securable_object WHERE metadata_object_id = ?",
            OTHER_OBJECT_ID));
    assertEquals(
        1,
        rowCount(
            "SELECT COUNT(*) FROM partition_statistic_meta WHERE table_id = ?", OTHER_OBJECT_ID));

    assertEquals(1, rowCount("SELECT COUNT(*) FROM tag_meta WHERE tag_id = ?", TAG_ID));
    assertEquals(1, rowCount("SELECT COUNT(*) FROM policy_meta WHERE policy_id = ?", POLICY_ID));
    assertEquals(1, rowCount("SELECT COUNT(*) FROM role_meta WHERE role_id = ?", ROLE_ID));
    assertEquals(
        1,
        rowCount(
            "SELECT COUNT(*) FROM iceberg_cleanup_job WHERE id = ?", Long.parseLong(PURGE_JOB_ID)));
    assertEquals(
        1,
        rowCount(
            "SELECT COUNT(*) FROM entity_change_log WHERE entity_full_name = ?",
            "purge.audit.marker"));
    assertEquals(
        1,
        rowCount(
            "SELECT COUNT(*) FROM table_metrics WHERE table_identifier = ?",
            "purge.metric.marker"));
  }

  @TestTemplate
  public void testFinalizePurgeRejectsWrongJobOrTableGeneration() throws IOException {
    TableDeletionService.getInstance().delete(liveTable, DELETED_AT, newDeletion("D1"));
    markPurging("D1", PURGE_JOB_ID);

    assertThrows(
        IllegalStateException.class,
        () -> TableDeletionService.getInstance().finalizePurge(table.id(), "D1", "102"));
    assertThrows(
        IllegalStateException.class,
        () -> TableDeletionService.getInstance().finalizePurge(table.id() + 1, "D1", PURGE_JOB_ID));

    assertNotNull(TableDeletionService.getInstance().getRetainedTable("D1"));
    assertNotNull(EntityDeletionService.getInstance().get("D1"));
    assertEquals(
        1, rowCount("SELECT COUNT(*) FROM table_version_info WHERE table_id = ?", table.id()));
    assertEquals(
        1,
        rowCount("SELECT COUNT(*) FROM table_column_version_info WHERE table_id = ?", table.id()));
  }

  private TablePO loadLiveTable() {
    long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    return SessionUtils.doWithCommitAndFetchResult(
        TableMetaMapper.class, mapper -> mapper.selectTableMetaBySchemaIdAndName(schemaId, TABLE));
  }

  private void insertPurgeFixtures(String deletionId, long purgeJobId) {
    executeUpdate(
        "INSERT INTO tag_meta (tag_id, tag_name, metalake_id, audit_info)" + " VALUES (?, ?, ?, ?)",
        TAG_ID,
        "purge_tag",
        liveTable.getMetalakeId(),
        "{}");
    executeUpdate(
        "INSERT INTO policy_meta"
            + " (policy_id, policy_name, policy_type, metalake_id, audit_info)"
            + " VALUES (?, ?, ?, ?, ?)",
        POLICY_ID,
        "purge_policy",
        "custom",
        liveTable.getMetalakeId(),
        "{}");
    executeUpdate(
        "INSERT INTO role_meta (role_id, role_name, metalake_id, audit_info)"
            + " VALUES (?, ?, ?, ?)",
        ROLE_ID,
        "purge_role",
        liveTable.getMetalakeId(),
        "{}");

    long[] objectIds = {table.id(), columnId, OTHER_OBJECT_ID};
    String[] objectTypes = {"TABLE", "COLUMN", "TABLE"};
    for (int index = 0; index < objectIds.length; index++) {
      executeUpdate(
          "INSERT INTO owner_meta"
              + " (metalake_id, owner_id, owner_type, metadata_object_id,"
              + " metadata_object_type, audit_info) VALUES (?, ?, ?, ?, ?, ?)",
          liveTable.getMetalakeId(),
          92_000L + index,
          "USER",
          objectIds[index],
          objectTypes[index],
          "{}");
      executeUpdate(
          "INSERT INTO tag_relation_meta"
              + " (tag_id, metadata_object_id, metadata_object_type, audit_info)"
              + " VALUES (?, ?, ?, ?)",
          TAG_ID,
          objectIds[index],
          objectTypes[index],
          "{}");
      executeUpdate(
          "INSERT INTO policy_relation_meta"
              + " (policy_id, metadata_object_id, metadata_object_type, audit_info)"
              + " VALUES (?, ?, ?, ?)",
          POLICY_ID,
          objectIds[index],
          objectTypes[index],
          "{}");
      executeUpdate(
          "INSERT INTO statistic_meta"
              + " (statistic_id, statistic_name, metalake_id, statistic_value,"
              + " metadata_object_id, metadata_object_type, audit_info)"
              + " VALUES (?, ?, ?, ?, ?, ?, ?)",
          93_000L + index,
          "row_count",
          liveTable.getMetalakeId(),
          "1",
          objectIds[index],
          objectTypes[index],
          "{}");
      executeUpdate(
          "INSERT INTO role_meta_securable_object"
              + " (role_id, metadata_object_id, type, privilege_names, privilege_conditions)"
              + " VALUES (?, ?, ?, ?, ?)",
          ROLE_ID,
          objectIds[index],
          objectTypes[index],
          "[]",
          "[]");
    }

    executeUpdate(
        "INSERT INTO partition_statistic_meta"
            + " (table_id, partition_name, statistic_name, statistic_value, audit_info,"
            + " created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        table.id(),
        "p=target",
        "row_count",
        "1",
        "{}",
        DELETED_AT,
        DELETED_AT);
    executeUpdate(
        "INSERT INTO partition_statistic_meta"
            + " (table_id, partition_name, statistic_name, statistic_value, audit_info,"
            + " created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        OTHER_OBJECT_ID,
        "p=other",
        "row_count",
        "1",
        "{}",
        DELETED_AT,
        DELETED_AT);
    executeUpdate(
        "INSERT INTO iceberg_cleanup_job"
            + " (id, table_id, deletion_id, catalog_id, namespace, table_name,"
            + " metadata_location, file_io_impl, file_io_props, state, attempts, heartbeat_at,"
            + " created_by, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        purgeJobId,
        table.id(),
        deletionId,
        liveTable.getCatalogId(),
        SCHEMA,
        TABLE,
        "file:/tmp/metadata.json",
        "org.example.FileIO",
        "{}",
        "RUNNING",
        1,
        DELETED_AT,
        "purge-test",
        DELETED_AT);
    executeUpdate(
        "INSERT INTO entity_change_log"
            + " (metalake_name, entity_type, entity_full_name, operate_type, created_at)"
            + " VALUES (?, ?, ?, ?, ?)",
        METALAKE,
        "TABLE",
        "purge.audit.marker",
        2,
        DELETED_AT);
    executeUpdate(
        "INSERT INTO table_metrics"
            + " (table_identifier, metric_name, metric_ts, metric_value) VALUES (?, ?, ?, ?)",
        "purge.metric.marker",
        "files",
        1,
        "1");
  }

  private void assertPurgeTargetPresent(String deletionId) {
    assertEquals(1, rowCount("SELECT COUNT(*) FROM table_meta WHERE table_id = ?", table.id()));
    assertEquals(
        1, rowCount("SELECT COUNT(*) FROM entity_deletion WHERE deletion_id = ?", deletionId));
    assertEquals(
        1, rowCount("SELECT COUNT(*) FROM table_version_info WHERE table_id = ?", table.id()));
    assertEquals(
        1,
        rowCount("SELECT COUNT(*) FROM table_column_version_info WHERE table_id = ?", table.id()));
    assertEquals(
        1,
        rowCount("SELECT COUNT(*) FROM partition_statistic_meta WHERE table_id = ?", table.id()));
    assertOwnedRelations(2);
  }

  private void assertOwnedRelations(int expected) {
    String relationPredicate = " WHERE metadata_object_id IN (?, ?)";
    assertEquals(
        expected,
        rowCount("SELECT COUNT(*) FROM owner_meta" + relationPredicate, table.id(), columnId));
    assertEquals(
        expected,
        rowCount(
            "SELECT COUNT(*) FROM tag_relation_meta" + relationPredicate, table.id(), columnId));
    assertEquals(
        expected,
        rowCount(
            "SELECT COUNT(*) FROM policy_relation_meta" + relationPredicate, table.id(), columnId));
    assertEquals(
        expected,
        rowCount("SELECT COUNT(*) FROM statistic_meta" + relationPredicate, table.id(), columnId));
    assertEquals(
        expected,
        rowCount(
            "SELECT COUNT(*) FROM role_meta_securable_object" + relationPredicate,
            table.id(),
            columnId));
  }

  private void markPurging(String deletionId, String purgeJobId) {
    executeUpdate(
        "UPDATE entity_deletion SET state = 'PURGING', purge_job_id = ?" + " WHERE deletion_id = ?",
        purgeJobId,
        deletionId);
  }

  private void executeUpdate(String sql, Object... parameters) {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int index = 0; index < parameters.length; index++) {
        statement.setObject(index + 1, parameters[index]);
      }
      assertEquals(1, statement.executeUpdate());
    } catch (SQLException e) {
      throw new RuntimeException("Failed to prepare purge test metadata", e);
    }
  }

  private int rowCount(String sql, Object... parameters) {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int index = 0; index < parameters.length; index++) {
        statement.setObject(index + 1, parameters[index]);
      }
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        return resultSet.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to inspect purge test metadata", e);
    }
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
