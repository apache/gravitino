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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Cross-backend tests for exact-generation table tombstone and restore transactions. */
public class TestTableDeletionService extends TestJDBCBackend {

  private static final String METALAKE = "deletion_metalake";
  private static final String CATALOG = "deletion_catalog";
  private static final String SCHEMA = "deletion_schema";
  private static final String TABLE = "orders";
  private static final long DELETED_AT = 1_784_800_000_000L;
  private static final long RESTORED_AT = DELETED_AT + 1_000L;

  private NameIdentifier tableIdentifier;
  private TableEntity table;

  @BeforeEach
  public void createTable() throws IOException {
    createParentEntities(METALAKE, CATALOG, SCHEMA, AUDIT_INFO);
    Namespace namespace = NamespaceUtil.ofTable(METALAKE, CATALOG, SCHEMA);
    tableIdentifier = NameIdentifier.of(namespace, TABLE);
    table = createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, TABLE, AUDIT_INFO);
    backend.insert(table, false);
  }

  @TestTemplate
  public void testDeleteAndRestoreExactGeneration() throws IOException {
    EntityDeletionPO deletion = delete("D1", "active-name");

    assertFalse(backend.exists(tableIdentifier, Entity.EntityType.TABLE));
    assertEquals(
        deletion.getDeletionId(),
        EntityDeletionService.getInstance()
            .getByActiveName(deletion.getActiveNameKey())
            .getDeletionId());

    AtomicReference<TablePO> restored = new AtomicReference<>();
    SessionUtils.doMultipleWithCommit(
        () -> {
          EntityDeletionPO locked =
              EntityDeletionService.getInstance().getForUpdate(deletion.getDeletionId());
          assertNotNull(locked);
          assertTrue(
              EntityDeletionService.getInstance()
                  .restore(
                      locked.getDeletionId(), locked.getRevision(), RESTORED_AT, "etag-D1-r0"));
          restored.set(TableDeletionService.getInstance().restore(locked, RESTORED_AT));
        });

    assertEquals(table.id(), restored.get().getTableId());
    assertTrue(backend.exists(tableIdentifier, Entity.EntityType.TABLE));
    assertNull(EntityDeletionService.getInstance().getByActiveName(deletion.getActiveNameKey()));
    EntityDeletionPO receipt = EntityDeletionService.getInstance().get(deletion.getDeletionId());
    assertEquals("RESTORED", receipt.getState());
    assertEquals(1L, receipt.getRevision());
    assertEquals("etag-D1-r0", receipt.getAcceptedRestoreEtag());
  }

  @TestTemplate
  public void testDeleteTransactionRollsBackAsOneUnit() throws IOException {
    assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> {
                  TablePO locked =
                      TableDeletionService.getInstance().lockLiveTable(tableIdentifier);
                  EntityDeletionPO deletion = newDeletion("D-rollback", "rollback-name", locked);
                  TableDeletionService.getInstance()
                      .tombstone(locked, deletion.getDeletedAt(), deletion.getDeletionId());
                  EntityDeletionService.getInstance().insert(deletion);
                  throw new IllegalStateException("force rollback");
                }));

    assertTrue(backend.exists(tableIdentifier, Entity.EntityType.TABLE));
    assertNull(EntityDeletionService.getInstance().get("D-rollback"));
  }

  @TestTemplate
  public void testRestoreCannotCrossDeletionGeneration() throws IOException {
    EntityDeletionPO deletion = delete("D1", "generation-name");
    EntityDeletionPO wrongGeneration = newDeletion("D2", "other-name", tablePO(deletion));

    assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> TableDeletionService.getInstance().restore(wrongGeneration, RESTORED_AT)));

    assertFalse(backend.exists(tableIdentifier, Entity.EntityType.TABLE));
    assertEquals("DELETED", EntityDeletionService.getInstance().get("D1").getState());
  }

  @TestTemplate
  public void testRestoreCasRejectsStaleRevisionAndExpiredAction() throws IOException {
    EntityDeletionPO deletion = delete("D1", "cas-name");

    SessionUtils.doMultipleWithCommit(
        () ->
            assertFalse(
                EntityDeletionService.getInstance()
                    .restore("D1", deletion.getRevision() + 1, RESTORED_AT, "stale")));
    SessionUtils.doMultipleWithCommit(
        () ->
            assertFalse(
                EntityDeletionService.getInstance()
                    .restore(
                        "D1",
                        deletion.getRevision(),
                        deletion.getRetentionExpiresAt(),
                        "expired")));

    assertFalse(backend.exists(tableIdentifier, Entity.EntityType.TABLE));
    assertEquals("DELETED", EntityDeletionService.getInstance().get("D1").getState());
  }

  @TestTemplate
  public void testActiveNameReservationIsUnique() throws IOException {
    EntityDeletionPO deletion = delete("D1", "reserved-name");
    EntityDeletionPO duplicate =
        EntityDeletionPO.builder()
            .deletionId("D2")
            .entityType("TABLE")
            .entityId(RandomIdGenerator.INSTANCE.nextId())
            .entityVersion(0L)
            .metalakeId(deletion.getMetalakeId())
            .catalogId(deletion.getCatalogId())
            .parentId(deletion.getParentId())
            .namespaceSnapshot(deletion.getNamespaceSnapshot())
            .entityNameSnapshot(deletion.getEntityNameSnapshot())
            .activeNameKey(deletion.getActiveNameKey())
            .state("DELETED")
            .revision(0L)
            .deletedAt(DELETED_AT + 1)
            .retentionExpiresAt(DELETED_AT + 10_000L)
            .deletedBy("bob")
            .purgeRequested(false)
            .purgeJobType("ICEBERG_REST_PURGE")
            .cleanupStatus("PENDING")
            .cleanupAttemptCount(0)
            .updatedAt(DELETED_AT + 1)
            .build();

    assertThrows(
        RuntimeException.class, () -> EntityDeletionService.getInstance().insert(duplicate));
    assertEquals(
        "D1",
        EntityDeletionService.getInstance()
            .getByActiveName(deletion.getActiveNameKey())
            .getDeletionId());
  }

  private EntityDeletionPO delete(String deletionId, String activeNameKey) {
    AtomicReference<EntityDeletionPO> result = new AtomicReference<>();
    SessionUtils.doMultipleWithCommit(
        () -> {
          TablePO locked = TableDeletionService.getInstance().lockLiveTable(tableIdentifier);
          EntityDeletionPO deletion = newDeletion(deletionId, activeNameKey, locked);
          TableDeletionService.getInstance()
              .tombstone(locked, deletion.getDeletedAt(), deletion.getDeletionId());
          EntityDeletionService.getInstance().insert(deletion);
          result.set(deletion);
        });
    return result.get();
  }

  private static EntityDeletionPO newDeletion(
      String deletionId, String activeNameKey, TablePO tablePO) {
    return EntityDeletionPO.builder()
        .deletionId(deletionId)
        .entityType("TABLE")
        .entityId(tablePO.getTableId())
        .entityVersion(tablePO.getCurrentVersion())
        .metalakeId(tablePO.getMetalakeId())
        .catalogId(tablePO.getCatalogId())
        .parentId(tablePO.getSchemaId())
        .namespaceSnapshot(METALAKE + "." + CATALOG + "." + SCHEMA)
        .entityNameSnapshot(tablePO.getTableName())
        .activeNameKey(activeNameKey)
        .state("DELETED")
        .revision(0L)
        .deletedAt(DELETED_AT)
        .retentionExpiresAt(DELETED_AT + 10_000L)
        .deletedBy("alice")
        .purgeRequested(false)
        .purgeJobType("ICEBERG_REST_PURGE")
        .cleanupStatus("PENDING")
        .cleanupAttemptCount(0)
        .updatedAt(DELETED_AT)
        .build();
  }

  private static TablePO tablePO(EntityDeletionPO deletion) {
    return TablePO.builder()
        .withTableId(deletion.getEntityId())
        .withTableName(deletion.getEntityNameSnapshot())
        .withMetalakeId(deletion.getMetalakeId())
        .withCatalogId(deletion.getCatalogId())
        .withSchemaId(deletion.getParentId())
        .withAuditInfo("{}")
        .withCurrentVersion(deletion.getEntityVersion())
        .withLastVersion(deletion.getEntityVersion())
        .withDeletedAt(deletion.getDeletedAt())
        .withDeletionId(deletion.getDeletionId())
        .build();
  }
}
