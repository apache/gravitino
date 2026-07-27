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

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.storage.relational.mapper.TableDeletionMapper;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Relational primitives for one table deletion generation. */
public class TableDeletionService {

  private static final TableDeletionService INSTANCE = new TableDeletionService();

  /**
   * Returns the singleton table deletion service.
   *
   * @return table deletion service
   */
  public static TableDeletionService getInstance() {
    return INSTANCE;
  }

  private TableDeletionService() {}

  /**
   * Locks and returns the live table addressed by an Iceberg deletion request.
   *
   * <p>The caller must own an outer transaction. Locking the parent namespace serializes same-name
   * create, delete, and restore operations across server nodes.
   *
   * @param identifier full Gravitino table identifier
   * @return locked live table
   */
  public TablePO lockLiveTable(NameIdentifier identifier) {
    NameIdentifierUtil.checkTable(identifier);
    long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(identifier.namespace().levels()), Entity.EntityType.SCHEMA);
    return SessionUtils.getWithoutCommit(
        TableDeletionMapper.class,
        mapper -> {
          if (mapper.lockLiveSchema(schemaId) == null) {
            throw new NoSuchEntityException(
                NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
                Entity.EntityType.SCHEMA.name().toLowerCase(),
                identifier.namespace());
          }
          TablePO table = mapper.selectLiveTableForUpdate(schemaId, identifier.name());
          if (table == null) {
            throw new NoSuchEntityException(
                NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
                Entity.EntityType.TABLE.name().toLowerCase(),
                identifier.name());
          }
          return table;
        });
  }

  /**
   * Tombstones the locked table and every table-owned row using one deletion identifier.
   *
   * <p>The caller must own an outer transaction. Failure of any required exact-row predicate aborts
   * that transaction; no partial generation can become visible.
   *
   * @param table locked live table
   * @param deletedAt server deletion time
   * @param deletionId opaque deletion identifier
   */
  public void tombstone(TablePO table, long deletedAt, String deletionId) {
    Objects.requireNonNull(table, "table must not be null");
    Objects.requireNonNull(deletionId, "deletionId must not be null");
    SessionUtils.doWithoutCommit(
        TableDeletionMapper.class,
        mapper -> {
          int root =
              mapper.tombstoneTable(
                  table.getTableId(),
                  table.getSchemaId(),
                  table.getTableName(),
                  table.getCurrentVersion(),
                  deletedAt,
                  deletionId);
          if (root != 1) {
            throw generationChanged(deletionId, "table row changed before delete");
          }

          mapper.tombstoneOwnerRelations(table.getTableId(), deletedAt, deletionId);
          mapper.tombstoneColumns(table.getTableId(), deletedAt, deletionId);
          mapper.tombstoneSecurableObjects(table.getTableId(), deletedAt, deletionId);
          mapper.tombstoneTagRelations(table.getTableId(), deletedAt, deletionId);
          mapper.tombstoneStatistics(table.getTableId(), deletedAt, deletionId);
          mapper.tombstonePolicyRelations(table.getTableId(), deletedAt, deletionId);
          if (mapper.tombstoneTableVersion(
                  table.getTableId(), table.getCurrentVersion(), deletedAt, deletionId)
              != 1) {
            throw generationChanged(deletionId, "current table version changed before delete");
          }
        });
  }

  /**
   * Reactivates only rows stamped by the exact deletion action.
   *
   * <p>The caller must own an outer transaction and must lock the action first. This method never
   * resolves by name alone and cannot reactivate a later same-name table.
   *
   * @param deletion exact deletion action
   * @param restoredAt server restore time
   * @return restored table row
   */
  public TablePO restore(EntityDeletionPO deletion, long restoredAt) {
    Objects.requireNonNull(deletion, "deletion must not be null");
    return SessionUtils.getWithoutCommit(
        TableDeletionMapper.class,
        mapper -> {
          if (mapper.lockLiveSchema(deletion.getParentId()) == null) {
            throw generationChanged(deletion.getDeletionId(), "parent namespace is not live");
          }

          TablePO occupied =
              mapper.selectLiveTableForUpdate(
                  deletion.getParentId(), deletion.getEntityNameSnapshot());
          if (occupied != null) {
            throw generationChanged(deletion.getDeletionId(), "table name is already occupied");
          }

          TablePO table =
              mapper.selectTableGenerationForUpdate(
                  deletion.getEntityId(), deletion.getDeletionId());
          if (table == null
              || !Objects.equals(table.getSchemaId(), deletion.getParentId())
              || !Objects.equals(table.getTableName(), deletion.getEntityNameSnapshot())
              || !Objects.equals(table.getCurrentVersion(), deletion.getEntityVersion())) {
            throw generationChanged(deletion.getDeletionId(), "table generation does not match");
          }

          mapper.restoreOwnerRelations(
              deletion.getEntityId(), deletion.getDeletionId(), restoredAt);
          mapper.restoreColumns(deletion.getEntityId(), deletion.getDeletionId());
          mapper.restoreSecurableObjects(deletion.getEntityId(), deletion.getDeletionId());
          mapper.restoreTagRelations(deletion.getEntityId(), deletion.getDeletionId());
          mapper.restoreStatistics(deletion.getEntityId(), deletion.getDeletionId());
          mapper.restorePolicyRelations(deletion.getEntityId(), deletion.getDeletionId());
          if (mapper.restoreTableVersion(
                  deletion.getEntityId(), deletion.getEntityVersion(), deletion.getDeletionId())
              != 1) {
            throw generationChanged(
                deletion.getDeletionId(), "table version no longer matches deletion");
          }
          if (mapper.restoreTable(
                  deletion.getEntityId(),
                  deletion.getParentId(),
                  deletion.getEntityNameSnapshot(),
                  deletion.getEntityVersion(),
                  deletion.getDeletionId())
              != 1) {
            throw generationChanged(deletion.getDeletionId(), "table row restore lost its CAS");
          }

          TablePO restored =
              mapper.selectRestoredTable(
                  deletion.getEntityId(), deletion.getParentId(), deletion.getEntityNameSnapshot());
          if (restored == null) {
            throw generationChanged(deletion.getDeletionId(), "restored table is not visible");
          }
          return restored;
        });
  }

  /**
   * Finds a previously restored exact generation for idempotent replay.
   *
   * @param deletion deletion action receipt
   * @return current live row, or {@code null} when the exact identity is no longer live
   */
  @Nullable
  public TablePO getRestoredTable(EntityDeletionPO deletion) {
    Objects.requireNonNull(deletion, "deletion must not be null");
    return SessionUtils.getWithoutCommit(
        TableDeletionMapper.class,
        mapper ->
            mapper.selectRestoredTable(
                deletion.getEntityId(), deletion.getParentId(), deletion.getEntityNameSnapshot()));
  }

  private static IllegalStateException generationChanged(String deletionId, String reason) {
    return new IllegalStateException(
        "Deletion generation " + deletionId + " cannot be changed: " + reason);
  }
}
