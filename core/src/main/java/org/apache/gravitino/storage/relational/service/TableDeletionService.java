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
import org.apache.gravitino.storage.relational.mapper.TableDeletionMapper;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Atomic relational operations for an active table deletion. */
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
   * Creates a deletion action and points the exact live table root to it atomically.
   *
   * <p>Related table metadata remains attached to the immutable table ID. It is not independently
   * tombstoned and therefore needs no separate restore path.
   *
   * @param table expected live table root
   * @param deletedAt authoritative deletion time
   * @param deletion active deletion action
   */
  public void delete(TablePO table, long deletedAt, EntityDeletionPO deletion) {
    Objects.requireNonNull(table, "table must not be null");
    Objects.requireNonNull(deletion, "deletion must not be null");
    Objects.requireNonNull(deletion.getDeletionId(), "deletion ID must not be null");
    Objects.requireNonNull(deletion.getRetentionExpiresAt(), "retention deadline must not be null");
    if (deletedAt <= 0
        || deletion.getRetentionExpiresAt() < deletedAt
        || !"DELETED".equals(deletion.getState())
        || deletion.getPurgeJobId() != null) {
      throw new IllegalArgumentException("Deletion action is not a valid initial action");
    }

    SessionUtils.doMultipleWithCommit(
        () -> EntityDeletionService.getInstance().insertWithoutCommit(deletion),
        () ->
            SessionUtils.doWithoutCommit(
                TableDeletionMapper.class,
                mapper -> {
                  int updated =
                      mapper.tombstoneTable(
                          table.getTableId(),
                          table.getSchemaId(),
                          table.getTableName(),
                          table.getCurrentVersion(),
                          deletedAt,
                          deletion.getDeletionId());
                  if (updated != 1) {
                    throw generationChanged(
                        deletion.getDeletionId(), "table root changed before delete");
                  }
                }));
  }

  /**
   * Returns the table root associated with an exact active deletion.
   *
   * @param deletionId opaque deletion identifier
   * @return retained table root, or {@code null} when absent
   */
  @Nullable
  public TablePO getRetainedTable(String deletionId) {
    Objects.requireNonNull(deletionId, "deletionId must not be null");
    return SessionUtils.doWithCommitAndFetchResult(
        TableDeletionMapper.class, mapper -> mapper.selectRetainedTable(deletionId));
  }

  /**
   * Reactivates an exact retained table and consumes its deletion action atomically.
   *
   * <p>The exact action row is locked before the table root. Future purge claiming must use the
   * same lock order, so only UNDROP or purge can own a deletion at one time.
   *
   * @param deletionId opaque deletion identifier
   * @return the reactivated table root
   */
  public TablePO restore(String deletionId) {
    Objects.requireNonNull(deletionId, "deletionId must not be null");
    return SessionUtils.doWithCommitAndFetchResult(
        TableDeletionMapper.class,
        mapper -> {
          EntityDeletionPO deletion = EntityDeletionService.getInstance().getForUpdate(deletionId);
          long serverNow = System.currentTimeMillis();
          if (deletion == null
              || !"DELETED".equals(deletion.getState())
              || deletion.getRetentionExpiresAt() <= serverNow
              || deletion.getPurgeJobId() != null) {
            throw generationChanged(deletionId, "deletion action is not recoverable");
          }
          TablePO table = mapper.selectRetainedTableForUpdate(deletionId);
          if (table == null) {
            throw generationChanged(deletionId, "retained table root is absent");
          }
          if (mapper.restoreTable(table.getTableId(), deletionId) != 1) {
            throw generationChanged(deletionId, "table root changed before restore");
          }
          if (!EntityDeletionService.getInstance().delete(deletionId)) {
            throw generationChanged(deletionId, "deletion action changed before restore");
          }
          TablePO restored = mapper.selectLiveTable(table.getTableId());
          if (restored == null) {
            throw generationChanged(deletionId, "restored table root is not visible");
          }
          return restored;
        });
  }

  private static IllegalStateException generationChanged(String deletionId, String reason) {
    return new IllegalStateException(
        "Deletion generation " + deletionId + " cannot be changed: " + reason);
  }
}
