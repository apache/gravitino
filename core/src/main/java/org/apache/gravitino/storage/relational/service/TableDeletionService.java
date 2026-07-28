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

import java.security.Principal;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.storage.relational.mapper.TableDeletionMapper;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.TableDeletionEntryPO;
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
   * Loads the exact live table root used to identify the DELETE target.
   *
   * @param identifier live table identifier
   * @return current live table root
   */
  public TablePO getLiveTable(NameIdentifier identifier) {
    Objects.requireNonNull(identifier, "identifier must not be null");
    long tableId = EntityIdService.getEntityId(identifier, Entity.EntityType.TABLE);
    TablePO table =
        SessionUtils.doWithCommitAndFetchResult(
            TableDeletionMapper.class, mapper -> mapper.selectLiveTable(tableId));
    if (table == null) {
      throw generationChanged("unknown", "live table root is absent");
    }
    return table;
  }

  /**
   * Locks the identified live table root, creates a deletion action, and points the root to it
   * atomically.
   *
   * <p>Related table metadata remains attached to the immutable table ID. It is not independently
   * tombstoned and therefore needs no separate restore path.
   *
   * @param table expected live table identity
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
        () -> {
          SessionUtils.doWithoutCommit(
              TableDeletionMapper.class,
              mapper -> {
                TablePO locked = mapper.selectLiveTableForUpdate(table.getTableId());
                if (locked == null
                    || !Objects.equals(locked.getSchemaId(), table.getSchemaId())
                    || !Objects.equals(locked.getTableName(), table.getTableName())) {
                  throw generationChanged(
                      deletion.getDeletionId(), "locked table root does not match the target");
                }
                EntityDeletionService.getInstance().insertWithoutCommit(deletion);
                if (mapper.tombstoneTable(
                        locked.getTableId(),
                        locked.getSchemaId(),
                        locked.getTableName(),
                        locked.getCurrentVersion(),
                        deletedAt,
                        deletion.getDeletionId())
                    != 1) {
                  throw generationChanged(
                      deletion.getDeletionId(), "table root changed before delete");
                }
              });
        });
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

  /** Returns all retained table roots under one exact schema identity. */
  public List<TablePO> listRetainedTables(long schemaId) {
    return SessionUtils.doWithCommitAndFetchResult(
        TableDeletionMapper.class, mapper -> mapper.selectRetainedTables(schemaId));
  }

  /** Returns retained table roots joined to their actions under one exact schema identity. */
  public List<TableDeletionEntryPO> listRetainedTableDeletions(long schemaId) {
    return SessionUtils.doWithCommitAndFetchResult(
        TableDeletionMapper.class, mapper -> mapper.selectRetainedTableDeletions(schemaId));
  }

  /**
   * Returns one retained table root joined to its action for an exact schema identity and name.
   *
   * @param schemaId immutable parent schema identifier
   * @param tableName table name reserved by the retained root
   * @return retained table/action join, or {@code null} when the name is unreserved
   */
  @Nullable
  public TableDeletionEntryPO getRetainedTableDeletion(long schemaId, String tableName) {
    Objects.requireNonNull(tableName, "tableName must not be null");
    List<TableDeletionEntryPO> entries =
        SessionUtils.doWithCommitAndFetchResult(
            TableDeletionMapper.class,
            mapper -> mapper.selectRetainedTableDeletionsByName(schemaId, tableName));
    if (entries.size() > 1) {
      throw new IllegalStateException(
          "Multiple active deletions reserve table name " + tableName + " in schema " + schemaId);
    }
    return entries.isEmpty() ? null : entries.get(0);
  }

  /**
   * Returns the retained table root for one exact schema identity and table name.
   *
   * @return retained table root, or {@code null} when the name is unreserved
   */
  @Nullable
  public TablePO getRetainedTable(long schemaId, String tableName) {
    Objects.requireNonNull(tableName, "tableName must not be null");
    List<TablePO> tables =
        SessionUtils.doWithCommitAndFetchResult(
            TableDeletionMapper.class,
            mapper -> mapper.selectRetainedTablesByName(schemaId, tableName));
    if (tables.size() > 1) {
      throw new IllegalStateException(
          "Multiple active deletions reserve table name " + tableName + " in schema " + schemaId);
    }
    return tables.isEmpty() ? null : tables.get(0);
  }

  /** Returns all table names reserved by retained roots under one schema identity. */
  public Set<String> getReservedTableNames(long schemaId) {
    Set<String> names = new HashSet<>();
    listRetainedTables(schemaId).forEach(table -> names.add(table.getTableName()));
    return names;
  }

  /** Returns whether the current principal owns an unchanged retained table identity. */
  public boolean isRetainedOwner(long tableId, Principal principal) {
    Objects.requireNonNull(principal, "principal must not be null");
    return SessionUtils.doWithCommitAndFetchResult(
        TableDeletionMapper.class,
        mapper -> {
          String userOwner = mapper.selectRetainedUserOwnerName(tableId);
          if (Objects.equals(userOwner, principal.getName())) {
            return true;
          }
          if (!(principal instanceof UserPrincipal)) {
            return false;
          }
          String groupOwner = mapper.selectRetainedGroupOwnerName(tableId);
          return groupOwner != null
              && ((UserPrincipal) principal)
                  .getGroups().stream()
                      .anyMatch(group -> Objects.equals(groupOwner, group.getGroupName()));
        });
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
