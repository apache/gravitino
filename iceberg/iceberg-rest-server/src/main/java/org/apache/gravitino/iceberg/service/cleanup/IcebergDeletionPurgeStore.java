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

package org.apache.gravitino.iceberg.service.cleanup;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.gravitino.storage.relational.mapper.EntityDeletionMapper;
import org.apache.gravitino.storage.relational.mapper.TableDeletionMapper;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.TableDeletionEntryPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * Atomic handoff from an expired retained table deletion to the Iceberg cleanup queue.
 *
 * <p>Candidate discovery is an unlocked, bounded scan. A claim then locks the deletion action
 * before the retained table row, matching UNDROP's lock order. The PENDING cleanup job and the
 * action's {@code PURGING} ownership marker commit in one relational transaction.
 */
public class IcebergDeletionPurgeStore {

  private static final String DELETED = "DELETED";

  private final IcebergCleanupJobStore cleanupJobStore;

  /**
   * Creates a retained-deletion purge store.
   *
   * @param cleanupJobStore cleanup queue persistence sharing the core relational backend
   */
  public IcebergDeletionPurgeStore(IcebergCleanupJobStore cleanupJobStore) {
    this.cleanupJobStore =
        Objects.requireNonNull(cleanupJobStore, "cleanupJobStore must not be null");
  }

  /**
   * Returns a bounded snapshot of retained table deletions eligible at {@code serverNow}.
   *
   * <p>The result is advisory. Callers may perform Iceberg context discovery from it, but must use
   * {@link #claimAndEnqueue} to revalidate and atomically claim an exact generation.
   *
   * @param serverNow authoritative server time; expiry is inclusive
   * @param limit maximum candidates to return
   * @return oldest eligible retained deletions first
   */
  public List<TableDeletionEntryPO> findEligibleDeletions(long serverNow, int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be positive");
    }
    return SessionUtils.getWithoutCommit(
        TableDeletionMapper.class,
        mapper -> mapper.selectEligibleRetainedTableDeletions(serverNow, limit));
  }

  /**
   * Returns the next bounded eligible candidate window after an ordered action cursor.
   *
   * @param serverNow authoritative server time; expiry is inclusive
   * @param afterRetentionExpiresAt previous candidate's retention deadline
   * @param afterDeletionId previous candidate's opaque deletion id
   * @param limit maximum candidates to return
   * @return eligible retained deletions after the cursor
   */
  public List<TableDeletionEntryPO> findEligibleDeletionsAfter(
      long serverNow, long afterRetentionExpiresAt, String afterDeletionId, int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be positive");
    }
    Objects.requireNonNull(afterDeletionId, "afterDeletionId must not be null");
    return SessionUtils.getWithoutCommit(
        TableDeletionMapper.class,
        mapper ->
            mapper.selectEligibleRetainedTableDeletionsAfter(
                serverNow, afterRetentionExpiresAt, afterDeletionId, limit));
  }

  /**
   * Atomically claims one candidate and enqueues its fully constructed cleanup job.
   *
   * <p>The job must carry the same immutable table and deletion identifiers as the candidate. A
   * stale candidate or a generation already won by UNDROP or another claimant returns empty. Any
   * insert or transition failure rolls back both rows.
   *
   * @param candidate candidate previously returned by {@link #findEligibleDeletions}
   * @param cleanupJob cleanup context assembled outside the row-locked transaction
   * @param serverNow authoritative server time; expiry is inclusive
   * @return the durable cleanup-job id when this caller won, otherwise empty
   */
  public Optional<Long> claimAndEnqueue(
      TableDeletionEntryPO candidate, IcebergCleanupJob cleanupJob, long serverNow) {
    validateRequest(candidate, cleanupJob);
    long cleanupJobId = cleanupJobStore.allocateJobId();
    String deletionId = candidate.getDeletion().getDeletionId();

    return SessionUtils.doWithCommitAndFetchResult(
        EntityDeletionMapper.class,
        deletionMapper -> {
          EntityDeletionPO deletion = deletionMapper.selectEntityDeletionForUpdate(deletionId);
          if (!isEligible(deletion, serverNow)) {
            return Optional.empty();
          }

          TablePO lockedTable =
              SessionUtils.getWithoutCommit(
                  TableDeletionMapper.class,
                  mapper -> mapper.selectRetainedTableForUpdate(deletionId));
          if (!matchesCandidate(lockedTable, candidate.getTable())) {
            return Optional.empty();
          }

          cleanupJobStore.insertJobWithoutCommit(cleanupJob, cleanupJobId, serverNow);
          if (deletionMapper.claimEntityDeletionForPurge(
                  deletionId, Long.toString(cleanupJobId), serverNow)
              != 1) {
            throw new IllegalStateException(
                "Deletion generation " + deletionId + " changed during purge claim");
          }
          return Optional.of(cleanupJobId);
        });
  }

  private static void validateRequest(
      TableDeletionEntryPO candidate, IcebergCleanupJob cleanupJob) {
    Objects.requireNonNull(candidate, "candidate must not be null");
    Objects.requireNonNull(cleanupJob, "cleanupJob must not be null");
    TablePO table =
        Objects.requireNonNull(candidate.getTable(), "candidate table must not be null");
    EntityDeletionPO deletion =
        Objects.requireNonNull(candidate.getDeletion(), "candidate deletion must not be null");
    String deletionId =
        Objects.requireNonNull(deletion.getDeletionId(), "candidate deletion ID must not be null");

    if (cleanupJob.id() != 0
        || !Objects.equals(cleanupJob.tableId(), table.getTableId())
        || !Objects.equals(cleanupJob.deletionId(), deletionId)
        || cleanupJob.catalogId() != table.getCatalogId()
        || !Objects.equals(cleanupJob.tableName(), table.getTableName())) {
      throw new IllegalArgumentException(
          "Cleanup job does not identify the candidate table-deletion generation");
    }
  }

  private static boolean isEligible(EntityDeletionPO deletion, long serverNow) {
    return deletion != null
        && DELETED.equals(deletion.getState())
        && deletion.getPurgeJobId() == null
        && deletion.getRetentionExpiresAt() != null
        && deletion.getRetentionExpiresAt() <= serverNow;
  }

  private static boolean matchesCandidate(TablePO locked, TablePO expected) {
    return locked != null
        && Objects.equals(locked.getTableId(), expected.getTableId())
        && Objects.equals(locked.getDeletionId(), expected.getDeletionId())
        && Objects.equals(locked.getMetalakeId(), expected.getMetalakeId())
        && Objects.equals(locked.getCatalogId(), expected.getCatalogId())
        && Objects.equals(locked.getSchemaId(), expected.getSchemaId())
        && Objects.equals(locked.getTableName(), expected.getTableName())
        && locked.getDeletedAt() > 0;
  }
}
