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

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.iceberg.service.cleanup.mapper.IcebergCleanupJobMapper;
import org.apache.gravitino.iceberg.service.cleanup.po.IcebergCleanupJobPO;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * Persistence for {@code iceberg_cleanup_job}, layered on the Gravitino entity store's shared
 * relational backend. Async cleanup reuses the entity store's connection pool, transaction
 * management, and per-backend SQL dispatch instead of opening its own JDBC connections. Row ids and
 * timestamps are supplied by the application, keeping the SQL portable across H2, MySQL, and
 * PostgreSQL.
 */
public class IcebergCleanupJobStore {

  private static final int MAX_ERROR_LENGTH = 2048;

  private final IdGenerator idGenerator;

  /**
   * Creates a cleanup job store.
   *
   * @param idGenerator generator for new row ids
   */
  public IcebergCleanupJobStore(IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
  }

  /**
   * Persists a new PENDING job.
   *
   * @param job job to persist
   * @return generated id
   */
  public long addJob(IcebergCleanupJob job) {
    long id = idGenerator.nextId();
    long now = System.currentTimeMillis();
    IcebergCleanupJobPO po = IcebergCleanupJobPO.fromCleanupJob(job, id, now);
    SessionUtils.doWithCommit(IcebergCleanupJobMapper.class, mapper -> mapper.insertCleanupJob(po));
    return id;
  }

  /**
   * Scans a small candidate window and takes the first available row via compare-and-swap.
   *
   * @param now current epoch millis, written as the initial heartbeat
   * @param heartbeatTimeoutMs age past which a RUNNING heartbeat is stale
   * @param window max candidates to consider
   * @return the taken job, or {@link Optional#empty()} if nothing was available
   */
  public Optional<IcebergCleanupJob> takePendingJob(long now, long heartbeatTimeoutMs, int window) {
    long heartbeatExpiry = now - heartbeatTimeoutMs;
    List<IcebergCleanupJobPO> candidates =
        SessionUtils.getWithoutCommit(
            IcebergCleanupJobMapper.class,
            mapper -> mapper.selectCandidateJobs(heartbeatExpiry, window));
    for (IcebergCleanupJobPO po : candidates) {
      long id = po.getId();
      int marked =
          SessionUtils.doWithCommitAndFetchResult(
              IcebergCleanupJobMapper.class,
              mapper -> mapper.markRunning(id, now, heartbeatExpiry));
      if (marked == 1) {
        // The claim only flips mutable columns (state, heartbeat_at, updated_at); everything
        // toCleanupJob reads was fixed at enqueue, so the candidate snapshot is still accurate.
        return Optional.of(po.toCleanupJob());
      }
    }
    return Optional.empty();
  }

  /**
   * Marks a RUNNING job SUCCEEDED, only if the caller still owns it.
   *
   * @param id job id
   * @param heartbeat the caller's heartbeat token; the update applies only if the row's {@code
   *     heartbeat_at} still matches, so a reclaimed worker cannot flip a job a peer now owns
   * @return {@code true} iff the row was updated (still RUNNING and owned by the caller)
   */
  public boolean markSucceeded(long id, long heartbeat) {
    long now = System.currentTimeMillis();
    return SessionUtils.doWithCommitAndFetchResult(
            IcebergCleanupJobMapper.class,
            mapper ->
                mapper.markFinished(
                    id, IcebergCleanupJob.State.SUCCEEDED.name(), null, now, heartbeat))
        > 0;
  }

  /**
   * Records a transient failure, only if the caller still owns the job: {@code attempts++}, then
   * FAILED at the ceiling else PENDING.
   *
   * @param id job id
   * @param reason failure text
   * @param maxAttempts ceiling from config
   * @param heartbeat the caller's heartbeat token; the update applies only if the row's {@code
   *     heartbeat_at} still matches, so a reclaimed worker cannot disturb a job a peer now owns
   * @return {@code true} iff the row was updated (still RUNNING and owned by the caller)
   */
  public boolean recordFailure(long id, String reason, int maxAttempts, long heartbeat) {
    long now = System.currentTimeMillis();
    String err = truncate(reason);
    return SessionUtils.doWithCommitAndFetchResult(
            IcebergCleanupJobMapper.class,
            mapper -> mapper.recordFailure(id, err, maxAttempts, now, heartbeat))
        > 0;
  }

  /**
   * Refreshes a heartbeat with compare-and-swap ownership check.
   *
   * @param id job id
   * @param lastHeartbeat previous heartbeat value
   * @param now new heartbeat value
   * @return {@code true} iff the row was still owned by the caller
   */
  public boolean heartbeat(long id, long lastHeartbeat, long now) {
    return SessionUtils.doWithCommitAndFetchResult(
            IcebergCleanupJobMapper.class, mapper -> mapper.heartbeat(id, lastHeartbeat, now))
        > 0;
  }

  /**
   * Finds the id of an unfinished (PENDING or RUNNING) cleanup job for the identifier, if any.
   *
   * @param catalogId globally unique id of the owning catalog
   * @param namespace table namespace
   * @param table table name
   * @return the unfinished job id, or {@link Optional#empty()} if none exists
   */
  public Optional<Long> findUnfinishedJobId(long catalogId, String namespace, String table) {
    return Optional.ofNullable(
        SessionUtils.getWithoutCommit(
            IcebergCleanupJobMapper.class,
            mapper -> mapper.selectUnfinishedJobId(catalogId, namespace, table)));
  }

  /**
   * Deletes finished (SUCCEEDED or FAILED) jobs whose last update predates the timeline.
   *
   * @param legacyTimeline cutoff epoch millis; rows updated before this are removed
   * @return rows deleted
   */
  public int deleteFinishedJobsByLegacyTimeline(long legacyTimeline) {
    return SessionUtils.doWithCommitAndFetchResult(
        IcebergCleanupJobMapper.class,
        mapper -> mapper.deleteFinishedJobsByLegacyTimeline(legacyTimeline));
  }

  /**
   * Reads a job state for tests.
   *
   * @param id job id
   * @return its current state
   * @throws IllegalStateException if the row is gone
   */
  @VisibleForTesting
  IcebergCleanupJob.State stateOf(long id) {
    String state =
        SessionUtils.getWithoutCommit(
            IcebergCleanupJobMapper.class, mapper -> mapper.selectState(id));
    if (state == null) {
      throw new IllegalStateException("No cleanup job " + id);
    }
    return IcebergCleanupJob.State.valueOf(state);
  }

  private static String truncate(String value) {
    return value == null || value.length() <= MAX_ERROR_LENGTH
        ? value
        : value.substring(0, MAX_ERROR_LENGTH);
  }
}
