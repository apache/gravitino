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
import javax.annotation.Nullable;
import org.apache.gravitino.iceberg.service.cleanup.mapper.IcebergCleanupJobMapper;
import org.apache.gravitino.iceberg.service.cleanup.mapper.provider.IcebergCleanupMapperPackageProvider;
import org.apache.gravitino.iceberg.service.cleanup.po.IcebergCleanupJobPO;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.service.TableDeletionService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.Configuration;

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
    registerMappers();
  }

  /**
   * Registers the cleanup mappers into the entity store's shared MyBatis configuration.
   *
   * <p>Gravitino core builds the shared {@code SqlSessionFactory} during startup, before any
   * auxiliary service is loaded, and in deploy mode the iceberg-rest-server runs in an isolated
   * auxiliary-service class loader that core cannot see into. Core therefore cannot discover this
   * module's mappers, and without them every cleanup query fails with {@code BindingException}. We
   * register them here instead, lazily, from within the class loader that can see them the first
   * time a store is created. The {@code hasMapper} guard keeps it idempotent across stores and
   * threads.
   */
  private static void registerMappers() {
    Configuration configuration =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().getConfiguration();
    for (Class<?> mapper : new IcebergCleanupMapperPackageProvider().getMapperClasses()) {
      synchronized (configuration) {
        if (!configuration.hasMapper(mapper)) {
          configuration.addMapper(mapper);
        }
      }
    }
  }

  /**
   * Persists a legacy immediate-cleanup job as PENDING.
   *
   * <p>A job linked to a retained deletion must be inserted through {@link
   * IcebergDeletionPurgeStore#claimAndEnqueue} so job creation and {@code DELETED -> PURGING}
   * ownership commit in one transaction.
   *
   * @param job unlinked legacy job to persist
   * @return generated id
   */
  public long addJob(IcebergCleanupJob job) {
    if (job.tableId() != null || job.deletionId() != null) {
      throw new IllegalArgumentException(
          "Retained-deletion jobs must be enqueued through the atomic purge handoff");
    }
    long id = allocateJobId();
    long now = System.currentTimeMillis();
    SessionUtils.doWithCommit(
        IcebergCleanupJobMapper.class,
        mapper -> mapper.insertCleanupJob(IcebergCleanupJobPO.fromCleanupJob(job, id, now)));
    return id;
  }

  /** Allocates an application-generated cleanup-job identifier. */
  long allocateJobId() {
    return idGenerator.nextId();
  }

  /** Inserts a preallocated PENDING job into the caller's transaction. */
  void insertJobWithoutCommit(IcebergCleanupJob job, long id, long now) {
    IcebergCleanupJobPO po = IcebergCleanupJobPO.fromCleanupJob(job, id, now);
    SessionUtils.doWithoutCommit(
        IcebergCleanupJobMapper.class, mapper -> mapper.insertCleanupJob(po));
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
   * Atomically succeeds a linked cleanup job and consumes its exact retained relational generation.
   *
   * <p>The heartbeat compare-and-swap is the worker fence. Only the worker that still owns the
   * RUNNING job may proceed to the action/table row locks and exact metadata deletes. A failure in
   * either half rolls back both the job transition and every relational delete; the already
   * completed external cleanup is safe to replay.
   *
   * @param job linked retained-deletion cleanup job
   * @param heartbeat caller's current heartbeat ownership token
   * @return {@code true} iff this worker still owned and finalized the job
   */
  public boolean finalizeRetainedDeletion(IcebergCleanupJob job, long heartbeat) {
    if (job.tableId() == null || job.deletionId() == null) {
      throw new IllegalArgumentException("Cleanup job is not linked to a retained deletion");
    }
    long now = System.currentTimeMillis();
    return SessionUtils.doWithCommitAndFetchResult(
        IcebergCleanupJobMapper.class,
        mapper -> {
          if (mapper.markFinished(
                  job.id(), IcebergCleanupJob.State.SUCCEEDED.name(), null, now, heartbeat)
              != 1) {
            return false;
          }
          TableDeletionService.getInstance()
              .finalizePurge(job.tableId(), job.deletionId(), Long.toString(job.id()));
          return true;
        });
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
    return heartbeat(id, lastHeartbeat, now, null, null);
  }

  /**
   * Refreshes a heartbeat and advisory manifest progress with compare-and-swap ownership check.
   *
   * <p>The counters are observability only. They are not cleanup checkpoints and do not affect
   * claims, retries, or completion.
   *
   * @param id job id
   * @param lastHeartbeat previous heartbeat value
   * @param now new heartbeat value
   * @param manifestsTotal advisory number of manifests discovered
   * @param manifestsDone advisory number of manifests processed
   * @return {@code true} iff the row was still owned by the caller
   */
  public boolean heartbeat(
      long id, long lastHeartbeat, long now, long manifestsTotal, long manifestsDone) {
    if (manifestsTotal < 0 || manifestsDone < 0) {
      throw new IllegalArgumentException("Manifest progress must not be negative");
    }
    if (manifestsDone > manifestsTotal) {
      throw new IllegalArgumentException("Completed manifests must not exceed total manifests");
    }
    return heartbeat(
        id, lastHeartbeat, now, Long.valueOf(manifestsTotal), Long.valueOf(manifestsDone));
  }

  private boolean heartbeat(
      long id,
      long lastHeartbeat,
      long now,
      @Nullable Long manifestsTotal,
      @Nullable Long manifestsDone) {
    return SessionUtils.doWithCommitAndFetchResult(
            IcebergCleanupJobMapper.class,
            mapper -> mapper.heartbeat(id, lastHeartbeat, now, manifestsTotal, manifestsDone))
        > 0;
  }

  /**
   * Reads safe cleanup status and advisory manifest progress.
   *
   * @param id job id
   * @return status, or empty if the job does not exist
   */
  public Optional<IcebergCleanupJobStatus> getStatus(long id) {
    IcebergCleanupJobPO po =
        SessionUtils.getWithoutCommit(
            IcebergCleanupJobMapper.class, mapper -> mapper.selectStatus(id));
    return po == null
        ? Optional.empty()
        : Optional.of(
            new IcebergCleanupJobStatus(
                po.getId(),
                IcebergCleanupJob.State.valueOf(po.getState()),
                po.getAttempts(),
                po.getManifestsTotal(),
                po.getManifestsDone(),
                po.getUpdatedAt()));
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
