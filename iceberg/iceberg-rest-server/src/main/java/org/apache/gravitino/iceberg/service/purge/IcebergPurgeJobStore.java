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

package org.apache.gravitino.iceberg.service.purge;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * Persistence for {@code iceberg_cleanup_job}, layered on the Gravitino entity store's shared
 * relational backend. All access goes through {@link IcebergPurgeJobMapper} and {@link
 * SessionUtils}, so async purge reuses the entity store's connection pool, transaction management,
 * and per-backend SQL dispatch rather than opening its own JDBC connections. Row ids are generated
 * by the application {@link IdGenerator}, matching the rest of the relational store.
 */
public class IcebergPurgeJobStore {

  private static final int MAX_ERROR_LENGTH = 2048;

  private final IdGenerator idGenerator;

  /**
   * Creates a purge job store.
   *
   * @param idGenerator generator for new row ids
   */
  public IcebergPurgeJobStore(IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
  }

  /**
   * Persists a new PENDING job.
   *
   * @param job job to persist
   * @return generated id
   */
  public long addJob(IcebergPurgeJob job) {
    long id = idGenerator.nextId();
    long now = System.currentTimeMillis();
    IcebergPurgeJobPO po = toPO(job, id, now);
    SessionUtils.doWithCommit(IcebergPurgeJobMapper.class, mapper -> mapper.insertPurgeJob(po));
    return id;
  }

  /**
   * Scans a small candidate window and takes the first available row via compare-and-swap.
   *
   * @param now current epoch millis, written as the initial heartbeat
   * @param heartbeatTimeoutMs age past which a RUNNING heartbeat is stale
   * @param window max candidates to consider
   * @return the taken job, or {@code null} if nothing was available
   */
  public IcebergPurgeJob takePendingJob(long now, long heartbeatTimeoutMs, int window) {
    long staleBefore = now - heartbeatTimeoutMs;
    List<Long> ids =
        SessionUtils.getWithoutCommit(
            IcebergPurgeJobMapper.class,
            mapper -> mapper.selectRunnableJobIds(staleBefore, window));
    for (long id : ids) {
      int marked =
          SessionUtils.doWithCommitAndFetchResult(
              IcebergPurgeJobMapper.class, mapper -> mapper.markRunning(id, now, staleBefore));
      if (marked == 1) {
        IcebergPurgeJobPO po =
            SessionUtils.getWithoutCommit(
                IcebergPurgeJobMapper.class, mapper -> mapper.selectById(id));
        return fromPO(po);
      }
    }
    return null;
  }

  /**
   * Marks a RUNNING job SUCCEEDED.
   *
   * @param id job id
   */
  public void markSucceeded(long id) {
    long now = System.currentTimeMillis();
    SessionUtils.doWithCommit(
        IcebergPurgeJobMapper.class,
        mapper -> mapper.markFinished(id, IcebergPurgeJob.State.SUCCEEDED.name(), null, now));
  }

  /**
   * Marks a RUNNING job FAILED immediately.
   *
   * @param id job id
   * @param reason failure text
   */
  public void markFailed(long id, String reason) {
    long now = System.currentTimeMillis();
    String err = truncate(reason);
    SessionUtils.doWithCommit(
        IcebergPurgeJobMapper.class,
        mapper -> mapper.markFinished(id, IcebergPurgeJob.State.FAILED.name(), err, now));
  }

  /**
   * Records a transient failure: {@code attempts++}, then FAILED at the ceiling else PENDING.
   *
   * @param id job id
   * @param reason failure text
   * @param maxAttempts ceiling from config
   */
  public void recordFailure(long id, String reason, int maxAttempts) {
    long now = System.currentTimeMillis();
    String err = truncate(reason);
    SessionUtils.doWithCommit(
        IcebergPurgeJobMapper.class, mapper -> mapper.recordFailure(id, err, maxAttempts, now));
  }

  /**
   * Refreshes a heartbeat with compare-and-swap ownership check.
   *
   * @param id job id
   * @param lastWritten previous heartbeat value
   * @param now new heartbeat value
   * @return {@code true} iff the row was still owned by the caller
   */
  public boolean heartbeat(long id, long lastWritten, long now) {
    return SessionUtils.doWithCommitAndFetchResult(
            IcebergPurgeJobMapper.class, mapper -> mapper.heartbeat(id, lastWritten, now))
        == 1;
  }

  /**
   * Checks whether a PENDING or RUNNING job occupies the identifier.
   *
   * @param catalog catalog name
   * @param namespace table namespace
   * @param table table name
   * @return true iff an active purge job exists for the identifier
   */
  public boolean hasActiveJob(String catalog, String namespace, String table) {
    Long id =
        SessionUtils.getWithoutCommit(
            IcebergPurgeJobMapper.class,
            mapper -> mapper.selectActiveJobId(catalog, namespace, table));
    return id != null;
  }

  /**
   * Deletes finished (SUCCEEDED or FAILED) jobs whose last update predates the timeline.
   *
   * @param legacyTimeline cutoff epoch millis; rows updated before this are removed
   * @return rows deleted
   */
  public int deleteFinishedJobsByLegacyTimeline(long legacyTimeline) {
    return SessionUtils.doWithCommitAndFetchResult(
        IcebergPurgeJobMapper.class,
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
  public IcebergPurgeJob.State stateOf(long id) {
    String state =
        SessionUtils.getWithoutCommit(
            IcebergPurgeJobMapper.class, mapper -> mapper.selectState(id));
    if (state == null) {
      throw new IllegalStateException("No purge job " + id);
    }
    return IcebergPurgeJob.State.valueOf(state);
  }

  private IcebergPurgeJobPO toPO(IcebergPurgeJob job, long id, long now) {
    IcebergPurgeJobPO po = new IcebergPurgeJobPO();
    po.setId(id);
    po.setMetalakeName(job.metalakeName());
    po.setCatalogName(job.catalogName());
    po.setNamespace(job.namespace());
    po.setTableName(job.tableName());
    po.setMetadataLocation(job.metadataLocation());
    po.setFileIOImpl(job.fileIOImpl());
    po.setFileIOProps(propertiesToJson(job.fileIOProperties()));
    po.setState(IcebergPurgeJob.State.PENDING.name());
    po.setAttempts(0);
    po.setLastError(null);
    po.setHeartbeatAt(null);
    po.setCreatedBy(job.createdBy());
    po.setUpdatedAt(now);
    return po;
  }

  private IcebergPurgeJob fromPO(IcebergPurgeJobPO po) {
    return new IcebergPurgeJob(
        po.getId(),
        po.getMetalakeName(),
        po.getCatalogName(),
        po.getNamespace(),
        po.getTableName(),
        po.getMetadataLocation(),
        po.getFileIOImpl(),
        jsonToProperties(po.getFileIOProps()),
        po.getCreatedBy());
  }

  private static String truncate(String value) {
    return value == null || value.length() <= MAX_ERROR_LENGTH
        ? value
        : value.substring(0, MAX_ERROR_LENGTH);
  }

  private static String propertiesToJson(Map<String, String> props) {
    try {
      return JsonUtils.objectMapper().writeValueAsString(props);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize fileIOProperties", e);
    }
  }

  private static Map<String, String> jsonToProperties(String json) {
    try {
      return JsonUtils.objectMapper().readValue(json, new TypeReference<Map<String, String>>() {});
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to deserialize fileIOProperties", e);
    }
  }
}
