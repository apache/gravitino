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
package org.apache.gravitino.maintenance.scheduler.job.commit;

import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.scheduler.AutocommitDataSources;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;

/**
 * Enqueues one-shot {@code commit-job} rows. Chain collection, cool-down filter, auth, and spec
 * build are handled by {@link CommitJobMerger}.
 *
 * <p>Instance id is {@code table:{table_id}}; bursts of commits on the same table coalesce via
 * {@code scheduleIfNotExists}. Coalesce leaves an existing row's {@code task_data} untouched.
 */
public class CommitJobClient {

  /**
   * Outcome of {@link #enqueueNow(long, NameIdentifier)}.
   *
   * <p>Distinguishes coalesce ({@link #ALREADY_QUEUED}) from enqueue gate skips ({@link #SKIPPED})
   * so callers do not log "already queued" when no spec was written.
   */
  public enum EnqueueResult {

    /** A new {@code commit-job} row was inserted with a MaintenanceJobSpec. */
    INSERTED,

    /**
     * A {@code commit-job} row already exists for the table (coalesced; {@code task_data} kept).
     */
    ALREADY_QUEUED,

    /**
     * Nothing written: empty / cooled-down chain, auth failure, unresolved table, or jobConf build
     * failure.
     */
    SKIPPED
  }

  private final DataSource dataSource;
  private final CommitJobMerger merger;

  /**
   * @param dataSource relational JDBC pool shared with the entity store
   * @param gravitinoConfig server config for auth overlay and minInterval fallbacks
   */
  public CommitJobClient(DataSource dataSource, Config gravitinoConfig) {
    this(dataSource, new CommitJobMerger(gravitinoConfig));
  }

  /**
   * @param dataSource JDBC pool
   * @param merger builds the enqueue-time chain spec
   */
  CommitJobClient(DataSource dataSource, CommitJobMerger merger) {
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.merger = Preconditions.checkNotNull(merger);
  }

  /**
   * Builds an enqueue-time spec and schedules a {@code commit-job} when one is not already queued
   * and the spec is non-empty.
   *
   * @param tableId table entity id
   * @param identifier {@code metalake.catalog.schema.table} from the commit event
   * @return insert / coalesce / skip outcome
   */
  public EnqueueResult enqueueNow(long tableId, NameIdentifier identifier) {
    Preconditions.checkArgument(tableId > 0, "tableId must be positive");
    Preconditions.checkNotNull(identifier, "identifier must not be null");
    SchedulerClient client = schedulerClient(dataSource);
    String id = MaintenanceTaskIds.forCommitTable(tableId);
    // Coalesce without rebuilding task_data: keep the first enqueue-time spec.
    if (client
        .getScheduledExecution(TaskInstanceId.of(TableMaintenanceSchedulerPools.COMMIT_JOB, id))
        .isPresent()) {
      return EnqueueResult.ALREADY_QUEUED;
    }
    Optional<MaintenanceJobSpec> spec = merger.merge(tableId, identifier);
    if (spec.isEmpty()) {
      return EnqueueResult.SKIPPED;
    }
    boolean inserted =
        client.scheduleIfNotExists(
            new TaskInstance<>(TableMaintenanceSchedulerPools.COMMIT_JOB, id, spec.get()),
            Instant.now());
    return inserted ? EnqueueResult.INSERTED : EnqueueResult.ALREADY_QUEUED;
  }

  /**
   * Builds a db-scheduler {@link SchedulerClient} that can commit against the entity-store
   * DataSource.
   *
   * @param dataSource entity-store pool
   * @return client bound to {@link TableMaintenanceSchedulerPools#COMMIT_TASKS_TABLE}
   */
  public static SchedulerClient schedulerClient(DataSource dataSource) {
    return SchedulerClient.Builder.create(
            AutocommitDataSources.wrap(dataSource), TableMaintenanceSchedulerPools.commitJobTask())
        .serializer(new JacksonSerializer(JsonUtils.anyFieldMapper()))
        .tableName(TableMaintenanceSchedulerPools.COMMIT_TASKS_TABLE)
        .build();
  }
}
