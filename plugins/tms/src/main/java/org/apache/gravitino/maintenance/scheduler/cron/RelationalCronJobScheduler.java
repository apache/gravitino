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
package org.apache.gravitino.maintenance.scheduler.cron;

import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.scheduler.AutocommitDataSources;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot.TableTarget;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshotFactory;
import org.apache.gravitino.maintenance.scheduler.MaintenanceSchedulerSerializers;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceTableRefs.ResolvedTableRef;
import org.apache.gravitino.maintenance.scheduler.expand.ExpandTableCandidate;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schedules {@code cron-job} rows through a db-scheduler {@link SchedulerClient} on the
 * entity-store DataSource.
 *
 * <p>Follows the usual client pattern: {@link SchedulerClient#scheduleIfNotExists} for new instance
 * ids, {@link SchedulerClient#getScheduledExecutionsForTask} to inspect open rows, and {@link
 * SchedulerClient#reschedule(TaskInstanceId, Instant, Object)} to replace {@code task_data} when
 * filling an incomplete snapshot-expiry batch (same due time, new table list + shared jobConf).
 *
 * <p>The entity store disables JDBC autocommit; connections are wrapped via {@link
 * AutocommitDataSources} so each client call commits. {@code task_data} is JSON via {@link
 * MaintenanceSchedulerSerializers#jackson()}.
 */
public final class RelationalCronJobScheduler implements CronJobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(RelationalCronJobScheduler.class);

  private final DataSource dataSource;
  private final MaintenanceJobSnapshotFactory snapshotFactory;
  private final Function<List<Long>, List<ResolvedTableRef>> tableRefResolver;

  /**
   * @param dataSource relational JDBC pool shared with the entity store
   * @param gravitinoConfig server config for jobConf overlay
   */
  public RelationalCronJobScheduler(DataSource dataSource, Config gravitinoConfig) {
    this(dataSource, gravitinoConfig, MaintenanceTableRefs::resolve);
  }

  /**
   * Visible for tests that inject table-id resolution without a JDBC catalog store.
   *
   * @param dataSource relational JDBC pool
   * @param gravitinoConfig server config
   * @param tableRefResolver resolves entity ids to catalog + schema.table
   */
  RelationalCronJobScheduler(
      DataSource dataSource,
      Config gravitinoConfig,
      Function<List<Long>, List<ResolvedTableRef>> tableRefResolver) {
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.snapshotFactory =
        new MaintenanceJobSnapshotFactory(Preconditions.checkNotNull(gravitinoConfig));
    this.tableRefResolver = Preconditions.checkNotNull(tableRefResolver);
  }

  @Override
  public int enqueueDueNow(
      PolicyEntity policy, List<ExpandTableCandidate> candidates, int batchCapacity) {
    if (policy == null || candidates == null || candidates.isEmpty()) {
      return 0;
    }
    if (MaintenanceJobTemplates.templateName(policy.policyType()).isEmpty()) {
      LOG.warn(
          "Skipping cron-job enqueue for policy {} with unsupported type {}",
          policy.id(),
          policy.policyType());
      return 0;
    }
    int capacity = Math.max(1, batchCapacity);
    SchedulerClient client = client(dataSource);
    Instant due = Instant.now();

    if (policy.policyType() == Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION) {
      return enqueueSnapshotExpiry(client, policy, candidates, capacity, due);
    }
    int scheduled = 0;
    int limit = Math.min(candidates.size(), capacity);
    for (ExpandTableCandidate candidate : candidates.subList(0, limit)) {
      if (scheduleTable(client, policy, candidate.tableId(), due)) {
        scheduled++;
      }
    }
    return scheduled;
  }

  /**
   * Builds a SchedulerClient that can commit against the entity-store DataSource.
   *
   * @param dataSource entity-store pool
   * @return client bound to {@link TableMaintenanceSchedulerPools#CRON_TASKS_TABLE}
   */
  static SchedulerClient client(DataSource dataSource) {
    return SchedulerClient.Builder.create(
            AutocommitDataSources.wrap(dataSource), TableMaintenanceSchedulerPools.cronJobTask())
        .serializer(MaintenanceSchedulerSerializers.jackson())
        .tableName(TableMaintenanceSchedulerPools.CRON_TASKS_TABLE)
        .build();
  }

  private int enqueueSnapshotExpiry(
      SchedulerClient client,
      PolicyEntity policy,
      List<ExpandTableCandidate> slice,
      int batchCapacity,
      Instant due) {
    List<Long> batchable = new ArrayList<>();
    int scheduled = 0;
    for (ExpandTableCandidate candidate : slice) {
      if (candidate.batchableForSnapshotExpiry()) {
        if (!isQueuedForPolicy(client, candidate.tableId(), policy.id())) {
          batchable.add(candidate.tableId());
        }
      } else if (scheduleTable(client, policy, candidate.tableId(), due)) {
        scheduled++;
      }
    }
    scheduled += packIntoBatches(client, policy, batchable, batchCapacity, due);
    return scheduled;
  }

  private int packIntoBatches(
      SchedulerClient client,
      PolicyEntity policy,
      List<Long> tableIds,
      int batchCapacity,
      Instant due) {
    if (tableIds.isEmpty()) {
      return 0;
    }
    List<Long> remaining = new ArrayList<>(tableIds);
    int newRows = 0;
    while (!remaining.isEmpty()) {
      Optional<OpenBatch> incomplete = findIncompleteBatch(client, policy.id(), batchCapacity);
      if (incomplete.isPresent()) {
        OpenBatch open = incomplete.get();
        int room = batchCapacity - open.snapshot.tableIds().size();
        List<Long> chunk = new ArrayList<>(remaining.subList(0, Math.min(room, remaining.size())));
        if (mergeIntoBatch(client, open, chunk)) {
          remaining.subList(0, chunk.size()).clear();
          continue;
        }
      }
      int end = Math.min(batchCapacity, remaining.size());
      List<Long> chunk = new ArrayList<>(remaining.subList(0, end));
      if (scheduleBatch(client, policy, chunk, due)) {
        newRows++;
      }
      remaining.subList(0, end).clear();
    }
    return newRows;
  }

  private boolean scheduleTable(
      SchedulerClient client, PolicyEntity policy, long tableId, Instant due) {
    Optional<MaintenanceJobSnapshot> snapshot = buildSnapshot(policy, List.of(tableId));
    if (snapshot.isEmpty()) {
      return false;
    }
    String id = CronJobIds.forTable(tableId, policy.id());
    return client.scheduleIfNotExists(
        new TaskInstance<>(TableMaintenanceSchedulerPools.CRON_JOB, id, snapshot.get()), due);
  }

  private boolean scheduleBatch(
      SchedulerClient client, PolicyEntity policy, List<Long> tableIds, Instant due) {
    Optional<MaintenanceJobSnapshot> snapshot = buildSnapshot(policy, tableIds);
    if (snapshot.isEmpty()) {
      return false;
    }
    long batchId = nextBatchId(client, policy.id());
    String id = CronJobIds.forBatch(batchId, policy.id());
    return client.scheduleIfNotExists(
        new TaskInstance<>(TableMaintenanceSchedulerPools.CRON_JOB, id, snapshot.get()), due);
  }

  private boolean mergeIntoBatch(SchedulerClient client, OpenBatch open, List<Long> extraTableIds) {
    Map<Long, TableTarget> byId = new LinkedHashMap<>();
    for (TableTarget target : open.snapshot.getTables()) {
      byId.put(target.getTableId(), target);
    }
    List<ResolvedTableRef> resolved = tableRefResolver.apply(extraTableIds);
    // Require every requested id so packIntoBatches does not drop unresolved tables from
    // remaining after a partial merge.
    if (resolved.size() != extraTableIds.size()) {
      LOG.warn(
          "Could not merge tables into cron-job {}: resolved {} of {} ids {}",
          open.instanceId.getId(),
          resolved.size(),
          extraTableIds.size(),
          extraTableIds);
      return false;
    }
    String metalake = open.snapshot.getMetalake();
    boolean added = false;
    for (ResolvedTableRef ref : resolved) {
      if (!byId.containsKey(ref.tableId())) {
        byId.put(ref.tableId(), MaintenanceJobSnapshotFactory.toTarget(metalake, ref));
        added = true;
      }
    }
    if (!added) {
      return true;
    }
    MaintenanceJobSnapshot merged =
        new MaintenanceJobSnapshot(
            metalake,
            new ArrayList<>(byId.values()),
            open.snapshot.getSteps(),
            open.snapshot.getJobConf());
    try {
      return client.reschedule(open.instanceId, open.executionTime, merged);
    } catch (RuntimeException e) {
      LOG.debug(
          "Could not merge tables into cron-job {}: {}", open.instanceId.getId(), e.toString());
      return false;
    }
  }

  private Optional<MaintenanceJobSnapshot> buildSnapshot(PolicyEntity policy, List<Long> tableIds) {
    List<ResolvedTableRef> resolved = tableRefResolver.apply(tableIds);
    if (resolved.size() != tableIds.size()) {
      LOG.warn(
          "Skipping cron-job enqueue for policy {}: resolved {} of {} table ids {}",
          policy.id(),
          resolved.size(),
          tableIds.size(),
          tableIds);
      return Optional.empty();
    }
    try {
      return Optional.of(snapshotFactory.forCron(policy, resolved));
    } catch (RuntimeException e) {
      LOG.warn(
          "Skipping cron-job enqueue for policy {}: failed to build jobConf snapshot: {}",
          policy.id(),
          e.toString());
      return Optional.empty();
    }
  }

  private static Optional<OpenBatch> findIncompleteBatch(
      SchedulerClient client, long policyId, int batchCapacity) {
    return listOpenBatches(client, policyId).stream()
        .filter(b -> !b.picked && b.snapshot.tableIds().size() < batchCapacity)
        .min(Comparator.comparingLong(b -> b.batchId));
  }

  private static List<OpenBatch> listOpenBatches(SchedulerClient client, long policyId) {
    List<OpenBatch> open = new ArrayList<>();
    for (ScheduledExecution<MaintenanceJobSnapshot> execution :
        client.getScheduledExecutionsForTask(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceJobSnapshot.class)) {
      Optional<CronJobIds.BatchPolicyIds> parsed =
          CronJobIds.parseBatch(execution.getTaskInstance().getId());
      if (parsed.isEmpty() || parsed.get().policyId() != policyId) {
        continue;
      }
      MaintenanceJobSnapshot data = execution.getData();
      if (data == null) {
        continue;
      }
      open.add(
          new OpenBatch(
              TaskInstanceId.of(
                  TableMaintenanceSchedulerPools.CRON_JOB, execution.getTaskInstance().getId()),
              parsed.get().batchId(),
              execution.getExecutionTime(),
              data,
              execution.isPicked()));
    }
    return open;
  }

  static long nextBatchId(SchedulerClient client, long policyId) {
    long max = -1L;
    for (OpenBatch batch : listOpenBatches(client, policyId)) {
      max = Math.max(max, batch.batchId);
    }
    return max + 1L;
  }

  /** True when a table already has a queued {@code table:} or {@code batch:} cron-job. */
  static boolean isQueuedForPolicy(SchedulerClient client, long tableId, long policyId) {
    if (client
        .getScheduledExecution(
            TaskInstanceId.of(
                TableMaintenanceSchedulerPools.CRON_JOB, CronJobIds.forTable(tableId, policyId)))
        .isPresent()) {
      return true;
    }
    for (OpenBatch batch : listOpenBatches(client, policyId)) {
      if (batch.snapshot.tableIds().contains(tableId)) {
        return true;
      }
    }
    return false;
  }

  private static final class OpenBatch {
    private final TaskInstanceId instanceId;
    private final long batchId;
    private final Instant executionTime;
    private final MaintenanceJobSnapshot snapshot;
    private final boolean picked;

    private OpenBatch(
        TaskInstanceId instanceId,
        long batchId,
        Instant executionTime,
        MaintenanceJobSnapshot snapshot,
        boolean picked) {
      this.instanceId = instanceId;
      this.batchId = batchId;
      this.executionTime = executionTime;
      this.snapshot = snapshot;
      this.picked = picked;
    }
  }
}
