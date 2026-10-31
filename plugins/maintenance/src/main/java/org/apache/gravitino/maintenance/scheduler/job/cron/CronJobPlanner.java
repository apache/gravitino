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
package org.apache.gravitino.maintenance.scheduler.job.cron;

import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.google.common.base.Preconditions;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.scheduler.AutocommitDataSources;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Task;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.MaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.TableCandidate;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceAuthConsistency;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plans schedule-time {@code cron-job} work: builds specs for non-snapshot policies, and for
 * snapshot-expiry merges same-catalog rows under a fixed batch capacity (claim-first {@code cancel}
 * then {@code reschedule} of target {@code task_data} in one JDBC transaction).
 */
public class CronJobPlanner {

  private static final Logger LOG = LoggerFactory.getLogger(CronJobPlanner.class);

  /** Max tables in one snapshot-expiry {@code batch:} / merge target. */
  private static final int SNAPSHOT_EXPIRY_BATCH_CAPACITY = 10;

  private final Config gravitinoConfig;
  private final DataSource dataSource;
  private final MaintenanceJobSpecFactory specFactory;

  /**
   * @param gravitinoConfig server config for jobConf overlay and auth checks
   * @param dataSource entity-store pool used to open merge transactions
   */
  public CronJobPlanner(Config gravitinoConfig, DataSource dataSource) {
    this.gravitinoConfig = Preconditions.checkNotNull(gravitinoConfig);
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.specFactory = new MaintenanceJobSpecFactory(this.gravitinoConfig);
  }

  /**
   * Plans and writes due-now cron-job rows for the given expand candidates.
   *
   * @param client cron-job scheduler client
   * @param policy effective maintenance policy
   * @param candidates schedulable tables
   * @param expandBatchCapacity max candidates for non-snapshot types
   * @param due execution time for newly inserted rows
   * @return number of newly inserted cron-job rows (merges into existing rows do not count)
   */
  public int plan(
      SchedulerClient client,
      PolicyEntity policy,
      List<TableCandidate> candidates,
      int expandBatchCapacity,
      Instant due) {
    if (policy == null || candidates == null || candidates.isEmpty()) {
      return 0;
    }
    if (MaintenanceJobTemplates.templateName(policy.policyType()).isEmpty()) {
      LOG.warn(
          "Skipping cron-job schedule for policy {} with unsupported type {}",
          policy.id(),
          policy.policyType());
      return 0;
    }
    if (policy.policyType() != Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION) {
      int scheduled = 0;
      int limit = Math.min(candidates.size(), Math.max(1, expandBatchCapacity));
      for (TableCandidate candidate : candidates.subList(0, limit)) {
        Optional<MaintenanceJobSpec> spec = buildSpec(List.of(candidate));
        if (spec.isEmpty()) {
          continue;
        }
        String id = MaintenanceTaskIds.forCronTable(candidate.tableId(), policy.id());
        if (client.scheduleIfNotExists(
            new TaskInstance<>(MaintenanceSchedulerPools.CRON_JOB, id, spec.get()), due)) {
          scheduled++;
        }
      }
      return scheduled;
    }
    int batchCapacity = SNAPSHOT_EXPIRY_BATCH_CAPACITY;
    List<TableCandidate> toSchedule = new ArrayList<>();
    for (TableCandidate candidate : candidates) {
      if (isTableAlreadyQueued(client, candidate.tableId(), policy.id())) {
        continue;
      }
      toSchedule.add(candidate);
    }
    int scheduled = 0;
    if (!toSchedule.isEmpty()) {
      Map<String, List<TableCandidate>> byCatalog = new LinkedHashMap<>();
      for (TableCandidate candidate : toSchedule) {
        byCatalog
            .computeIfAbsent(candidate.catalogName(), ignored -> new ArrayList<>())
            .add(candidate);
      }
      for (Map.Entry<String, List<TableCandidate>> entry : byCatalog.entrySet()) {
        scheduled +=
            scheduleSameCatalog(
                client, policy, entry.getKey(), entry.getValue(), batchCapacity, due);
      }
    }
    mergeOpenExpireJobs(client, batchCapacity);
    return scheduled;
  }

  private int scheduleSameCatalog(
      SchedulerClient client,
      PolicyEntity policy,
      String catalogName,
      List<TableCandidate> tables,
      int batchCapacity,
      Instant due) {
    List<TableCandidate> remaining = new ArrayList<>(tables);
    int newRows = 0;
    while (!remaining.isEmpty()) {
      Optional<QueuedExpireJob> incompleteBatch =
          findIncompleteExpireBatch(client, batchCapacity, catalogName);
      if (incompleteBatch.isPresent()) {
        QueuedExpireJob target = incompleteBatch.get();
        int remainingCapacity = batchCapacity - target.spec.tableIds().size();
        int takeCount = Math.min(remainingCapacity, remaining.size());
        List<TableCandidate> scheduleTables = new ArrayList<>(remaining.subList(0, takeCount));
        Optional<MaintenanceJobSpec> scheduleSpec = buildSpec(scheduleTables);
        if (scheduleSpec.isPresent()) {
          List<QueuedExpireJob> peers =
              selectQueuedJobsToClaim(
                  client,
                  catalogName,
                  remainingCapacity - scheduleTables.size(),
                  batchCapacity,
                  Set.of(target.instanceId.getId()),
                  unionTableIds(target.spec.tableIds(), tableIds(scheduleTables)));
          if (mergeInto(
              target, scheduleSpec.get().getTasks(), scheduleSpec.get(), peers, batchCapacity)) {
            remaining.subList(0, takeCount).clear();
            continue;
          }
        }
      }
      int takeCount = Math.min(batchCapacity, remaining.size());
      List<TableCandidate> scheduleTables = new ArrayList<>(remaining.subList(0, takeCount));
      Optional<MaintenanceJobSpec> scheduleSpec = buildSpec(scheduleTables);
      if (scheduleSpec.isPresent()) {
        Optional<QueuedExpireJob> batchJob =
            scheduleBatchJob(client, policy, scheduleSpec.get(), due);
        if (batchJob.isPresent()) {
          List<QueuedExpireJob> peers =
              selectQueuedJobsToClaim(
                  client,
                  catalogName,
                  batchCapacity - batchJob.get().spec.tableIds().size(),
                  batchCapacity,
                  Set.of(batchJob.get().instanceId.getId()),
                  new HashSet<>(batchJob.get().spec.tableIds()));
          if (!peers.isEmpty()) {
            mergeInto(batchJob.get(), List.of(), null, peers, batchCapacity);
          }
          newRows++;
          remaining.subList(0, takeCount).clear();
          continue;
        }
      }
      long droppedTableId = remaining.remove(0).tableId();
      LOG.warn(
          "Dropping table {} from snapshot-expiry schedule for policy {} after failed batch build",
          droppedTableId,
          policy.id());
    }
    return newRows;
  }

  /** Merges under-capacity expire rows (prefer {@code batch:}, then {@code table:}). */
  private void mergeOpenExpireJobs(SchedulerClient client, int batchCapacity) {
    Set<String> saturatedInstanceIds = new HashSet<>();
    for (; ; ) {
      Optional<QueuedExpireJob> target =
          findMergeTarget(client, batchCapacity, saturatedInstanceIds);
      if (target.isEmpty()) {
        return;
      }
      QueuedExpireJob mergeTarget = target.get();
      int remainingCapacity = batchCapacity - mergeTarget.spec.tableIds().size();
      List<QueuedExpireJob> peers =
          selectQueuedJobsToClaim(
              client,
              mergeTarget.catalog,
              remainingCapacity,
              batchCapacity,
              Set.of(mergeTarget.instanceId.getId()),
              new HashSet<>(mergeTarget.spec.tableIds()));
      if (peers.isEmpty()) {
        saturatedInstanceIds.add(mergeTarget.instanceId.getId());
        continue;
      }
      if (!mergeInto(mergeTarget, List.of(), null, peers, batchCapacity)) {
        saturatedInstanceIds.add(mergeTarget.instanceId.getId());
      }
    }
  }

  /** Prefers the oldest under-capacity {@code batch:}, else any under-capacity {@code table:}. */
  private static Optional<QueuedExpireJob> findMergeTarget(
      SchedulerClient client, int batchCapacity, Set<String> saturatedInstanceIds) {
    List<QueuedExpireJob> underCapacity =
        listQueuedExpireJobs(client).stream()
            .filter(job -> !job.picked && job.spec.tableIds().size() < batchCapacity)
            .filter(job -> job.catalog != null)
            .filter(job -> !saturatedInstanceIds.contains(job.instanceId.getId()))
            .toList();
    Optional<QueuedExpireJob> batchTarget =
        underCapacity.stream()
            .filter(job -> job.isBatch)
            .min(Comparator.comparingLong(job -> job.batchId));
    if (batchTarget.isPresent()) {
      return batchTarget;
    }
    return underCapacity.stream()
        .filter(job -> !job.isBatch)
        .min(Comparator.comparing(job -> job.instanceId.getId()));
  }

  private static List<Long> tableIds(List<TableCandidate> tables) {
    List<Long> ids = new ArrayList<>(tables.size());
    for (TableCandidate table : tables) {
      ids.add(table.tableId());
    }
    return ids;
  }

  private static Set<Long> unionTableIds(List<Long> left, List<Long> right) {
    Set<Long> tableIds = new HashSet<>(left);
    tableIds.addAll(right);
    return tableIds;
  }

  /**
   * Claim-first merge into {@code target}: cancel each unpicked peer then reschedule the target's
   * {@code task_data} in one JDBC transaction. Target {@code task_instance} is unchanged. Envelope
   * {@code spark_conf} is max-merged across target, optional schedule spec, and claimed peers.
   *
   * @return true when the target was updated, or when there was nothing to merge
   */
  private boolean mergeInto(
      QueuedExpireJob target,
      List<Task> scheduleTasks,
      @Nullable MaintenanceJobSpec scheduleSpec,
      List<QueuedExpireJob> peersToClaim,
      int batchCapacity) {
    if (scheduleSpec != null
        && !MaintenanceAuthConsistency.hasSameAuth(gravitinoConfig, target.spec, scheduleSpec)) {
      LOG.debug(
          "Skipping snapshot-expiry merge into {}: schedule auth mismatch with target",
          target.instanceId.getId());
      return false;
    }

    List<QueuedExpireJob> claimable = new ArrayList<>();
    for (QueuedExpireJob peer : peersToClaim) {
      if (!MaintenanceAuthConsistency.hasSameAuth(gravitinoConfig, target.spec, peer.spec)) {
        LOG.debug(
            "Skipping claim of cron-job {} for snapshot-expiry merge: auth mismatch with target {}",
            peer.instanceId.getId(),
            target.instanceId.getId());
        continue;
      }
      claimable.add(peer);
    }

    Set<Long> plannedTableIds = new HashSet<>(target.spec.tableIds());
    for (Task task : scheduleTasks) {
      plannedTableIds.add(task.getTableId());
    }
    for (QueuedExpireJob peer : claimable) {
      plannedTableIds.addAll(peer.spec.tableIds());
    }
    if (plannedTableIds.size() > batchCapacity) {
      return false;
    }

    if (claimable.isEmpty() && scheduleTasks.isEmpty()) {
      return peersToClaim.isEmpty();
    }

    try {
      Function<DataSource, Boolean> mergeWork =
          txDs -> {
            SchedulerClient txClient = CronJobClient.schedulerClientOn(txDs);
            List<Task> claimedTasks = new ArrayList<>();
            List<String> sparkConfJsons = new ArrayList<>();
            sparkConfJsons.add(target.spec.getSparkConf());
            if (scheduleSpec != null) {
              sparkConfJsons.add(scheduleSpec.getSparkConf());
            }
            for (QueuedExpireJob peer : claimable) {
              Optional<MaintenanceJobSpec> claimedSpec = tryClaimQueuedJob(txClient, peer);
              if (claimedSpec.isEmpty()) {
                continue;
              }
              claimedTasks.addAll(claimedSpec.get().getTasks());
              sparkConfJsons.add(claimedSpec.get().getSparkConf());
            }

            List<Task> tasksToAdd = new ArrayList<>(scheduleTasks);
            tasksToAdd.addAll(claimedTasks);
            if (tasksToAdd.isEmpty()) {
              if (peersToClaim.isEmpty()) {
                return true;
              }
              throw new MergeTransactionAborted();
            }
            String mergedSparkConf =
                MaintenanceJobSpecFactory.mergeSparkConfJson(sparkConfJsons.toArray(new String[0]));
            if (!rescheduleMergedTaskData(
                txClient, target, tasksToAdd, mergedSparkConf, batchCapacity)) {
              throw new MergeTransactionAborted();
            }
            return true;
          };
      AutocommitDataSources.runInTransaction(dataSource, mergeWork);
      return true;
    } catch (MergeTransactionAborted e) {
      return false;
    } catch (SQLException e) {
      LOG.warn(
          "Snapshot-expiry merge into {} failed to open or finish JDBC transaction: {}",
          target.instanceId.getId(),
          e.toString());
      return false;
    }
  }

  /**
   * Signals intentional merge abort so {@link AutocommitDataSources#runInTransaction} rolls back.
   */
  private static final class MergeTransactionAborted extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private MergeTransactionAborted() {
      super(null, null, false, false);
    }
  }

  /**
   * Cancels an unpicked queued expire row and returns its latest {@code task_data}. Empty means the
   * row was missing, already picked, or cancel failed — do not merge those tables into the target.
   */
  private static Optional<MaintenanceJobSpec> tryClaimQueuedJob(
      SchedulerClient client, QueuedExpireJob queuedJob) {
    Optional<ScheduledExecution<Object>> execution =
        client.getScheduledExecution(queuedJob.instanceId);
    if (execution.isEmpty() || execution.get().isPicked()) {
      return Optional.empty();
    }
    Object data = execution.get().getData();
    if (!(data instanceof MaintenanceJobSpec)) {
      return Optional.empty();
    }
    MaintenanceJobSpec spec = (MaintenanceJobSpec) data;
    if (!spec.isValid()) {
      return Optional.empty();
    }
    try {
      client.cancel(queuedJob.instanceId);
    } catch (RuntimeException e) {
      LOG.debug(
          "Could not claim cron-job {} for snapshot-expiry merge: {}",
          queuedJob.instanceId.getId(),
          e.toString());
      return Optional.empty();
    }
    return Optional.of(spec);
  }

  /**
   * Selects whole unpicked expire cron rows that fit in {@code remainingCapacity} without exceeding
   * {@code batchCapacity}. Skips rows already at capacity. All-or-nothing per row.
   */
  private static List<QueuedExpireJob> selectQueuedJobsToClaim(
      SchedulerClient client,
      String catalogName,
      int remainingCapacity,
      int batchCapacity,
      Set<String> excludeInstanceIds,
      Set<Long> excludeTableIds) {
    if (remainingCapacity <= 0) {
      return List.of();
    }
    List<QueuedExpireJob> candidates =
        listQueuedExpireJobs(client).stream()
            .filter(job -> !job.picked)
            .filter(job -> catalogName.equals(job.catalog))
            .filter(job -> !excludeInstanceIds.contains(job.instanceId.getId()))
            .filter(job -> job.spec.tableIds().size() < batchCapacity)
            .filter(job -> job.spec.tableIds().stream().noneMatch(excludeTableIds::contains))
            .sorted(
                Comparator.comparingInt((QueuedExpireJob job) -> job.spec.tableIds().size())
                    .thenComparing(job -> job.instanceId.getId()))
            .toList();
    List<QueuedExpireJob> selected = new ArrayList<>();
    int capacityLeft = remainingCapacity;
    Set<Long> reservedTableIds = new HashSet<>(excludeTableIds);
    for (QueuedExpireJob candidate : candidates) {
      int size = candidate.spec.tableIds().size();
      if (size > capacityLeft) {
        continue;
      }
      if (candidate.spec.tableIds().stream().anyMatch(reservedTableIds::contains)) {
        continue;
      }
      selected.add(candidate);
      reservedTableIds.addAll(candidate.spec.tableIds());
      capacityLeft -= size;
      if (capacityLeft == 0) {
        break;
      }
    }
    return selected;
  }

  /**
   * Reschedules {@code target} with claimed/schedule tasks appended. Per-task {@code policyId} is
   * preserved; {@code sparkConf} is the already max-merged {@code spark_conf}.
   */
  private boolean rescheduleMergedTaskData(
      SchedulerClient client,
      QueuedExpireJob target,
      List<Task> tasksToAdd,
      @Nullable String sparkConf,
      int batchCapacity) {
    Map<Long, Task> tasksByTableId = new LinkedHashMap<>();
    for (Task task : target.spec.getTasks()) {
      tasksByTableId.putIfAbsent(task.getTableId(), task);
    }
    String targetCatalog = target.catalog;
    if (targetCatalog == null || targetCatalog.isEmpty()) {
      LOG.warn(
          "Could not merge tables into cron-job {}: target missing catalog name",
          target.instanceId.getId());
      return false;
    }
    if (target.spec.metalake() == null) {
      LOG.warn(
          "Could not merge tables into cron-job {}: missing metalake on target spec",
          target.instanceId.getId());
      return false;
    }
    boolean added = false;
    for (Task task : tasksToAdd) {
      if (tasksByTableId.containsKey(task.getTableId())) {
        continue;
      }
      String catalog = task.catalogName();
      if (catalog == null || !targetCatalog.equals(catalog)) {
        LOG.warn(
            "Could not merge table {} into cron-job {}: catalog {} != target catalog {}",
            task.getTableId(),
            target.instanceId.getId(),
            catalog,
            targetCatalog);
        return false;
      }
      tasksByTableId.put(task.getTableId(), task);
      added = true;
    }
    if (!added) {
      return true;
    }
    if (tasksByTableId.size() > batchCapacity) {
      return false;
    }
    MaintenanceJobSpec mergedSpec =
        new MaintenanceJobSpec(
            new ArrayList<>(tasksByTableId.values()), target.spec.getUpdaterOptions(), sparkConf);
    try {
      return client.reschedule(target.instanceId, target.executionTime, mergedSpec);
    } catch (RuntimeException e) {
      LOG.debug(
          "Could not reschedule cron-job {} after snapshot-expiry merge: {}",
          target.instanceId.getId(),
          e.toString());
      return false;
    }
  }

  /** Schedules a new {@code batch:} for schedule tables only; peers are claimed afterward. */
  private Optional<QueuedExpireJob> scheduleBatchJob(
      SchedulerClient client, PolicyEntity policy, MaintenanceJobSpec spec, Instant due) {
    if (spec.getTasks() == null || spec.getTasks().isEmpty()) {
      return Optional.empty();
    }
    long batchId = nextBatchId(client, policy.id());
    String id = MaintenanceTaskIds.forCronBatch(batchId, policy.id());
    boolean created =
        client.scheduleIfNotExists(
            new TaskInstance<>(MaintenanceSchedulerPools.CRON_JOB, id, spec), due);
    if (!created) {
      return Optional.empty();
    }
    return findExpireJob(client, id);
  }

  private static Optional<QueuedExpireJob> findExpireJob(
      SchedulerClient client, String instanceId) {
    return listQueuedExpireJobs(client).stream()
        .filter(j -> j.instanceId.getId().equals(instanceId))
        .findFirst();
  }

  private Optional<MaintenanceJobSpec> buildSpec(List<TableCandidate> tables) {
    try {
      return Optional.of(specFactory.forCron(tables));
    } catch (RuntimeException e) {
      long policyId =
          tables.isEmpty() || tables.get(0).policies().isEmpty()
              ? -1L
              : tables.get(0).policies().get(0).id();
      LOG.warn(
          "Skipping cron-job schedule for policy {}: failed to build jobConf / MaintenanceJobSpec: {}",
          policyId,
          e.toString());
      return Optional.empty();
    }
  }

  private static Optional<QueuedExpireJob> findIncompleteExpireBatch(
      SchedulerClient client, int batchCapacity, String catalogName) {
    return listQueuedExpireJobs(client).stream()
        .filter(j -> j.isBatch && !j.picked && j.spec.tableIds().size() < batchCapacity)
        .filter(j -> catalogName.equals(j.catalog))
        .min(Comparator.comparingLong(j -> j.batchId));
  }

  private static List<QueuedExpireJob> listQueuedExpireJobs(SchedulerClient client) {
    List<QueuedExpireJob> open = new ArrayList<>();
    for (ScheduledExecution<MaintenanceJobSpec> execution :
        client.getScheduledExecutionsForTask(
            MaintenanceSchedulerPools.CRON_JOB, MaintenanceJobSpec.class)) {
      MaintenanceJobSpec data = execution.getData();
      if (data == null || !data.isValid() || !isExpireSnapshots(data)) {
        continue;
      }
      String instanceId = execution.getTaskInstance().getId();
      String catalog = data.getTasks().isEmpty() ? null : data.getTasks().get(0).catalogName();
      Optional<long[]> batch = MaintenanceTaskIds.parseCronBatch(instanceId);
      if (batch.isPresent()) {
        // [batchId, policyId]
        open.add(
            new QueuedExpireJob(
                TaskInstanceId.of(MaintenanceSchedulerPools.CRON_JOB, instanceId),
                true,
                batch.get()[0],
                batch.get()[1],
                execution.getExecutionTime(),
                data,
                catalog,
                execution.isPicked()));
        continue;
      }
      Optional<long[]> table = MaintenanceTaskIds.parseCronTable(instanceId);
      if (table.isPresent()) {
        // [tableId, policyId]
        open.add(
            new QueuedExpireJob(
                TaskInstanceId.of(MaintenanceSchedulerPools.CRON_JOB, instanceId),
                false,
                -1L,
                table.get()[1],
                execution.getExecutionTime(),
                data,
                catalog,
                execution.isPicked()));
      }
    }
    return open;
  }

  private static boolean isExpireSnapshots(MaintenanceJobSpec spec) {
    String expireType =
        MaintenanceJobTemplates.maintenanceType(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION)
            .orElseThrow();
    return spec.getTasks().stream().anyMatch(t -> expireType.equals(t.getMaintenanceType()));
  }

  private static long nextBatchId(SchedulerClient client, long policyId) {
    long max = -1L;
    for (QueuedExpireJob job : listQueuedExpireJobs(client)) {
      if (job.isBatch && job.policyId == policyId) {
        max = Math.max(max, job.batchId);
      }
    }
    return max + 1L;
  }

  /**
   * True when the table should not be scheduled again: a {@code table:} row already exists for this
   * policy, any queued snapshot-expiry cron-job (any policy) already covers the table, or a
   * non-expire {@code batch:} for this policy already includes the table.
   *
   * @param client cron-job scheduler client
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return true when a queued cron-job already covers the table
   */
  public boolean isTableAlreadyQueued(SchedulerClient client, long tableId, long policyId) {
    if (client
        .getScheduledExecution(
            TaskInstanceId.of(
                MaintenanceSchedulerPools.CRON_JOB,
                MaintenanceTaskIds.forCronTable(tableId, policyId)))
        .isPresent()) {
      return true;
    }
    for (QueuedExpireJob job : listQueuedExpireJobs(client)) {
      if (job.spec.tableIds().contains(tableId)) {
        return true;
      }
    }
    for (ScheduledExecution<MaintenanceJobSpec> execution :
        client.getScheduledExecutionsForTask(
            MaintenanceSchedulerPools.CRON_JOB, MaintenanceJobSpec.class)) {
      Optional<long[]> parsed =
          MaintenanceTaskIds.parseCronBatch(execution.getTaskInstance().getId());
      // [batchId, policyId]
      if (parsed.isEmpty() || parsed.get()[1] != policyId) {
        continue;
      }
      MaintenanceJobSpec data = execution.getData();
      if (data != null
          && data.isValid()
          && !isExpireSnapshots(data)
          && data.tableIds().contains(tableId)) {
        return true;
      }
    }
    return false;
  }

  private static final class QueuedExpireJob {
    private final TaskInstanceId instanceId;
    private final boolean isBatch;
    private final long batchId;
    private final long policyId;
    private final Instant executionTime;
    private final MaintenanceJobSpec spec;
    private final String catalog;
    private final boolean picked;

    private QueuedExpireJob(
        TaskInstanceId instanceId,
        boolean isBatch,
        long batchId,
        long policyId,
        Instant executionTime,
        MaintenanceJobSpec spec,
        String catalog,
        boolean picked) {
      this.instanceId = instanceId;
      this.isBatch = isBatch;
      this.batchId = batchId;
      this.policyId = policyId;
      this.executionTime = executionTime;
      this.spec = spec;
      this.catalog = catalog;
      this.picked = picked;
    }
  }
}
