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
import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.scheduler.AutocommitDataSources;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Task;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceTableRefs.ResolvedTableRef;
import org.apache.gravitino.maintenance.scheduler.policy.expand.ExpandTableCandidate;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enqueues one-shot {@code cron-job} rows through a db-scheduler {@link SchedulerClient} on the
 * entity-store DataSource.
 *
 * <p>Follows the usual client pattern: {@link SchedulerClient#scheduleIfNotExists} for new instance
 * ids, {@link SchedulerClient#getScheduledExecutionsForTask} to inspect open rows, and {@link
 * SchedulerClient#reschedule(TaskInstanceId, Instant, Object)} to replace {@code task_data} when
 * filling an incomplete snapshot-expiry batch.
 *
 * <p>Existing instance ids are left untouched so a repeated expand pick does not reset an
 * already-queued submit. Snapshot-expiry winners attached at schema/catalog are packed into {@code
 * batch:{batch_id};policy:{policy_id}} rows; incomplete batches are filled before a new {@code
 * batch_id} is allocated (§5.2.2).
 *
 * <p>Snapshot-expiry coalesce happens at expand enqueue (not cron pick): unpicked same-catalog
 * {@code expire_snapshots} {@code table:} / {@code batch:} rows may merge while under capacity
 * (whole queued row at a time). Claim-first: {@code cancel} each unpicked source row, then {@code
 * reschedule} the target row's {@code task_data} (target {@code task_instance} unchanged) — never
 * allocate a new instance just to coalesce. {@code spark_conf} is max-merged across the target,
 * optional new enqueue spec, and successfully claimed sources. Policy id need not match;
 * each merged {@link Task} keeps its original {@code policyId}. Picked rows are never claimed.
 *
 * <p>Enqueue builds a {@link MaintenanceJobSpec} once (policy → jobConf) into {@code task_data};
 * pick consumes that spec. The entity store disables JDBC autocommit; connections are wrapped via
 * {@link AutocommitDataSources} so each client call commits. {@code task_data} is JSON via {@link
 * JacksonSerializer + JsonUtils.anyFieldMapper()}.
 */
public class CronJobClient {

  private static final Logger LOG = LoggerFactory.getLogger(CronJobClient.class);

  /**
   * Max tables in one snapshot-expiry {@code batch:} cron-job (merge capacity). Independent of
   * {@code gravitino.maintenance.expand.enqueueBatchSize}, which only bounds how many tables one
   * expand pick may hand to {@link #enqueueNow}.
   */
  private static final int SNAPSHOT_EXPIRY_BATCH_CAPACITY = 10;

  private final DataSource dataSource;
  private final MaintenanceJobSpecFactory specFactory;
  private final Function<List<Long>, List<ResolvedTableRef>> tableRefResolver;

  /**
   * @param dataSource relational JDBC pool shared with the entity store
   * @param gravitinoConfig server config for jobConf overlay
   */
  public CronJobClient(DataSource dataSource, Config gravitinoConfig) {
    this(dataSource, gravitinoConfig, MaintenanceTableRefs::resolve);
  }

  /**
   * Visible for tests that inject table-id resolution without a JDBC catalog store.
   *
   * @param dataSource relational JDBC pool
   * @param gravitinoConfig server config
   * @param tableRefResolver resolves entity ids to catalog + schema.table
   */
  @VisibleForTesting
  public CronJobClient(
      DataSource dataSource,
      Config gravitinoConfig,
      Function<List<Long>, List<ResolvedTableRef>> tableRefResolver) {
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.specFactory = new MaintenanceJobSpecFactory(Preconditions.checkNotNull(gravitinoConfig));
    this.tableRefResolver = Preconditions.checkNotNull(tableRefResolver);
  }

  /**
   * Enqueues due-now cron jobs for the given expand candidates.
   *
   * @param policy effective maintenance policy (used to build the jobConf spec)
   * @param candidates enqueueable tables with winner levels
   * @param batchCapacity expand enqueue budget (max candidates considered for non-snapshot types).
   *     Snapshot-expiry merge capacity is a fixed per-batch table limit inside this client, not
   *     this value.
   * @return number of newly inserted cron-job rows (merges into existing batches do not count)
   */
  public int enqueueNow(
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
    int expandLimit = Math.max(1, batchCapacity);
    SchedulerClient client = schedulerClient(dataSource);
    Instant due = Instant.now();

    if (policy.policyType() == Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION) {
      return enqueueSnapshotExpiry(client, policy, candidates, SNAPSHOT_EXPIRY_BATCH_CAPACITY, due);
    }
    int scheduled = 0;
    int limit = Math.min(candidates.size(), expandLimit);
    for (ExpandTableCandidate candidate : candidates.subList(0, limit)) {
      Optional<MaintenanceJobSpec> spec = buildSpec(policy, List.of(candidate.tableId()));
      if (spec.isPresent()
          && scheduleTableJob(client, policy, candidate.tableId(), spec.get(), due).isPresent()) {
        scheduled++;
      }
    }
    return scheduled;
  }

  /**
   * Builds a db-scheduler {@link SchedulerClient} that can commit against the entity-store
   * DataSource.
   *
   * @param dataSource entity-store pool
   * @return client bound to {@link TableMaintenanceSchedulerPools#CRON_TASKS_TABLE}
   */
  public static SchedulerClient schedulerClient(DataSource dataSource) {
    return SchedulerClient.Builder.create(
            AutocommitDataSources.wrap(dataSource), TableMaintenanceSchedulerPools.cronJobTask())
        .serializer(new JacksonSerializer(JsonUtils.anyFieldMapper()))
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
    List<Long> tableLevel = new ArrayList<>();
    for (ExpandTableCandidate candidate : slice) {
      if (isTableAlreadyQueued(client, candidate.tableId(), policy.id())) {
        continue;
      }
      if (candidate.batchableForSnapshotExpiry()) {
        batchable.add(candidate.tableId());
      } else {
        tableLevel.add(candidate.tableId());
      }
    }
    int scheduled = 0;
    if (!batchable.isEmpty()) {
      for (Map.Entry<String, List<Long>> entry : groupByCatalog(batchable).entrySet()) {
        scheduled +=
            enqueueSameCatalog(
                client, policy, entry.getKey(), entry.getValue(), batchCapacity, due);
      }
    }
    for (Long tableId : tableLevel) {
      scheduled += enqueueTableSnapshotExpiry(client, policy, tableId, batchCapacity, due);
    }
    coalesceOpenExpireJobs(client, batchCapacity);
    return scheduled;
  }

  /**
   * Enqueues one table-level snapshot-expiry winner: prefer coalescing into an incomplete same-
   * catalog {@code batch:}, else schedule this {@code table:} and claim peer {@code table:}/ {@code
   * batch:} rows into it (target instance id unchanged).
   */
  private int enqueueTableSnapshotExpiry(
      SchedulerClient client, PolicyEntity policy, long tableId, int batchCapacity, Instant due) {
    if (isTableAlreadyQueued(client, tableId, policy.id())) {
      return 0;
    }
    List<ResolvedTableRef> resolved = tableRefResolver.apply(List.of(tableId));
    if (resolved.isEmpty()) {
      LOG.warn("Skipping unresolved table {} from snapshot-expiry table enqueue", tableId);
      return 0;
    }
    String catalogName = resolved.get(0).catalogName();
    Optional<MaintenanceJobSpec> enqueueSpec = buildSpec(policy, List.of(tableId));
    if (enqueueSpec.isEmpty()) {
      return 0;
    }
    List<Task> enqueueTasks = enqueueSpec.get().getTasks();

    Optional<QueuedExpireJob> incompleteBatch =
        findIncompleteExpireBatch(client, batchCapacity, catalogName);
    if (incompleteBatch.isPresent()) {
      QueuedExpireJob target = incompleteBatch.get();
      int remainingCapacity = batchCapacity - target.spec.tableIds().size() - enqueueTasks.size();
      List<QueuedExpireJob> peers =
          selectQueuedJobsToClaim(
              client,
              catalogName,
              remainingCapacity,
              batchCapacity,
              Set.of(target.instanceId.getId()),
              unionTableIds(target.spec.tableIds(), List.of(tableId)));
      if (coalesceInto(client, target, enqueueTasks, enqueueSpec.get(), peers, batchCapacity)) {
        return 0;
      }
    }

    Optional<QueuedExpireJob> tableJob =
        scheduleTableJob(client, policy, tableId, enqueueSpec.get(), due);
    if (tableJob.isEmpty()) {
      return 0;
    }
    List<QueuedExpireJob> peers =
        selectQueuedJobsToClaim(
            client,
            catalogName,
            batchCapacity - tableJob.get().spec.tableIds().size(),
            batchCapacity,
            Set.of(tableJob.get().instanceId.getId()),
            new HashSet<>(tableJob.get().spec.tableIds()));
    if (!peers.isEmpty()) {
      coalesceInto(client, tableJob.get(), List.of(), null, peers, batchCapacity);
    }
    return 1;
  }

  private Map<String, List<Long>> groupByCatalog(List<Long> tableIds) {
    List<ResolvedTableRef> resolved = tableRefResolver.apply(tableIds);
    Map<Long, ResolvedTableRef> byId = new LinkedHashMap<>();
    for (ResolvedTableRef ref : resolved) {
      byId.put(ref.tableId(), ref);
    }
    Map<String, List<Long>> byCatalog = new LinkedHashMap<>();
    for (Long tableId : tableIds) {
      ResolvedTableRef ref = byId.get(tableId);
      if (ref == null) {
        LOG.warn("Skipping unresolved table {} from snapshot-expiry batch enqueue", tableId);
        continue;
      }
      byCatalog.computeIfAbsent(ref.catalogName(), ignored -> new ArrayList<>()).add(tableId);
    }
    return byCatalog;
  }

  private int enqueueSameCatalog(
      SchedulerClient client,
      PolicyEntity policy,
      String catalogName,
      List<Long> tableIds,
      int batchCapacity,
      Instant due) {
    List<Long> remaining = new ArrayList<>(tableIds);
    int newRows = 0;
    while (!remaining.isEmpty()) {
      Optional<QueuedExpireJob> incompleteBatch =
          findIncompleteExpireBatch(client, batchCapacity, catalogName);
      if (incompleteBatch.isPresent()) {
        QueuedExpireJob target = incompleteBatch.get();
        int remainingCapacity = batchCapacity - target.spec.tableIds().size();
        int takeCount = Math.min(remainingCapacity, remaining.size());
        List<Long> enqueueTableIds = new ArrayList<>(remaining.subList(0, takeCount));
        Optional<MaintenanceJobSpec> enqueueSpec = buildSpec(policy, enqueueTableIds);
        if (enqueueSpec.isPresent()) {
          List<QueuedExpireJob> peers =
              selectQueuedJobsToClaim(
                  client,
                  catalogName,
                  remainingCapacity - enqueueTableIds.size(),
                  batchCapacity,
                  Set.of(target.instanceId.getId()),
                  unionTableIds(target.spec.tableIds(), enqueueTableIds));
          if (coalesceInto(
              client,
              target,
              enqueueSpec.get().getTasks(),
              enqueueSpec.get(),
              peers,
              batchCapacity)) {
            remaining.subList(0, takeCount).clear();
            continue;
          }
        }
      }
      int takeCount = Math.min(batchCapacity, remaining.size());
      List<Long> enqueueTableIds = new ArrayList<>(remaining.subList(0, takeCount));
      Optional<MaintenanceJobSpec> enqueueSpec = buildSpec(policy, enqueueTableIds);
      if (enqueueSpec.isPresent()) {
        Optional<QueuedExpireJob> batchJob =
            scheduleBatchJob(client, policy, enqueueSpec.get(), due);
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
            coalesceInto(client, batchJob.get(), List.of(), null, peers, batchCapacity);
          }
          newRows++;
          remaining.subList(0, takeCount).clear();
          continue;
        }
      }
      Long droppedTableId = remaining.remove(0);
      LOG.warn(
          "Dropping table {} from snapshot-expiry enqueue for policy {} after failed batch build",
          droppedTableId,
          policy.id());
    }
    return newRows;
  }

  /**
   * Coalesces under-capacity expire rows (prefer {@code batch:}, then {@code table:}) by claiming
   * peer rows. Shared by table-level and schema/catalog enqueue tails.
   */
  private void coalesceOpenExpireJobs(SchedulerClient client, int batchCapacity) {
    Set<String> saturatedInstanceIds = new HashSet<>();
    for (; ; ) {
      Optional<QueuedExpireJob> target =
          findCoalesceTarget(client, batchCapacity, saturatedInstanceIds);
      if (target.isEmpty()) {
        return;
      }
      QueuedExpireJob coalesceTarget = target.get();
      int remainingCapacity = batchCapacity - coalesceTarget.spec.tableIds().size();
      List<QueuedExpireJob> peers =
          selectQueuedJobsToClaim(
              client,
              coalesceTarget.catalog,
              remainingCapacity,
              batchCapacity,
              Set.of(coalesceTarget.instanceId.getId()),
              new HashSet<>(coalesceTarget.spec.tableIds()));
      if (peers.isEmpty()) {
        saturatedInstanceIds.add(coalesceTarget.instanceId.getId());
        continue;
      }
      if (!coalesceInto(client, coalesceTarget, List.of(), null, peers, batchCapacity)) {
        saturatedInstanceIds.add(coalesceTarget.instanceId.getId());
      }
    }
  }

  /** Prefers the oldest under-capacity {@code batch:}, else any under-capacity {@code table:}. */
  private static Optional<QueuedExpireJob> findCoalesceTarget(
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

  private static Set<Long> unionTableIds(List<Long> left, List<Long> right) {
    Set<Long> tableIds = new HashSet<>(left);
    tableIds.addAll(right);
    return tableIds;
  }

  /**
   * Claim-first coalesce into {@code target}: cancel each unpicked peer (takes ownership), then
   * reschedule the target's {@code task_data}. Target {@code task_instance} is unchanged. Envelope
   * {@code spark_conf} is max-merged across target, optional enqueue spec, and claimed peers.
   *
   * @return true when the target was updated, or when there was nothing to merge
   */
  private boolean coalesceInto(
      SchedulerClient client,
      QueuedExpireJob target,
      List<Task> enqueueTasks,
      @Nullable MaintenanceJobSpec enqueueSpec,
      List<QueuedExpireJob> peersToClaim,
      int batchCapacity) {
    Set<Long> plannedTableIds = new HashSet<>(target.spec.tableIds());
    for (Task task : enqueueTasks) {
      plannedTableIds.add(task.getTableId());
    }
    for (QueuedExpireJob peer : peersToClaim) {
      plannedTableIds.addAll(peer.spec.tableIds());
    }
    if (plannedTableIds.size() > batchCapacity) {
      return false;
    }

    List<Task> claimedTasks = new ArrayList<>();
    List<String> sparkConfJsons = new ArrayList<>();
    sparkConfJsons.add(target.spec.getSparkConf());
    if (enqueueSpec != null) {
      sparkConfJsons.add(enqueueSpec.getSparkConf());
    }
    for (QueuedExpireJob peer : peersToClaim) {
      Optional<MaintenanceJobSpec> claimedSpec = tryClaimQueuedJob(client, peer);
      if (claimedSpec.isEmpty()) {
        continue;
      }
      claimedTasks.addAll(claimedSpec.get().getTasks());
      sparkConfJsons.add(claimedSpec.get().getSparkConf());
    }

    List<Task> tasksToAdd = new ArrayList<>(enqueueTasks);
    tasksToAdd.addAll(claimedTasks);
    if (tasksToAdd.isEmpty()) {
      // No enqueue tables and no successful claims (peers picked/gone) — not progress.
      return peersToClaim.isEmpty();
    }
    String mergedSparkConf =
        MaintenanceJobSpecFactory.mergeSparkConfJson(sparkConfJsons.toArray(new String[0]));
    return rescheduleMergedTaskData(client, target, tasksToAdd, mergedSparkConf, batchCapacity);
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
          "Could not claim cron-job {} for snapshot-expiry coalesce: {}",
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
   * Reschedules {@code target} with claimed/enqueue tasks appended. Per-task {@code policyId} is
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
          "Could not reschedule cron-job {} after snapshot-expiry coalesce: {}",
          target.instanceId.getId(),
          e.toString());
      return false;
    }
  }

  private Optional<QueuedExpireJob> scheduleTableJob(
      SchedulerClient client,
      PolicyEntity policy,
      long tableId,
      MaintenanceJobSpec spec,
      Instant due) {
    String id = MaintenanceTaskIds.forCronTable(tableId, policy.id());
    boolean created =
        client.scheduleIfNotExists(
            new TaskInstance<>(TableMaintenanceSchedulerPools.CRON_JOB, id, spec), due);
    if (!created) {
      return Optional.empty();
    }
    return findExpireJob(client, id);
  }

  /** Schedules a new {@code batch:} for enqueue tables only; peers are claimed afterward. */
  private Optional<QueuedExpireJob> scheduleBatchJob(
      SchedulerClient client, PolicyEntity policy, MaintenanceJobSpec spec, Instant due) {
    if (spec.getTasks() == null || spec.getTasks().isEmpty()) {
      return Optional.empty();
    }
    long batchId = nextBatchId(client, policy.id());
    String id = MaintenanceTaskIds.forCronBatch(batchId, policy.id());
    boolean created =
        client.scheduleIfNotExists(
            new TaskInstance<>(TableMaintenanceSchedulerPools.CRON_JOB, id, spec), due);
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

  private Optional<MaintenanceJobSpec> buildSpec(PolicyEntity policy, List<Long> tableIds) {
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
      return Optional.of(specFactory.forCron(policy, resolved));
    } catch (RuntimeException e) {
      LOG.warn(
          "Skipping cron-job enqueue for policy {}: failed to build jobConf / MaintenanceJobSpec: {}",
          policy.id(),
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
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceJobSpec.class)) {
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
                TaskInstanceId.of(TableMaintenanceSchedulerPools.CRON_JOB, instanceId),
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
                TaskInstanceId.of(TableMaintenanceSchedulerPools.CRON_JOB, instanceId),
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
   * True when the table should not be enqueued again: a {@code table:} row already exists for this
   * policy, any queued snapshot-expiry cron-job (any policy) already covers the table, or a
   * non-expire {@code batch:} for this policy already includes the table.
   */
  static boolean isTableAlreadyQueued(SchedulerClient client, long tableId, long policyId) {
    if (client
        .getScheduledExecution(
            TaskInstanceId.of(
                TableMaintenanceSchedulerPools.CRON_JOB,
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
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceJobSpec.class)) {
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
