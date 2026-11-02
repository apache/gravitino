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
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.scheduler.AutocommitDataSources;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Task;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
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
 * {@code expire_snapshots} {@code table:} / {@code batch:} rows may merge while under capacity. A
 * queued row is merged only when it is not already at capacity and the combined size would not
 * exceed capacity (all-or-nothing). Policy id need not match. Picked rows are never cancelled or
 * merged.
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
      if (scheduleTable(client, policy, candidate.tableId(), due)) {
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
    mergeJobsIntoOpenBatches(client, batchCapacity);
    return scheduled;
  }

  /**
   * Enqueues one table-level snapshot-expiry winner: prefer filling an incomplete same-catalog
   * {@code batch:}, else coalesce with other unpicked expire rows into a new {@code batch:}, else
   * schedule a lone {@code table:} row.
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
    Optional<QueuedExpireJob> incomplete =
        findIncompleteExpireBatch(client, batchCapacity, catalogName);
    if (incomplete.isPresent()
        && mergeTablesIntoCronRow(client, incomplete.get(), List.of(tableId), batchCapacity)) {
      return 0;
    }

    List<QueuedExpireJob> toMerge =
        selectJobsToMerge(
            client, catalogName, batchCapacity - 1, batchCapacity, Set.of(), Set.of(tableId));
    if (!toMerge.isEmpty()) {
      List<Long> chunk = new ArrayList<>();
      chunk.add(tableId);
      for (QueuedExpireJob job : toMerge) {
        chunk.addAll(job.spec.tableIds());
      }
      if (chunk.size() <= batchCapacity && scheduleBatch(client, policy, chunk, due)) {
        cancelJobs(client, toMerge);
        return 1;
      }
    }
    return scheduleTable(client, policy, tableId, due) ? 1 : 0;
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
      Optional<QueuedExpireJob> incomplete =
          findIncompleteExpireBatch(client, batchCapacity, catalogName);
      if (incomplete.isPresent()) {
        QueuedExpireJob open = incomplete.get();
        int room = batchCapacity - open.spec.tableIds().size();
        int fromRemaining = Math.min(room, remaining.size());
        List<Long> chunk = new ArrayList<>(remaining.subList(0, fromRemaining));
        Set<Long> excludeTables = new HashSet<>(open.spec.tableIds());
        excludeTables.addAll(chunk);
        List<QueuedExpireJob> toMerge =
            selectJobsToMerge(
                client,
                catalogName,
                room - chunk.size(),
                batchCapacity,
                Set.of(open.instanceId.getId()),
                excludeTables);
        for (QueuedExpireJob job : toMerge) {
          chunk.addAll(job.spec.tableIds());
        }
        if (mergeTablesIntoCronRow(client, open, chunk, batchCapacity)) {
          remaining.subList(0, fromRemaining).clear();
          cancelJobs(client, toMerge);
          continue;
        }
      }
      int end = Math.min(batchCapacity, remaining.size());
      List<Long> chunk = new ArrayList<>(remaining.subList(0, end));
      List<QueuedExpireJob> toMerge =
          selectJobsToMerge(
              client,
              catalogName,
              batchCapacity - chunk.size(),
              batchCapacity,
              Set.of(),
              new HashSet<>(chunk));
      for (QueuedExpireJob job : toMerge) {
        chunk.addAll(job.spec.tableIds());
      }
      if (chunk.size() <= batchCapacity && scheduleBatch(client, policy, chunk, due)) {
        newRows++;
        remaining.subList(0, end).clear();
        cancelJobs(client, toMerge);
        continue;
      }
      Long first = remaining.remove(0);
      if (end > 1 && scheduleBatch(client, policy, List.of(first), due)) {
        newRows++;
      } else {
        LOG.warn(
            "Dropping table {} from snapshot-expiry enqueue for policy {} after failed batch build",
            first,
            policy.id());
      }
    }
    return newRows;
  }

  /** Fills incomplete expire {@code batch:} rows with whole unpicked same-catalog cron rows. */
  private void mergeJobsIntoOpenBatches(SchedulerClient client, int batchCapacity) {
    Set<String> saturated = new HashSet<>();
    for (; ; ) {
      Optional<QueuedExpireJob> incomplete =
          listQueuedExpireJobs(client).stream()
              .filter(j -> j.isBatch && !j.picked && j.spec.tableIds().size() < batchCapacity)
              .filter(j -> j.catalog != null)
              .filter(j -> !saturated.contains(j.instanceId.getId()))
              .min(Comparator.comparingLong(j -> j.batchId));
      if (incomplete.isEmpty()) {
        return;
      }
      QueuedExpireJob open = incomplete.get();
      int room = batchCapacity - open.spec.tableIds().size();
      List<QueuedExpireJob> toMerge =
          selectJobsToMerge(
              client,
              open.catalog,
              room,
              batchCapacity,
              Set.of(open.instanceId.getId()),
              new HashSet<>(open.spec.tableIds()));
      if (toMerge.isEmpty()) {
        saturated.add(open.instanceId.getId());
        continue;
      }
      List<Long> extra = new ArrayList<>();
      for (QueuedExpireJob job : toMerge) {
        extra.addAll(job.spec.tableIds());
      }
      if (!mergeTablesIntoCronRow(client, open, extra, batchCapacity)) {
        return;
      }
      cancelJobs(client, toMerge);
    }
  }

  /**
   * Selects whole unpicked expire cron rows that fit in {@code room} without exceeding capacity.
   * Skips rows already at capacity. All-or-nothing per row.
   */
  private static List<QueuedExpireJob> selectJobsToMerge(
      SchedulerClient client,
      String catalogName,
      int room,
      int batchCapacity,
      Set<String> excludeInstanceIds,
      Set<Long> excludeTableIds) {
    if (room <= 0) {
      return List.of();
    }
    List<QueuedExpireJob> candidates =
        listQueuedExpireJobs(client).stream()
            .filter(j -> !j.picked)
            .filter(j -> catalogName.equals(j.catalog))
            .filter(j -> !excludeInstanceIds.contains(j.instanceId.getId()))
            .filter(j -> j.spec.tableIds().size() < batchCapacity)
            .filter(j -> j.spec.tableIds().stream().noneMatch(excludeTableIds::contains))
            .sorted(
                Comparator.comparingInt((QueuedExpireJob j) -> j.spec.tableIds().size())
                    .thenComparing(j -> j.instanceId.getId()))
            .toList();
    List<QueuedExpireJob> taken = new ArrayList<>();
    int left = room;
    Set<Long> claimedTables = new HashSet<>(excludeTableIds);
    for (QueuedExpireJob candidate : candidates) {
      int size = candidate.spec.tableIds().size();
      if (size > left) {
        continue;
      }
      if (candidate.spec.tableIds().stream().anyMatch(claimedTables::contains)) {
        continue;
      }
      taken.add(candidate);
      claimedTables.addAll(candidate.spec.tableIds());
      left -= size;
      if (left == 0) {
        break;
      }
    }
    return taken;
  }

  private boolean mergeTablesIntoCronRow(
      SchedulerClient client, QueuedExpireJob open, List<Long> extraTableIds, int batchCapacity) {
    if (extraTableIds.isEmpty()) {
      return true;
    }
    Map<Long, Task> byTableId = new LinkedHashMap<>();
    for (Task task : open.spec.getTasks()) {
      byTableId.putIfAbsent(task.getTableId(), task);
    }
    if (byTableId.size() + extraTableIds.stream().filter(id -> !byTableId.containsKey(id)).count()
        > batchCapacity) {
      return false;
    }
    String openCatalog = open.catalog;
    if (openCatalog == null || openCatalog.isEmpty()) {
      LOG.warn(
          "Could not merge tables into cron-job {}: open job missing catalog name",
          open.instanceId.getId());
      return false;
    }
    List<ResolvedTableRef> resolved = tableRefResolver.apply(extraTableIds);
    if (resolved.size() != extraTableIds.size()) {
      LOG.warn(
          "Could not merge tables into cron-job {}: resolved {} of {} ids {}",
          open.instanceId.getId(),
          resolved.size(),
          extraTableIds.size(),
          extraTableIds);
      return false;
    }
    if (open.spec.metalake() == null) {
      LOG.warn(
          "Could not merge tables into cron-job {}: missing metalake on open spec",
          open.instanceId.getId());
      return false;
    }
    Task template = open.spec.getTasks().get(0);
    boolean added = false;
    for (ResolvedTableRef ref : resolved) {
      if (!openCatalog.equals(ref.catalogName())) {
        LOG.warn(
            "Could not merge table {} into cron-job {}: catalog {} != open job catalog {}",
            ref.tableId(),
            open.instanceId.getId(),
            ref.catalogName(),
            openCatalog);
        return false;
      }
      if (!byTableId.containsKey(ref.tableId())) {
        Map<String, String> parameters = new LinkedHashMap<>(template.getParameters());
        parameters.put("catalog_name", ref.catalogName());
        parameters.put("table_identifier", ref.tableIdentifier());
        byTableId.put(
            ref.tableId(),
            new Task(
                ref.tableId(),
                template.getPolicyId(),
                template.getMaintenanceType(),
                null,
                parameters));
        added = true;
      }
    }
    if (!added) {
      return true;
    }
    if (byTableId.size() > batchCapacity) {
      return false;
    }
    MaintenanceJobSpec merged =
        new MaintenanceJobSpec(
            new ArrayList<>(byTableId.values()),
            open.spec.getUpdaterOptions(),
            open.spec.getSparkConf());
    try {
      return client.reschedule(open.instanceId, open.executionTime, merged);
    } catch (RuntimeException e) {
      LOG.debug(
          "Could not merge tables into cron-job {}: {}", open.instanceId.getId(), e.toString());
      return false;
    }
  }

  private static void cancelJobs(SchedulerClient client, List<QueuedExpireJob> jobs) {
    for (QueuedExpireJob job : jobs) {
      Optional<ScheduledExecution<Object>> execution = client.getScheduledExecution(job.instanceId);
      if (execution.isEmpty()) {
        continue;
      }
      if (execution.get().isPicked()) {
        LOG.warn(
            "Skip cancel of picked cron-job {} after snapshot-expiry merge",
            job.instanceId.getId());
        continue;
      }
      try {
        client.cancel(job.instanceId);
      } catch (RuntimeException e) {
        LOG.warn(
            "Failed to cancel merged cron-job {} after snapshot-expiry merge: {}",
            job.instanceId.getId(),
            e.toString());
      }
    }
  }

  private boolean scheduleTable(
      SchedulerClient client, PolicyEntity policy, long tableId, Instant due) {
    Optional<MaintenanceJobSpec> spec = buildSpec(policy, List.of(tableId));
    if (spec.isEmpty()) {
      return false;
    }
    String id = MaintenanceTaskIds.forCronTable(tableId, policy.id());
    return client.scheduleIfNotExists(
        new TaskInstance<>(TableMaintenanceSchedulerPools.CRON_JOB, id, spec.get()), due);
  }

  private boolean scheduleBatch(
      SchedulerClient client, PolicyEntity policy, List<Long> tableIds, Instant due) {
    Optional<MaintenanceJobSpec> spec = buildSpec(policy, tableIds);
    if (spec.isEmpty()) {
      return false;
    }
    long batchId = nextBatchId(client, policy.id());
    String id = MaintenanceTaskIds.forCronBatch(batchId, policy.id());
    return client.scheduleIfNotExists(
        new TaskInstance<>(TableMaintenanceSchedulerPools.CRON_JOB, id, spec.get()), due);
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
