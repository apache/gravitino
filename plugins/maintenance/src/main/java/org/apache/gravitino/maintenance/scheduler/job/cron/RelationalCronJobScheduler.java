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
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.TableTarget;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceTableRefs.ResolvedTableRef;
import org.apache.gravitino.maintenance.scheduler.policy.expand.ExpandTableCandidate;
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
 * filling an incomplete snapshot-expiry batch.
 *
 * <p>Snapshot-expiry coalesce happens at expand enqueue (not cron pick): unpicked same-catalog
 * {@code expire_snapshots} {@code table:} / {@code batch:} rows may merge while under capacity. A
 * peer is absorbed only when it is not already at capacity and the combined size would not exceed
 * capacity (all-or-nothing). Policy id need not match. Picked rows are never cancelled or merged.
 *
 * <p>The entity store disables JDBC autocommit; connections are wrapped via {@link
 * AutocommitDataSources} so each client call commits. {@code task_data} is JSON via {@link
 * JacksonSerializer + JsonUtils.anyFieldMapper()}.
 */
public final class RelationalCronJobScheduler implements CronJobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(RelationalCronJobScheduler.class);

  private static final String EXPIRE_SNAPSHOTS = "expire_snapshots";

  /**
   * Max tables in one snapshot-expiry {@code batch:} cron-job (merge capacity). Independent of
   * {@code gravitino.maintenance.expand.enqueueBatchSize}, which only bounds how many tables one
   * expand pick may hand to {@link #enqueueDueNow}.
   */
  static final int SNAPSHOT_EXPIRY_BATCH_CAPACITY = 10;

  private final DataSource dataSource;
  private final MaintenanceJobSpecFactory specFactory;
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
  @VisibleForTesting
  public RelationalCronJobScheduler(
      DataSource dataSource,
      Config gravitinoConfig,
      Function<List<Long>, List<ResolvedTableRef>> tableRefResolver) {
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.specFactory = new MaintenanceJobSpecFactory(Preconditions.checkNotNull(gravitinoConfig));
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
    int expandLimit = Math.max(1, batchCapacity);
    SchedulerClient client = client(dataSource);
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
   * Builds a SchedulerClient that can commit against the entity-store DataSource.
   *
   * @param dataSource entity-store pool
   * @return client bound to {@link TableMaintenanceSchedulerPools#CRON_TASKS_TABLE}
   */
  @VisibleForTesting
  public static SchedulerClient client(DataSource dataSource) {
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
      if (isQueuedForPolicy(client, candidate.tableId(), policy.id())) {
        continue;
      }
      if (candidate.batchableForSnapshotExpiry()) {
        batchable.add(candidate.tableId());
      } else {
        tableLevel.add(candidate.tableId());
      }
    }
    int scheduled = packIntoBatches(client, policy, batchable, batchCapacity, due);
    for (Long tableId : tableLevel) {
      scheduled += enqueueTableSnapshotExpiry(client, policy, tableId, batchCapacity, due);
    }
    absorbPeersIntoOpenBatches(client, batchCapacity);
    return scheduled;
  }

  /**
   * Enqueues one table-level snapshot-expiry winner: prefer filling an incomplete same-catalog
   * {@code batch:}, else coalesce with other unpicked expire peers into a new {@code batch:}, else
   * schedule a lone {@code table:} row.
   */
  private int enqueueTableSnapshotExpiry(
      SchedulerClient client, PolicyEntity policy, long tableId, int batchCapacity, Instant due) {
    if (isQueuedForPolicy(client, tableId, policy.id())) {
      return 0;
    }
    Optional<String> catalog = catalogNameOf(tableId);
    if (catalog.isEmpty()) {
      return 0;
    }
    Optional<OpenExpireJob> incomplete =
        findIncompleteExpireBatch(client, batchCapacity, catalog.get());
    if (incomplete.isPresent()
        && mergeTablesIntoJob(client, incomplete.get(), List.of(tableId), batchCapacity)) {
      return 0;
    }

    List<OpenExpireJob> peers =
        selectAbsorbablePeers(
            client, catalog.get(), batchCapacity - 1, batchCapacity, Set.of(), Set.of(tableId));
    if (!peers.isEmpty()) {
      List<Long> chunk = new ArrayList<>();
      chunk.add(tableId);
      for (OpenExpireJob peer : peers) {
        chunk.addAll(peer.spec.tableIds());
      }
      if (chunk.size() <= batchCapacity && scheduleBatch(client, policy, chunk, due)) {
        cancelJobs(client, peers);
        return 1;
      }
    }
    return scheduleTable(client, policy, tableId, due) ? 1 : 0;
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
    Map<String, List<Long>> byCatalog = groupByCatalog(tableIds);
    int newRows = 0;
    for (Map.Entry<String, List<Long>> entry : byCatalog.entrySet()) {
      newRows +=
          packSameCatalog(client, policy, entry.getKey(), entry.getValue(), batchCapacity, due);
    }
    return newRows;
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
        LOG.warn("Skipping unresolved table {} from snapshot-expiry batch packing", tableId);
        continue;
      }
      byCatalog.computeIfAbsent(ref.catalogName(), ignored -> new ArrayList<>()).add(tableId);
    }
    return byCatalog;
  }

  private Optional<String> catalogNameOf(long tableId) {
    List<ResolvedTableRef> resolved = tableRefResolver.apply(List.of(tableId));
    if (resolved.isEmpty()) {
      LOG.warn("Skipping unresolved table {} from snapshot-expiry table enqueue", tableId);
      return Optional.empty();
    }
    return Optional.of(resolved.get(0).catalogName());
  }

  private int packSameCatalog(
      SchedulerClient client,
      PolicyEntity policy,
      String catalogName,
      List<Long> tableIds,
      int batchCapacity,
      Instant due) {
    List<Long> remaining = new ArrayList<>(tableIds);
    int newRows = 0;
    while (!remaining.isEmpty()) {
      Optional<OpenExpireJob> incomplete =
          findIncompleteExpireBatch(client, batchCapacity, catalogName);
      if (incomplete.isPresent()) {
        OpenExpireJob open = incomplete.get();
        int room = batchCapacity - open.spec.tableIds().size();
        int fromRemaining = Math.min(room, remaining.size());
        List<Long> chunk = new ArrayList<>(remaining.subList(0, fromRemaining));
        Set<Long> excludeTables = new HashSet<>(open.spec.tableIds());
        excludeTables.addAll(chunk);
        List<OpenExpireJob> peers =
            selectAbsorbablePeers(
                client,
                catalogName,
                room - chunk.size(),
                batchCapacity,
                Set.of(open.instanceId.getId()),
                excludeTables);
        for (OpenExpireJob peer : peers) {
          chunk.addAll(peer.spec.tableIds());
        }
        if (mergeTablesIntoJob(client, open, chunk, batchCapacity)) {
          remaining.subList(0, fromRemaining).clear();
          cancelJobs(client, peers);
          continue;
        }
      }
      int end = Math.min(batchCapacity, remaining.size());
      List<Long> chunk = new ArrayList<>(remaining.subList(0, end));
      List<OpenExpireJob> peers =
          selectAbsorbablePeers(
              client,
              catalogName,
              batchCapacity - chunk.size(),
              batchCapacity,
              Set.of(),
              new HashSet<>(chunk));
      for (OpenExpireJob peer : peers) {
        chunk.addAll(peer.spec.tableIds());
      }
      if (chunk.size() <= batchCapacity && scheduleBatch(client, policy, chunk, due)) {
        newRows++;
        remaining.subList(0, end).clear();
        cancelJobs(client, peers);
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

  /** Fills incomplete expire {@code batch:} rows with whole unpicked same-catalog peers. */
  private void absorbPeersIntoOpenBatches(SchedulerClient client, int batchCapacity) {
    Set<String> saturated = new HashSet<>();
    for (; ; ) {
      Optional<OpenExpireJob> incomplete =
          listOpenExpireJobs(client).stream()
              .filter(j -> j.isBatch && !j.picked && j.spec.tableIds().size() < batchCapacity)
              .filter(j -> j.catalog != null)
              .filter(j -> !saturated.contains(j.instanceId.getId()))
              .min(Comparator.comparingLong(j -> j.batchId));
      if (incomplete.isEmpty()) {
        return;
      }
      OpenExpireJob open = incomplete.get();
      int room = batchCapacity - open.spec.tableIds().size();
      List<OpenExpireJob> peers =
          selectAbsorbablePeers(
              client,
              open.catalog,
              room,
              batchCapacity,
              Set.of(open.instanceId.getId()),
              new HashSet<>(open.spec.tableIds()));
      if (peers.isEmpty()) {
        saturated.add(open.instanceId.getId());
        continue;
      }
      List<Long> extra = new ArrayList<>();
      for (OpenExpireJob peer : peers) {
        extra.addAll(peer.spec.tableIds());
      }
      if (!mergeTablesIntoJob(client, open, extra, batchCapacity)) {
        return;
      }
      cancelJobs(client, peers);
    }
  }

  /**
   * Selects whole unpicked expire peers that fit in {@code room} without exceeding capacity. Skips
   * peers already at capacity. All-or-nothing per peer.
   */
  private static List<OpenExpireJob> selectAbsorbablePeers(
      SchedulerClient client,
      String catalogName,
      int room,
      int batchCapacity,
      Set<String> excludeInstanceIds,
      Set<Long> excludeTableIds) {
    if (room <= 0) {
      return List.of();
    }
    List<OpenExpireJob> candidates =
        listOpenExpireJobs(client).stream()
            .filter(j -> !j.picked)
            .filter(j -> catalogName.equals(j.catalog))
            .filter(j -> !excludeInstanceIds.contains(j.instanceId.getId()))
            .filter(j -> j.spec.tableIds().size() < batchCapacity)
            .filter(j -> j.spec.tableIds().stream().noneMatch(excludeTableIds::contains))
            .sorted(
                Comparator.comparingInt((OpenExpireJob j) -> j.spec.tableIds().size())
                    .thenComparing(j -> j.instanceId.getId()))
            .toList();
    List<OpenExpireJob> taken = new ArrayList<>();
    int left = room;
    Set<Long> claimedTables = new HashSet<>(excludeTableIds);
    for (OpenExpireJob peer : candidates) {
      int size = peer.spec.tableIds().size();
      if (size > left) {
        continue;
      }
      if (peer.spec.tableIds().stream().anyMatch(claimedTables::contains)) {
        continue;
      }
      taken.add(peer);
      claimedTables.addAll(peer.spec.tableIds());
      left -= size;
      if (left == 0) {
        break;
      }
    }
    return taken;
  }

  private boolean mergeTablesIntoJob(
      SchedulerClient client, OpenExpireJob open, List<Long> extraTableIds, int batchCapacity) {
    if (extraTableIds.isEmpty()) {
      return true;
    }
    Map<Long, TableTarget> byId = new LinkedHashMap<>();
    for (TableTarget target : open.spec.getTables()) {
      byId.put(target.getTableId(), target);
    }
    if (byId.size() + extraTableIds.stream().filter(id -> !byId.containsKey(id)).count()
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
    String metalake = open.spec.metalake();
    if (metalake == null) {
      LOG.warn(
          "Could not merge tables into cron-job {}: missing metalake on open spec",
          open.instanceId.getId());
      return false;
    }
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
      if (!byId.containsKey(ref.tableId())) {
        byId.put(ref.tableId(), MaintenanceJobSpecFactory.toTarget(metalake, ref));
        added = true;
      }
    }
    if (!added) {
      return true;
    }
    if (byId.size() > batchCapacity) {
      return false;
    }
    MaintenanceJobSpec merged =
        new MaintenanceJobSpec(
            new ArrayList<>(byId.values()), open.spec.getOperations(), open.spec.getJobConf());
    try {
      return client.reschedule(open.instanceId, open.executionTime, merged);
    } catch (RuntimeException e) {
      LOG.debug(
          "Could not merge tables into cron-job {}: {}", open.instanceId.getId(), e.toString());
      return false;
    }
  }

  private static void cancelJobs(SchedulerClient client, List<OpenExpireJob> jobs) {
    for (OpenExpireJob job : jobs) {
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
            "Failed to cancel absorbed cron-job {} after snapshot-expiry merge: {}",
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
    String id = CronJobIds.forTable(tableId, policy.id());
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
    String id = CronJobIds.forBatch(batchId, policy.id());
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

  private static Optional<OpenExpireJob> findIncompleteExpireBatch(
      SchedulerClient client, int batchCapacity, String catalogName) {
    return listOpenExpireJobs(client).stream()
        .filter(j -> j.isBatch && !j.picked && j.spec.tableIds().size() < batchCapacity)
        .filter(j -> catalogName.equals(j.catalog))
        .min(Comparator.comparingLong(j -> j.batchId));
  }

  private static List<OpenExpireJob> listOpenExpireJobs(SchedulerClient client) {
    List<OpenExpireJob> open = new ArrayList<>();
    for (ScheduledExecution<MaintenanceJobSpec> execution :
        client.getScheduledExecutionsForTask(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceJobSpec.class)) {
      MaintenanceJobSpec data = execution.getData();
      if (data == null || !data.isValid() || !isExpireSnapshots(data)) {
        continue;
      }
      String instanceId = execution.getTaskInstance().getId();
      String catalog = data.getTables().isEmpty() ? null : data.getTables().get(0).catalogName();
      Optional<CronJobIds.BatchPolicyIds> batch = CronJobIds.parseBatch(instanceId);
      if (batch.isPresent()) {
        open.add(
            new OpenExpireJob(
                TaskInstanceId.of(TableMaintenanceSchedulerPools.CRON_JOB, instanceId),
                true,
                batch.get().batchId(),
                batch.get().policyId(),
                execution.getExecutionTime(),
                data,
                catalog,
                execution.isPicked()));
        continue;
      }
      Optional<CronJobIds.TablePolicyIds> table = CronJobIds.parseTable(instanceId);
      if (table.isPresent()) {
        open.add(
            new OpenExpireJob(
                TaskInstanceId.of(TableMaintenanceSchedulerPools.CRON_JOB, instanceId),
                false,
                -1L,
                table.get().policyId(),
                execution.getExecutionTime(),
                data,
                catalog,
                execution.isPicked()));
      }
    }
    return open;
  }

  private static boolean isExpireSnapshots(MaintenanceJobSpec spec) {
    return spec.getOperations().stream()
        .anyMatch(op -> EXPIRE_SNAPSHOTS.equals(op.getMaintenanceType()));
  }

  static long nextBatchId(SchedulerClient client, long policyId) {
    long max = -1L;
    for (OpenExpireJob job : listOpenExpireJobs(client)) {
      if (job.isBatch && job.policyId == policyId) {
        max = Math.max(max, job.batchId);
      }
    }
    return max + 1L;
  }

  /**
   * True when a table already has a queued cron-job for this policy, or any queued snapshot-expiry
   * cron-job (any policy) that already covers the table.
   */
  static boolean isQueuedForPolicy(SchedulerClient client, long tableId, long policyId) {
    if (client
        .getScheduledExecution(
            TaskInstanceId.of(
                TableMaintenanceSchedulerPools.CRON_JOB, CronJobIds.forTable(tableId, policyId)))
        .isPresent()) {
      return true;
    }
    for (OpenExpireJob job : listOpenExpireJobs(client)) {
      if (job.spec.tableIds().contains(tableId)) {
        return true;
      }
    }
    for (ScheduledExecution<MaintenanceJobSpec> execution :
        client.getScheduledExecutionsForTask(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceJobSpec.class)) {
      Optional<CronJobIds.BatchPolicyIds> parsed =
          CronJobIds.parseBatch(execution.getTaskInstance().getId());
      if (parsed.isEmpty() || parsed.get().policyId() != policyId) {
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

  private static final class OpenExpireJob {
    private final TaskInstanceId instanceId;
    private final boolean isBatch;
    private final long batchId;
    private final long policyId;
    private final Instant executionTime;
    private final MaintenanceJobSpec spec;
    private final String catalog;
    private final boolean picked;

    private OpenExpireJob(
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
