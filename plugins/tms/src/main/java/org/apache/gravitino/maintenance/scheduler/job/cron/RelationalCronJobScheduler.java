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
 * filling an incomplete snapshot-expiry batch (same due time, new table list + shared jobConf).
 *
 * <p>The entity store disables JDBC autocommit; connections are wrapped via {@link
 * AutocommitDataSources} so each client call commits. {@code task_data} is JSON via {@link
 * JacksonSerializer + JsonUtils.anyFieldMapper()}.
 */
public final class RelationalCronJobScheduler implements CronJobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(RelationalCronJobScheduler.class);

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
  RelationalCronJobScheduler(
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
    // Same-catalog batches only: mixed catalogs would reuse wrong spark.sql.catalog.* props.
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
      Optional<OpenBatch> incomplete =
          findIncompleteBatch(client, policy.id(), batchCapacity, catalogName);
      if (incomplete.isPresent()) {
        OpenBatch open = incomplete.get();
        int room = batchCapacity - open.spec.tableIds().size();
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
        remaining.subList(0, end).clear();
        continue;
      }
      // Fail-closed: do not drop a whole multi-table chunk on build failure — peel one id.
      Long first = remaining.remove(0);
      if (chunk.size() > 1 && scheduleBatch(client, policy, List.of(first), due)) {
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

  private boolean mergeIntoBatch(SchedulerClient client, OpenBatch open, List<Long> extraTableIds) {
    Map<Long, TableTarget> byId = new LinkedHashMap<>();
    for (TableTarget target : open.spec.getTables()) {
      byId.put(target.getTableId(), target);
    }
    String openCatalog = open.spec.getTables().get(0).catalogName();
    if (openCatalog == null || openCatalog.isEmpty()) {
      LOG.warn(
          "Could not merge tables into cron-job {}: open batch missing catalog name",
          open.instanceId.getId());
      return false;
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
            "Could not merge table {} into cron-job {}: catalog {} != open batch catalog {}",
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
    MaintenanceJobSpec merged =
        new MaintenanceJobSpec(
            new ArrayList<>(byId.values()),
            open.spec.getOperations(),
            open.spec.getJobConf());
    try {
      return client.reschedule(open.instanceId, open.executionTime, merged);
    } catch (RuntimeException e) {
      LOG.debug(
          "Could not merge tables into cron-job {}: {}", open.instanceId.getId(), e.toString());
      return false;
    }
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

  private static Optional<OpenBatch> findIncompleteBatch(
      SchedulerClient client, long policyId, int batchCapacity, String catalogName) {
    return listOpenBatches(client, policyId).stream()
        .filter(b -> !b.picked && b.spec.tableIds().size() < batchCapacity)
        .filter(b -> catalogName.equals(b.spec.getTables().get(0).catalogName()))
        .min(Comparator.comparingLong(b -> b.batchId));
  }

  private static List<OpenBatch> listOpenBatches(SchedulerClient client, long policyId) {
    List<OpenBatch> open = new ArrayList<>();
    for (ScheduledExecution<MaintenanceJobSpec> execution :
        client.getScheduledExecutionsForTask(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceJobSpec.class)) {
      Optional<CronJobIds.BatchPolicyIds> parsed =
          CronJobIds.parseBatch(execution.getTaskInstance().getId());
      if (parsed.isEmpty() || parsed.get().policyId() != policyId) {
        continue;
      }
      MaintenanceJobSpec data = execution.getData();
      if (data == null || !data.isValid()) {
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
      if (batch.spec.tableIds().contains(tableId)) {
        return true;
      }
    }
    return false;
  }

  private static final class OpenBatch {
    private final TaskInstanceId instanceId;
    private final long batchId;
    private final Instant executionTime;
    private final MaintenanceJobSpec spec;
    private final boolean picked;

    private OpenBatch(
        TaskInstanceId instanceId,
        long batchId,
        Instant executionTime,
        MaintenanceJobSpec spec,
        boolean picked) {
      this.instanceId = instanceId;
      this.batchId = batchId;
      this.executionTime = executionTime;
      this.spec = spec;
      this.picked = picked;
    }
  }
}
