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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.occupancy.OccupancyGate;
import org.apache.gravitino.maintenance.policy.MaintenanceMinIntervalConfigs;
import org.apache.gravitino.maintenance.scheduler.AutocommitDataSources;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceAuthConsistency;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.PolicyContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enqueues one-shot {@code commit-job} rows through a db-scheduler {@link SchedulerClient} on the
 * entity-store DataSource (§5.2.3).
 *
 * <p>Instance id is {@code {table_id}}; bursts of commits on the same table coalesce via {@code
 * scheduleIfNotExists}. Enqueue collects the onCommit chain, drops members still inside {@code
 * minIntervalMs}, checks auth consistency, builds a {@link MaintenanceJobSpec}, and inserts only
 * when the spec is non-empty. Coalesce leaves an existing row's {@code task_data} untouched.
 */
public class CommitJobClient {

  private static final Logger LOG = LoggerFactory.getLogger(CommitJobClient.class);

  private final DataSource dataSource;
  private final Config gravitinoConfig;
  private final Function<Long, Optional<CollectedCommitChain>> chainCollector;
  private final CommitOccupancyGate occupancyGate;
  private final MaintenanceJobSpecFactory specFactory;

  /**
   * @param dataSource relational JDBC pool shared with the entity store
   * @param gravitinoConfig server config for auth overlay and minInterval fallbacks
   */
  public CommitJobClient(DataSource dataSource, Config gravitinoConfig) {
    this(
        dataSource,
        gravitinoConfig,
        new CommitChainCollector()::collect,
        CommitOccupancyGate.of(
            new OccupancyGate(
                TableMaintenanceActionMetaService.getInstance(),
                TableMaintenanceConfig.from(gravitinoConfig).claimTimeoutMs())));
  }

  /**
   * Visible for tests.
   *
   * @param dataSource JDBC pool
   * @param gravitinoConfig server config
   * @param chainCollector returns the collected onCommit chain for a table id
   * @param occupancyGate cooldown / in-flight checks for enqueue gating
   */
  @VisibleForTesting
  public CommitJobClient(
      DataSource dataSource,
      Config gravitinoConfig,
      Function<Long, Optional<CollectedCommitChain>> chainCollector,
      CommitOccupancyGate occupancyGate) {
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.gravitinoConfig = Preconditions.checkNotNull(gravitinoConfig);
    this.chainCollector = Preconditions.checkNotNull(chainCollector);
    this.occupancyGate = Preconditions.checkNotNull(occupancyGate);
    this.specFactory = new MaintenanceJobSpecFactory(gravitinoConfig);
  }

  /**
   * Builds an enqueue-time spec (chain + jobConf) and schedules a {@code commit-job} when one is
   * not already queued and the spec is non-empty.
   *
   * @param tableId table entity id
   * @return insert / coalesce / skip outcome
   */
  public CommitJobEnqueueResult enqueueNow(long tableId) {
    Preconditions.checkArgument(tableId > 0, "tableId must be positive");
    SchedulerClient client = schedulerClient(dataSource);
    String id = MaintenanceTaskIds.forCommitTable(tableId);
    // Coalesce without rebuilding task_data: keep the first enqueue-time spec.
    if (client
        .getScheduledExecution(TaskInstanceId.of(TableMaintenanceSchedulerPools.COMMIT_JOB, id))
        .isPresent()) {
      return CommitJobEnqueueResult.ALREADY_QUEUED;
    }
    Optional<MaintenanceJobSpec> spec = buildSpec(tableId);
    if (spec.isEmpty()) {
      return CommitJobEnqueueResult.SKIPPED;
    }
    boolean inserted =
        client.scheduleIfNotExists(
            new TaskInstance<>(TableMaintenanceSchedulerPools.COMMIT_JOB, id, spec.get()),
            Instant.now());
    return inserted ? CommitJobEnqueueResult.INSERTED : CommitJobEnqueueResult.ALREADY_QUEUED;
  }

  /**
   * Builds a db-scheduler {@link SchedulerClient} that can commit against the entity-store
   * DataSource.
   *
   * @param dataSource entity-store pool
   * @return client bound to {@link TableMaintenanceSchedulerPools#COMMIT_TASKS_TABLE}
   */
  @VisibleForTesting
  public static SchedulerClient schedulerClient(DataSource dataSource) {
    return SchedulerClient.Builder.create(
            AutocommitDataSources.wrap(dataSource), TableMaintenanceSchedulerPools.commitJobTask())
        .serializer(new JacksonSerializer(JsonUtils.anyFieldMapper()))
        .tableName(TableMaintenanceSchedulerPools.COMMIT_TASKS_TABLE)
        .build();
  }

  private Optional<MaintenanceJobSpec> buildSpec(long tableId) {
    Optional<CollectedCommitChain> collected = chainCollector.apply(tableId);
    if (collected.isEmpty()) {
      return Optional.empty();
    }
    List<PolicyEntity> policies = collected.get().policies();
    if (policies.isEmpty()) {
      LOG.debug("Skipping commit-job enqueue for table {}: empty onCommit chain", tableId);
      return Optional.empty();
    }

    long nowMs = System.currentTimeMillis();
    List<PolicyEntity> chain = filterByMinInterval(tableId, policies, nowMs);
    if (chain.isEmpty()) {
      LOG.debug(
          "Skipping commit-job enqueue for table {}: all onCommit types within minIntervalMs",
          tableId);
      return Optional.empty();
    }

    try {
      MaintenanceAuthConsistency.requireSameAuth(chain, gravitinoConfig);
    } catch (IllegalArgumentException e) {
      LOG.error("Skipping commit-job enqueue for table {}: {}", tableId, e.getMessage());
      return Optional.empty();
    }

    try {
      return Optional.of(specFactory.forCommit(chain, collected.get().tableRef()));
    } catch (RuntimeException e) {
      LOG.warn(
          "Skipping commit-job enqueue for table {}: failed to build jobConf spec: {}",
          tableId,
          e.toString());
      return Optional.empty();
    }
  }

  private List<PolicyEntity> filterByMinInterval(
      long tableId, List<PolicyEntity> collected, long nowMs) {
    List<PolicyEntity> due = new ArrayList<>();
    for (PolicyEntity policy : collected) {
      long minIntervalMs =
          MaintenanceMinIntervalConfigs.resolve(
              gravitinoConfig, policy.policyType(), policyMinIntervalMs(policy.content()));
      if (occupancyGate.withinMinInterval(tableId, policy.id(), minIntervalMs, nowMs)) {
        LOG.debug(
            "Dropping type {} policy {} from commit-job enqueue for table {} (within minIntervalMs={})",
            policy.policyType(),
            policy.id(),
            tableId,
            minIntervalMs);
        continue;
      }
      due.add(policy);
    }
    return due;
  }

  @Nullable
  private static Long policyMinIntervalMs(@Nullable PolicyContent content) {
    if (!(content instanceof ExecutablePolicyContent)) {
      return null;
    }
    return ((ExecutablePolicyContent) content).executionSpec().minIntervalMs();
  }
}
