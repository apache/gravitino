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
package org.apache.gravitino.maintenance.scheduler.commit;

import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.occupancy.OccupancyGate;
import org.apache.gravitino.maintenance.policy.MaintenanceMinIntervalConfigs;
import org.apache.gravitino.maintenance.scheduler.AutocommitDataSources;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot;
import org.apache.gravitino.maintenance.scheduler.MaintenanceSchedulerSerializers;
import org.apache.gravitino.maintenance.scheduler.RelationalPolicyLookup;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceTableRefs.ResolvedTableRef;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.PolicyContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schedules {@code commit-job} rows through a db-scheduler {@link SchedulerClient} on the
 * entity-store DataSource.
 *
 * <p>Enqueue collects the onCommit chain, drops members still inside {@code minIntervalMs}, checks
 * auth consistency, builds a {@link MaintenanceJobSnapshot}, and inserts only when the snapshot is
 * non-empty. Coalesce leaves an existing row's {@code task_data} untouched.
 */
public final class RelationalCommitJobScheduler implements CommitJobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(RelationalCommitJobScheduler.class);

  private final DataSource dataSource;
  private final Config gravitinoConfig;
  private final CommitChainCollector chainCollector;
  private final CommitOccupancyGate occupancyGate;
  private final CommitJobSnapshotFactory snapshotFactory;

  /**
   * @param dataSource relational JDBC pool shared with the entity store
   * @param gravitinoConfig server config for auth overlay and minInterval fallbacks
   */
  public RelationalCommitJobScheduler(DataSource dataSource, Config gravitinoConfig) {
    this(
        dataSource,
        gravitinoConfig,
        new CommitChainCollector(new RelationalPolicyLookup()),
        CommitOccupancyGate.of(new OccupancyGate(TableMaintenanceActionMetaService.getInstance())));
  }

  /**
   * Visible for tests.
   *
   * @param dataSource JDBC pool
   * @param gravitinoConfig server config
   * @param chainCollector onCommit chain builder
   * @param occupancyGate cooldown / in-flight checks for enqueue gating
   */
  RelationalCommitJobScheduler(
      DataSource dataSource,
      Config gravitinoConfig,
      CommitChainCollector chainCollector,
      CommitOccupancyGate occupancyGate) {
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.gravitinoConfig = Preconditions.checkNotNull(gravitinoConfig);
    this.chainCollector = Preconditions.checkNotNull(chainCollector);
    this.occupancyGate = Preconditions.checkNotNull(occupancyGate);
    this.snapshotFactory = new CommitJobSnapshotFactory(gravitinoConfig);
  }

  @Override
  public boolean upsertDueNow(long tableId) {
    Preconditions.checkArgument(tableId > 0, "tableId must be positive");
    Optional<MaintenanceJobSnapshot> snapshot = buildSnapshot(tableId);
    if (snapshot.isEmpty()) {
      return false;
    }
    SchedulerClient client = client(dataSource);
    String id = Long.toString(tableId);
    return client.scheduleIfNotExists(
        new TaskInstance<>(TableMaintenanceSchedulerPools.COMMIT_JOB, id, snapshot.get()),
        Instant.now());
  }

  /**
   * Builds a SchedulerClient that can commit against the entity-store DataSource.
   *
   * @param dataSource entity-store pool
   * @return client bound to {@link TableMaintenanceSchedulerPools#COMMIT_TASKS_TABLE}
   */
  static SchedulerClient client(DataSource dataSource) {
    return SchedulerClient.Builder.create(
            AutocommitDataSources.wrap(dataSource), TableMaintenanceSchedulerPools.commitJobTask())
        .serializer(MaintenanceSchedulerSerializers.jackson())
        .tableName(TableMaintenanceSchedulerPools.COMMIT_TASKS_TABLE)
        .build();
  }

  private Optional<MaintenanceJobSnapshot> buildSnapshot(long tableId) {
    List<CommitChainMember> collected = chainCollector.collect(tableId);
    if (collected.isEmpty()) {
      LOG.debug("Skipping commit-job enqueue for table {}: empty onCommit chain", tableId);
      return Optional.empty();
    }

    long nowMs = System.currentTimeMillis();
    List<CommitChainMember> chain = filterDueToMinInterval(tableId, collected, nowMs);
    if (chain.isEmpty()) {
      LOG.debug(
          "Skipping commit-job enqueue for table {}: all onCommit types within minIntervalMs",
          tableId);
      return Optional.empty();
    }

    try {
      CommitAuthConsistency.requireConsistent(chain, gravitinoConfig);
    } catch (IllegalArgumentException e) {
      LOG.error("Skipping commit-job enqueue for table {}: {}", tableId, e.getMessage());
      return Optional.empty();
    }

    Optional<ResolvedTableRef> resolved = chainCollector.resolveTableRef(tableId);
    if (resolved.isEmpty()) {
      LOG.warn(
          "Skipping commit-job enqueue for table {}: could not resolve table identifier", tableId);
      return Optional.empty();
    }

    try {
      return Optional.of(snapshotFactory.forCommit(chain, resolved.get()));
    } catch (RuntimeException e) {
      LOG.warn(
          "Skipping commit-job enqueue for table {}: failed to build jobConf snapshot: {}",
          tableId,
          e.toString());
      return Optional.empty();
    }
  }

  private List<CommitChainMember> filterDueToMinInterval(
      long tableId, List<CommitChainMember> collected, long nowMs) {
    List<CommitChainMember> due = new ArrayList<>();
    for (CommitChainMember member : collected) {
      long minIntervalMs =
          MaintenanceMinIntervalConfigs.resolve(
              gravitinoConfig, member.policyType(), policyMinIntervalMs(member.policy().content()));
      if (occupancyGate.withinMinInterval(tableId, member.policy().id(), minIntervalMs, nowMs)) {
        LOG.debug(
            "Dropping type {} policy {} from commit-job enqueue for table {} (within minIntervalMs={})",
            member.policyType(),
            member.policy().id(),
            tableId,
            minIntervalMs);
        continue;
      }
      due.add(member);
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
