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
package org.apache.gravitino.maintenance.scheduler;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.occupancy.OccupancyGate;
import org.apache.gravitino.maintenance.scheduler.job.commit.CommitOccupancyGate;
import org.apache.gravitino.maintenance.scheduler.job.commit.MaintenanceCommitSubmitPipeline;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceJobConfBuilder;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceJobSubmitter;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceSparkSubmitPipeline;
import org.apache.gravitino.maintenance.scheduler.job.cron.PendingCronExpandOccupancyGate;
import org.apache.gravitino.maintenance.scheduler.job.cron.RelationalCronJobScheduler;
import org.apache.gravitino.maintenance.scheduler.policy.expand.ExpandOccupancyGate;
import org.apache.gravitino.maintenance.scheduler.policy.expand.NearestPolicyTableEnumerator;
import org.apache.gravitino.maintenance.scheduler.policy.expand.PolicyExpandPipeline;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts and stops three isolated db-scheduler pools (§5.2, §7.2), each with its own task table
 * ({@link #EXPAND_TASKS_TABLE}, {@link #CRON_TASKS_TABLE}, {@link #COMMIT_TASKS_TABLE}).
 *
 * <p>db-scheduler requires a dedicated {@code tableName} per Scheduler instance for multi-pool
 * isolation; sharing one physical table across pools is unsupported (see design §5.2).
 *
 * <p>The expand pool fans a due {@code policy-expand} lease into {@code cron-job} rows with an
 * enqueue-time {@link MaintenanceJobSpec}. The cron and commit pools consume that spec, claim a
 * soft slot, and submit Spark.
 */
public class TableMaintenanceSchedulerPools implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(TableMaintenanceSchedulerPools.class);

  /** Expand-pool db-scheduler table. */
  public static final String EXPAND_TASKS_TABLE = "scheduled_expand_tasks";

  /** Cron-pool db-scheduler table. */
  public static final String CRON_TASKS_TABLE = "scheduled_cron_tasks";

  /** Commit-pool db-scheduler table. */
  public static final String COMMIT_TASKS_TABLE = "scheduled_commit_tasks";

  /** Long-lived policy expand lease ({@code task_name}). */
  public static final String POLICY_EXPAND = "policy-expand";

  /** One-shot crontab Spark submit ({@code task_name}). */
  public static final String CRON_JOB = "cron-job";

  /** One-shot on-commit chained Spark submit ({@code task_name}). */
  public static final String COMMIT_JOB = "commit-job";

  /**
   * Expand pipeline supplier used by {@link #policyExpandTask()}. {@link #start} installs the
   * relational implementation.
   */
  private static final AtomicReference<Supplier<PolicyExpandPipeline>> EXPAND_PIPELINE =
      new AtomicReference<>(
          () -> {
            throw new IllegalStateException("policy-expand pipeline is not configured");
          });

  /** Cron submit handler used by {@link #cronJobTask()}; bound in {@link #start}. */
  static final AtomicReference<BiConsumer<String, MaintenanceJobSpec>> CRON_SUBMIT =
      new AtomicReference<>(unconfiguredSubmit("cron-job"));

  /** Commit submit handler used by {@link #commitJobTask()}; bound in {@link #start}. */
  static final AtomicReference<BiConsumer<String, MaintenanceJobSpec>> COMMIT_SUBMIT =
      new AtomicReference<>(unconfiguredSubmit("commit-job"));

  private final List<Scheduler> schedulers = new ArrayList<>();

  /**
   * Creates and starts expand, cron, and commit scheduler pools.
   *
   * <p>If any pool fails to start, already-started pools are stopped and this instance returns to
   * the idle state so {@link #start} may be retried.
   *
   * @param dataSource relational JDBC pool shared with the entity store
   * @param config maintenance scheduler settings
   * @param gravitinoConfig server config for {@code minIntervalMs} fallbacks and jobConf overlay
   */
  public void start(DataSource dataSource, TableMaintenanceConfig config, Config gravitinoConfig) {
    Preconditions.checkState(schedulers.isEmpty(), "scheduler pools already started");
    Preconditions.checkNotNull(dataSource, "dataSource must not be null");
    Preconditions.checkNotNull(gravitinoConfig, "gravitinoConfig must not be null");

    OccupancyGate occupancyGate =
        new OccupancyGate(TableMaintenanceActionMetaService.getInstance(), config.claimTimeoutMs());
    ExpandOccupancyGate expandGate =
        new PendingCronExpandOccupancyGate(occupancyGate::shouldSkip, dataSource);
    PolicyExpandPipeline pipeline =
        new PolicyExpandPipeline(
            new NearestPolicyTableEnumerator(),
            new RelationalCronJobScheduler(dataSource, gravitinoConfig),
            expandGate,
            gravitinoConfig,
            config.expandEnqueueBatchSize());
    EXPAND_PIPELINE.set(() -> pipeline);

    MaintenanceSparkSubmitPipeline cronSubmit =
        new MaintenanceSparkSubmitPipeline(
            occupancyGate::isInFlight,
            TableMaintenanceActionMetaService.getInstance(),
            MaintenanceJobSubmitter.fromEnv(),
            new MaintenanceJobConfBuilder(gravitinoConfig),
            config.claimTimeoutMs());
    CRON_SUBMIT.set(cronSubmit::submit);

    MaintenanceCommitSubmitPipeline commitSubmit =
        new MaintenanceCommitSubmitPipeline(
            CommitOccupancyGate.of(occupancyGate),
            TableMaintenanceActionMetaService.getInstance(),
            MaintenanceJobSubmitter.fromEnv(),
            new MaintenanceJobConfBuilder(gravitinoConfig),
            config.claimTimeoutMs());
    COMMIT_SUBMIT.set(commitSubmit::submit);

    List<Scheduler> built =
        List.of(
            buildScheduler(
                dataSource, config, config.expandThreads(), EXPAND_TASKS_TABLE, policyExpandTask()),
            buildScheduler(
                dataSource, config, config.cronThreads(), CRON_TASKS_TABLE, cronJobTask()),
            buildScheduler(
                dataSource, config, config.commitThreads(), COMMIT_TASKS_TABLE, commitJobTask()));

    try {
      for (Scheduler scheduler : built) {
        scheduler.start();
        schedulers.add(scheduler);
      }
    } catch (RuntimeException e) {
      stop();
      throw e;
    }
    LOG.info("Started {} table maintenance scheduler pools", schedulers.size());
  }

  /** Stops all scheduler pools. */
  public void stop() {
    for (Scheduler scheduler : schedulers) {
      try {
        scheduler.stop();
      } catch (Exception e) {
        LOG.warn("Failed to stop a table maintenance scheduler pool", e);
      }
    }
    schedulers.clear();
    EXPAND_PIPELINE.set(
        () -> {
          throw new IllegalStateException("policy-expand pipeline is not configured");
        });
    CRON_SUBMIT.set(unconfiguredSubmit("cron-job"));
    COMMIT_SUBMIT.set(unconfiguredSubmit("commit-job"));
    LOG.info("Stopped table maintenance scheduler pools");
  }

  @Override
  public void close() {
    stop();
  }

  private static Scheduler buildScheduler(
      DataSource dataSource,
      TableMaintenanceConfig config,
      int threads,
      String tableName,
      Task<?> task) {
    return Scheduler.create(dataSource, task)
        .threads(threads)
        .pollingInterval(Duration.ofMillis(config.pollingIntervalMs()))
        .heartbeatInterval(Duration.ofMillis(config.heartbeatIntervalMs()))
        .missedHeartbeatsLimit(config.missedHeartbeatsLimit())
        .serializer(new JacksonSerializer(JsonUtils.anyFieldMapper()))
        .tableName(tableName)
        .commitWhenAutocommitDisabled(true)
        .registerShutdownHook()
        .build();
  }

  /**
   * Expand-pool task definition used by the running expand scheduler.
   *
   * <p>On each due pick the handler expands the policy into {@code cron-job} rows and reschedules
   * (or removes) the expand lease.
   *
   * @return the {@code policy-expand} task
   */
  public static Task<Void> policyExpandTask() {
    return Tasks.custom(POLICY_EXPAND, Void.class)
        .execute(
            (taskInstance, executionContext) -> {
              long policyId = Long.parseLong(taskInstance.getId());
              Optional<Instant> nextDue = EXPAND_PIPELINE.get().get().expand(policyId);
              return (executionComplete, executionOperations) -> {
                if (nextDue.isEmpty()) {
                  executionOperations.remove();
                } else {
                  executionOperations.reschedule(executionComplete, nextDue.get());
                }
              };
            });
  }

  /**
   * Cron-pool task definition. Consumes the enqueue-time spec, claims a soft slot, submits Spark,
   * then removes the one-shot row so a later expand pick can schedule again.
   *
   * @return the {@code cron-job} task
   */
  public static Task<MaintenanceJobSpec> cronJobTask() {
    return Tasks.custom(CRON_JOB, MaintenanceJobSpec.class)
        .execute(
            (taskInstance, executionContext) -> {
              try {
                CRON_SUBMIT.get().accept(taskInstance.getId(), taskInstance.getData());
              } catch (RuntimeException e) {
                LOG.warn(
                    "cron-job submit failed for instance {}; removing one-shot row",
                    taskInstance.getId(),
                    e);
              }
              return (executionComplete, executionOperations) -> executionOperations.remove();
            });
  }

  /**
   * Commit-pool task definition. Consumes the enqueue-time chain spec, claims a soft slot, submits
   * Spark, then removes the one-shot row.
   *
   * @return the {@code commit-job} task
   */
  public static Task<MaintenanceJobSpec> commitJobTask() {
    return Tasks.custom(COMMIT_JOB, MaintenanceJobSpec.class)
        .execute(
            (taskInstance, executionContext) -> {
              try {
                COMMIT_SUBMIT.get().accept(taskInstance.getId(), taskInstance.getData());
              } catch (RuntimeException e) {
                LOG.warn(
                    "commit-job submit failed for instance {}; removing one-shot row",
                    taskInstance.getId(),
                    e);
              }
              return (executionComplete, executionOperations) -> executionOperations.remove();
            });
  }

  private static BiConsumer<String, MaintenanceJobSpec> unconfiguredSubmit(String pool) {
    return (instanceId, spec) -> {
      throw new IllegalStateException(pool + " submit pipeline is not configured");
    };
  }
}
