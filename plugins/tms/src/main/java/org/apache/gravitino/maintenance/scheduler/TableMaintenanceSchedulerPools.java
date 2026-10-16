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
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts and stops three isolated db-scheduler pools (§5.2, §7.2), each with its own task table
 * ({@link #EXPAND_TASKS_TABLE}, {@link #CRON_TASKS_TABLE}, {@link #COMMIT_TASKS_TABLE}).
 *
 * <p>db-scheduler requires a dedicated {@code tableName} per Scheduler instance for multi-pool
 * isolation; sharing one physical table across pools is unsupported (see design §5.2).
 *
 * <p>Task handlers are intentionally minimal in this change; submit and occupancy logic land in
 * follow-up PRs.
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

  private final List<Scheduler> schedulers = new ArrayList<>();

  /**
   * Creates and starts expand, cron, and commit scheduler pools.
   *
   * <p>If any pool fails to start, already-started pools are stopped and this instance returns to
   * the idle state so {@link #start} may be retried.
   *
   * @param dataSource relational JDBC pool shared with the entity store
   * @param config maintenance scheduler settings
   */
  public void start(DataSource dataSource, TableMaintenanceConfig config) {
    Preconditions.checkState(schedulers.isEmpty(), "scheduler pools already started");
    Preconditions.checkNotNull(dataSource, "dataSource must not be null");

    List<Scheduler> built =
        List.of(
            buildScheduler(
                dataSource,
                config,
                config.expandThreads(),
                EXPAND_TASKS_TABLE,
                noopTask(POLICY_EXPAND)),
            buildScheduler(
                dataSource, config, config.cronThreads(), CRON_TASKS_TABLE, noopTask(CRON_JOB)),
            buildScheduler(
                dataSource,
                config,
                config.commitThreads(),
                COMMIT_TASKS_TABLE,
                noopTask(COMMIT_JOB)));

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
      Task<Void> task) {
    return Scheduler.create(dataSource, task)
        .threads(threads)
        .pollingInterval(Duration.ofMillis(config.pollingIntervalMs()))
        .heartbeatInterval(Duration.ofMillis(config.heartbeatIntervalMs()))
        .missedHeartbeatsLimit(config.missedHeartbeatsLimit())
        .tableName(tableName)
        .registerShutdownHook()
        .build();
  }

  private static Task<Void> noopTask(String taskName) {
    return Tasks.custom(taskName, Void.class)
        .execute(
            (taskInstance, executionContext) -> {
              LOG.debug(
                  "TMS scheduler callback for {} instance {}", taskName, taskInstance.getId());
              return (executionComplete, executionOperations) -> {};
            });
  }
}
