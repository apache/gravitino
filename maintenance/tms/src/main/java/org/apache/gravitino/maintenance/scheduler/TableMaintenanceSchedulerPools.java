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
 * Starts and stops three isolated db-scheduler pools over {@code scheduled_tasks} (§5.2, §7.2).
 *
 * <p>Task handlers are intentionally minimal in this change; submit and occupancy logic land in
 * follow-up PRs.
 */
public class TableMaintenanceSchedulerPools implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(TableMaintenanceSchedulerPools.class);

  private static final String SCHEDULED_TASKS_TABLE = "scheduled_tasks";

  private final List<Scheduler> schedulers = new ArrayList<>();

  /**
   * Creates and starts expand, cron, and commit scheduler pools.
   *
   * @param dataSource relational JDBC pool shared with the entity store
   * @param config maintenance scheduler settings
   */
  public void start(DataSource dataSource, TableMaintenanceConfig config) {
    Preconditions.checkState(schedulers.isEmpty(), "scheduler pools already started");
    Preconditions.checkNotNull(dataSource, "dataSource must not be null");

    schedulers.add(
        buildScheduler(
            dataSource,
            config,
            config.expandThreads(),
            noopTask(TableMaintenanceTaskNames.POLICY_EXPAND)));
    schedulers.add(
        buildScheduler(
            dataSource,
            config,
            config.cronThreads(),
            noopTask(TableMaintenanceTaskNames.CRON_JOB)));
    schedulers.add(
        buildScheduler(
            dataSource,
            config,
            config.commitThreads(),
            noopTask(TableMaintenanceTaskNames.COMMIT_JOB)));

    schedulers.forEach(Scheduler::start);
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
      DataSource dataSource, TableMaintenanceConfig config, int threads, Task<Void> task) {
    return Scheduler.create(dataSource, task)
        .threads(threads)
        .pollingInterval(Duration.ofMillis(config.pollingIntervalMs()))
        .heartbeatInterval(Duration.ofMillis(config.heartbeatIntervalMs()))
        .missedHeartbeatsLimit(config.missedHeartbeatsLimit())
        .tableName(SCHEDULED_TASKS_TABLE)
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
