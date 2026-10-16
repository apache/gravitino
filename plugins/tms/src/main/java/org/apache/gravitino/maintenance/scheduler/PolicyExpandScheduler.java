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

import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.CronStyle;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.sql.DataSource;

/**
 * Writes {@code policy-expand} rows into {@link TableMaintenanceSchedulerPools#EXPAND_TASKS_TABLE}
 * through a db-scheduler client.
 *
 * <p>{@code timeTrigger} is a six-field Quartz cron. This class is the runtime that decides whether
 * the expression has a next fire time; policy content does not parse cron itself.
 */
public final class PolicyExpandScheduler implements PolicyExpandSchedule {

  private final SchedulerClient client;

  private PolicyExpandScheduler(SchedulerClient client) {
    this.client = client;
  }

  /**
   * Opens a client against the expand-task table.
   *
   * @param dataSource relational JDBC pool shared with the entity store
   * @return scheduler that inserts, moves, and deletes expand rows
   */
  public static PolicyExpandScheduler create(DataSource dataSource) {
    // SchedulerClient never commits when auto-commit is off, and the entity-store pool leaves
    // auto-commit off. Forcing auto-commit makes the insert survive connection close.
    SchedulerClient client =
        SchedulerClient.Builder.create(
                new AutoCommitDataSource(dataSource),
                TableMaintenanceSchedulerPools.policyExpandTask())
            .tableName(TableMaintenanceSchedulerPools.EXPAND_TASKS_TABLE)
            .build();
    return new PolicyExpandScheduler(client);
  }

  @Override
  public void sync(long policyId, boolean enabled, @Nullable String timeTrigger) {
    TaskInstanceId instanceId = instanceId(policyId);
    if (!enabled || timeTrigger == null || timeTrigger.isBlank()) {
      cancelIfPresent(instanceId);
      return;
    }
    Instant due = nextExecution(timeTrigger);
    boolean scheduled =
        client.schedule(
            new TaskInstance<>(TableMaintenanceSchedulerPools.POLICY_EXPAND, instanceId.getId()),
            due,
            SchedulerClient.ScheduleOptions.WHEN_EXISTS_RESCHEDULE);
    if (!scheduled) {
      throw new IllegalStateException(
          "Failed to schedule policy-expand instance " + instanceId.getId() + " at " + due);
    }
  }

  @Override
  public void remove(long policyId) {
    cancelIfPresent(instanceId(policyId));
  }

  /**
   * @param policyId persisted policy id
   * @return next execution time of the expand row, or empty when the row is absent
   */
  Optional<Instant> scheduledExecutionTime(long policyId) {
    return client
        .getScheduledExecution(instanceId(policyId))
        .map(execution -> execution.getExecutionTime());
  }

  /**
   * Next fire time for a Quartz cron, strictly after now.
   *
   * @param timeTrigger six-field Quartz cron expression
   * @return next execution instant in the system default zone
   */
  static Instant nextExecution(String timeTrigger) {
    CronSchedule schedule = new CronSchedule(timeTrigger, ZoneId.systemDefault(), CronStyle.QUARTZ);
    Instant next = schedule.getNextExecutionTime(ExecutionComplete.simulatedSuccess(Instant.now()));
    if (next == null || Schedule.NEVER.equals(next)) {
      throw new IllegalArgumentException("timeTrigger has no next execution: " + timeTrigger);
    }
    return next;
  }

  private static TaskInstanceId instanceId(long policyId) {
    return TaskInstanceId.of(TableMaintenanceSchedulerPools.POLICY_EXPAND, Long.toString(policyId));
  }

  private void cancelIfPresent(TaskInstanceId instanceId) {
    if (client.getScheduledExecution(instanceId).isPresent()) {
      client.cancel(instanceId);
    }
  }

  /** Enables auto-commit so db-scheduler client writes are not rolled back on close. */
  private static final class AutoCommitDataSource implements DataSource {
    private final DataSource delegate;

    private AutoCommitDataSource(DataSource delegate) {
      this.delegate = delegate;
    }

    @Override
    public Connection getConnection() throws SQLException {
      return enableAutoCommit(delegate.getConnection());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return enableAutoCommit(delegate.getConnection(username, password));
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
      delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
      delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      return delegate.getLoginTimeout();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return delegate.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return delegate.isWrapperFor(iface);
    }

    private static Connection enableAutoCommit(Connection connection) throws SQLException {
      connection.setAutoCommit(true);
      return connection;
    }
  }
}
