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
package org.apache.gravitino.maintenance.policy;

import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.CronStyle;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.google.common.annotations.VisibleForTesting;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.storage.relational.session.SqlSessions;

/**
 * Registers {@code policy-expand} tasks in {@link
 * TableMaintenanceSchedulerPools#EXPAND_TASKS_TABLE} through a db-scheduler client.
 *
 * <p>The instance id is the policy id. A blank {@code timeTrigger}, or a disabled policy, removes
 * the row. Handlers that fan the row out into cron jobs are out of scope here.
 *
 * <p>{@code timeTrigger} is a six-field Quartz cron. This class decides whether the expression has
 * a next fire time; policy content does not parse cron itself.
 *
 * <p>{@link #joiningCurrentTransaction()} writes through the open MyBatis session. The client must
 * not commit or close that connection, so the policy transaction and the expand row commit
 * together.
 */
public final class PolicyExpandRegistrar {

  private final DataSource dataSource;

  private PolicyExpandRegistrar(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  /**
   * Opens a client that writes on the current thread's open SQL session.
   *
   * <p>The db-scheduler client is created on each call, because building it borrows a connection.
   * That connection is the open session, so this registrar must be used inside a policy
   * transaction.
   *
   * @return registrar that inserts, moves, and deletes expand rows in the caller's transaction
   */
  public static PolicyExpandRegistrar joiningCurrentTransaction() {
    return new PolicyExpandRegistrar(new SessionDataSource());
  }

  /**
   * Opens a client that reads committed rows from the entity-store pool.
   *
   * @param dataSource relational JDBC pool shared with the entity store
   * @return registrar that can read expand rows after the policy transaction commits
   */
  public static PolicyExpandRegistrar forReads(DataSource dataSource) {
    return new PolicyExpandRegistrar(dataSource);
  }

  private SchedulerClient client() {
    return SchedulerClient.Builder.create(
            dataSource, TableMaintenanceSchedulerPools.policyExpandTask())
        .tableName(TableMaintenanceSchedulerPools.EXPAND_TASKS_TABLE)
        .build();
  }

  /**
   * Inserts or moves the expand row to the next cron fire time, or removes it when the policy
   * should not run.
   *
   * @param policyId persisted policy id, used as the db-scheduler task instance
   * @param enabled whether the policy is enabled
   * @param timeTrigger cron expression, or {@code null} when the policy has no time trigger
   */
  public void sync(long policyId, boolean enabled, @Nullable String timeTrigger) {
    TaskInstanceId instanceId = instanceId(policyId);
    if (!enabled || timeTrigger == null || timeTrigger.isBlank()) {
      cancelIfPresent(instanceId);
      return;
    }
    scheduleAt(policyId, nextExecution(timeTrigger));
  }

  /**
   * Inserts or moves the expand row to an explicit due time.
   *
   * @param policyId persisted policy id
   * @param due execution time to store
   */
  @VisibleForTesting
  void scheduleAt(long policyId, Instant due) {
    TaskInstanceId instanceId = instanceId(policyId);
    boolean scheduled =
        client()
            .schedule(
                new TaskInstance<>(
                    TableMaintenanceSchedulerPools.POLICY_EXPAND, instanceId.getId()),
                due,
                SchedulerClient.ScheduleOptions.WHEN_EXISTS_RESCHEDULE);
    if (!scheduled) {
      throw new IllegalStateException(
          "Failed to schedule policy-expand instance " + instanceId.getId() + " at " + due);
    }
  }

  /**
   * Deletes the expand row for a dropped policy.
   *
   * @param policyId persisted policy id
   */
  public void remove(long policyId) {
    cancelIfPresent(instanceId(policyId));
  }

  /**
   * @param policyId persisted policy id
   * @return next execution time of the expand row, or empty when the row is absent
   */
  Optional<Instant> scheduledExecutionTime(long policyId) {
    return client()
        .getScheduledExecution(instanceId(policyId))
        .map(execution -> execution.getExecutionTime());
  }

  /**
   * @return policy ids that currently have a {@code policy-expand} row
   */
  Set<Long> scheduledPolicyIds() {
    Set<Long> ids = new HashSet<>();
    client()
        .fetchScheduledExecutionsForTask(
            TableMaintenanceSchedulerPools.POLICY_EXPAND,
            Void.class,
            execution -> {
              try {
                ids.add(Long.parseLong(execution.getTaskInstance().getId()));
              } catch (NumberFormatException ignored) {
                // Non-numeric instance ids are not policy-expand leases from this registrar.
              }
            });
    return ids;
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
    SchedulerClient client = client();
    if (client.getScheduledExecution(instanceId).isPresent()) {
      client.cancel(instanceId);
    }
  }

  /**
   * Hands db-scheduler the connection of the open SQL session. {@code close}, {@code commit}, and
   * {@code rollback} are ignored so the session owner commits the expand row with the policy.
   */
  private static final class SessionDataSource implements DataSource {

    @Override
    public Connection getConnection() throws SQLException {
      return nonClosing(SqlSessions.currentConnection());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      if (iface.isInstance(this)) {
        return iface.cast(this);
      }
      throw new SQLException("Not a wrapper of " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return iface.isInstance(this);
    }

    private static Connection nonClosing(Connection connection) {
      InvocationHandler handler =
          (proxy, method, args) -> {
            String name = method.getName();
            if ("close".equals(name) || "commit".equals(name) || "rollback".equals(name)) {
              return null;
            }
            if ("setAutoCommit".equals(name)) {
              return null;
            }
            if ("getAutoCommit".equals(name)) {
              return false;
            }
            try {
              return method.invoke(connection, args);
            } catch (InvocationTargetException e) {
              throw e.getCause();
            }
          };
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }
  }
}
