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
package org.apache.gravitino.maintenance.scheduler.cron;

import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.expand.ExpandTableCandidate;
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
 * filling an incomplete snapshot-expiry batch (same due time, new table-id list).
 *
 * <p>The entity store disables JDBC autocommit; connections are wrapped so each client call
 * commits.
 */
public final class RelationalCronJobScheduler implements CronJobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(RelationalCronJobScheduler.class);

  private final DataSource dataSource;

  /**
   * @param dataSource relational JDBC pool shared with the entity store
   */
  public RelationalCronJobScheduler(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public int enqueueDueNow(
      Policy.BuiltInType policyType,
      long policyId,
      List<ExpandTableCandidate> candidates,
      int batchCapacity) {
    if (candidates == null || candidates.isEmpty()) {
      return 0;
    }
    // batchCapacity is tables-per-batch for snapshot-expiry; the expand pipeline already truncates
    // candidates to enqueueBatchSize.
    int capacity = Math.max(1, batchCapacity);

    SchedulerClient client = client(dataSource);
    Instant due = Instant.now();

    if (policyType == Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION) {
      // Pack all expand candidates; capacity is max table ids per batch:{n}:policy row.
      return enqueueSnapshotExpiry(client, policyId, candidates, capacity, due);
    }
    // One cron-job row per table; capacity is max new rows for this expand pick.
    int scheduled = 0;
    int limit = Math.min(candidates.size(), capacity);
    for (ExpandTableCandidate candidate : candidates.subList(0, limit)) {
      if (scheduleTable(client, policyId, candidate.tableId(), due)) {
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
            autocommitDataSource(dataSource), TableMaintenanceSchedulerPools.cronJobTask())
        .tableName(TableMaintenanceSchedulerPools.CRON_TASKS_TABLE)
        .build();
  }

  private static int enqueueSnapshotExpiry(
      SchedulerClient client,
      long policyId,
      List<ExpandTableCandidate> slice,
      int batchCapacity,
      Instant due) {
    List<Long> batchable = new ArrayList<>();
    int scheduled = 0;
    for (ExpandTableCandidate candidate : slice) {
      if (candidate.batchableForSnapshotExpiry()) {
        if (!isQueuedForPolicy(client, candidate.tableId(), policyId)) {
          batchable.add(candidate.tableId());
        }
      } else if (scheduleTable(client, policyId, candidate.tableId(), due)) {
        scheduled++;
      }
    }
    scheduled += packIntoBatches(client, policyId, batchable, batchCapacity, due);
    return scheduled;
  }

  /**
   * Fills incomplete open batches first (db-scheduler get + reschedule with new data), then
   * allocates {@code batch:{n}:{policyId}} for any remainder.
   *
   * @return number of newly inserted batch rows
   */
  private static int packIntoBatches(
      SchedulerClient client, long policyId, List<Long> tableIds, int batchCapacity, Instant due) {
    if (tableIds.isEmpty()) {
      return 0;
    }
    List<Long> remaining = new ArrayList<>(tableIds);
    int newRows = 0;
    while (!remaining.isEmpty()) {
      Optional<OpenBatch> incomplete = findIncompleteBatch(client, policyId, batchCapacity);
      if (incomplete.isPresent()) {
        OpenBatch open = incomplete.get();
        int room = batchCapacity - open.tableIds.size();
        List<Long> chunk = new ArrayList<>(remaining.subList(0, Math.min(room, remaining.size())));
        if (mergeIntoBatch(client, open, chunk)) {
          remaining.subList(0, chunk.size()).clear();
          continue;
        }
        // Row became picked / vanished — fall through to allocate a new batch id.
      }
      int end = Math.min(batchCapacity, remaining.size());
      List<Long> chunk = new ArrayList<>(remaining.subList(0, end));
      if (scheduleBatch(client, policyId, chunk, due)) {
        newRows++;
      }
      remaining.subList(0, end).clear();
    }
    return newRows;
  }

  private static boolean scheduleTable(
      SchedulerClient client, long policyId, long tableId, Instant due) {
    String id = CronJobIds.forTable(tableId, policyId);
    CronJobData data = new CronJobData(List.of(tableId));
    return client.scheduleIfNotExists(
        new TaskInstance<>(TableMaintenanceSchedulerPools.CRON_JOB, id, data), due);
  }

  private static boolean scheduleBatch(
      SchedulerClient client, long policyId, List<Long> tableIds, Instant due) {
    long batchId = nextBatchId(client, policyId);
    String id = CronJobIds.forBatch(batchId, policyId);
    return client.scheduleIfNotExists(
        new TaskInstance<>(TableMaintenanceSchedulerPools.CRON_JOB, id, new CronJobData(tableIds)),
        due);
  }

  private static boolean mergeIntoBatch(
      SchedulerClient client, OpenBatch open, List<Long> extraTableIds) {
    Set<Long> merged = new LinkedHashSet<>(open.tableIds);
    if (!merged.addAll(extraTableIds)) {
      return true;
    }
    try {
      // Conventional db-scheduler update: keep execution time, replace task_data.
      return client.reschedule(
          open.instanceId, open.executionTime, new CronJobData(new ArrayList<>(merged)));
    } catch (RuntimeException e) {
      LOG.debug(
          "Could not merge tables into cron-job {}: {}", open.instanceId.getId(), e.toString());
      return false;
    }
  }

  private static Optional<OpenBatch> findIncompleteBatch(
      SchedulerClient client, long policyId, int batchCapacity) {
    return listOpenBatches(client, policyId).stream()
        .filter(b -> !b.picked && b.tableIds.size() < batchCapacity)
        .min(Comparator.comparingLong(b -> b.batchId));
  }

  private static List<OpenBatch> listOpenBatches(SchedulerClient client, long policyId) {
    List<OpenBatch> open = new ArrayList<>();
    for (ScheduledExecution<CronJobData> execution :
        client.getScheduledExecutionsForTask(
            TableMaintenanceSchedulerPools.CRON_JOB, CronJobData.class)) {
      Optional<CronJobIds.BatchPolicyIds> parsed =
          CronJobIds.parseBatch(execution.getTaskInstance().getId());
      if (parsed.isEmpty() || parsed.get().policyId() != policyId) {
        continue;
      }
      CronJobData data = execution.getData();
      List<Long> tableIds = data == null ? List.of() : data.tableIds();
      open.add(
          new OpenBatch(
              TaskInstanceId.of(
                  TableMaintenanceSchedulerPools.CRON_JOB, execution.getTaskInstance().getId()),
              parsed.get().batchId(),
              execution.getExecutionTime(),
              tableIds,
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
      if (batch.tableIds.contains(tableId)) {
        return true;
      }
    }
    return false;
  }

  private static DataSource autocommitDataSource(DataSource delegate) {
    return new AutocommitDataSource(delegate);
  }

  private static final class OpenBatch {
    private final TaskInstanceId instanceId;
    private final long batchId;
    private final Instant executionTime;
    private final List<Long> tableIds;
    private final boolean picked;

    private OpenBatch(
        TaskInstanceId instanceId,
        long batchId,
        Instant executionTime,
        List<Long> tableIds,
        boolean picked) {
      this.instanceId = instanceId;
      this.batchId = batchId;
      this.executionTime = executionTime;
      this.tableIds = tableIds;
      this.picked = picked;
    }
  }

  /**
   * Returns connections with autocommit enabled so SchedulerClient persists each schedule without
   * an external transaction manager.
   */
  private static final class AutocommitDataSource implements DataSource {

    private final DataSource delegate;

    AutocommitDataSource(DataSource delegate) {
      this.delegate = delegate;
    }

    @Override
    public Connection getConnection() throws SQLException {
      return withAutocommit(delegate.getConnection());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return withAutocommit(delegate.getConnection(username, password));
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
      if (iface.isInstance(this)) {
        return iface.cast(this);
      }
      return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return iface.isInstance(this) || delegate.isWrapperFor(iface);
    }

    private static Connection withAutocommit(Connection connection) throws SQLException {
      Connection proxy =
          (Connection)
              Proxy.newProxyInstance(
                  Connection.class.getClassLoader(),
                  new Class<?>[] {Connection.class},
                  suppressingSetAutocommit(connection));
      connection.setAutoCommit(true);
      return proxy;
    }

    private static InvocationHandler suppressingSetAutocommit(Connection connection) {
      return (proxy, method, args) -> {
        if ("setAutoCommit".equals(method.getName())) {
          return null;
        }
        if ("getAutoCommit".equals(method.getName())) {
          return true;
        }
        try {
          return method.invoke(connection, args);
        } catch (InvocationTargetException e) {
          throw e.getCause();
        }
      };
    }
  }
}
