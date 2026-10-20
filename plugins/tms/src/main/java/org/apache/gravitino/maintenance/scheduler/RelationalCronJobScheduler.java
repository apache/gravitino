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
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;

/**
 * Schedules {@code cron-job} rows through a db-scheduler client on the entity-store DataSource.
 *
 * <p>The entity store disables JDBC autocommit. SchedulerClient does not commit in that mode, so
 * connections are wrapped to enable autocommit for each schedule call.
 */
public final class RelationalCronJobScheduler implements CronJobScheduler {

  private final DataSource dataSource;

  /**
   * @param dataSource relational JDBC pool shared with the entity store
   */
  public RelationalCronJobScheduler(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public int enqueueDueNow(long policyId, List<Long> tableIds, int batchSize) {
    if (tableIds == null || tableIds.isEmpty()) {
      return 0;
    }
    SchedulerClient client =
        SchedulerClient.Builder.create(
                autocommitDataSource(dataSource), TableMaintenanceSchedulerPools.cronJobTask())
            .tableName(TableMaintenanceSchedulerPools.CRON_TASKS_TABLE)
            .build();
    Instant due = Instant.now();
    int limit = Math.min(tableIds.size(), Math.max(1, batchSize));
    int scheduled = 0;
    for (int i = 0; i < limit; i++) {
      String id = CronJobIds.forTable(tableIds.get(i), policyId);
      if (client.scheduleIfNotExists(
          new TaskInstance<>(TableMaintenanceSchedulerPools.CRON_JOB, id), due)) {
        scheduled++;
      }
    }
    return scheduled;
  }

  private static DataSource autocommitDataSource(DataSource delegate) {
    return new AutocommitDataSource(delegate);
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
