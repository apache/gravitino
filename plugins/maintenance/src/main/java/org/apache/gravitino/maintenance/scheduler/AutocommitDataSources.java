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

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;

/**
 * {@link DataSource} adapters for db-scheduler clients: force JDBC autocommit on pooled
 * connections, or reuse the open MyBatis session so expand rows commit with the policy.
 */
public final class AutocommitDataSources {

  private static final Set<String> JOIN_SESSION_NOOPS =
      Set.of("close", "commit", "rollback", "setAutoCommit");

  private AutocommitDataSources() {}

  /**
   * Resolves the relational entity-store JDBC pool from the open MyBatis environment.
   *
   * @return entity-store {@link DataSource}
   */
  public static DataSource entityStore() {
    Object dataSource =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getEnvironment()
            .getDataSource();
    if (!(dataSource instanceof DataSource)) {
      throw new IllegalStateException("Entity store data source is not available");
    }
    return (DataSource) dataSource;
  }

  /**
   * @param delegate entity-store pool
   * @return proxy that forces {@code autoCommit=true} on borrowed connections
   */
  public static DataSource wrap(DataSource delegate) {
    return new AutocommitDataSource(delegate);
  }

  /**
   * Hands db-scheduler the open MyBatis connection. {@code close}/{@code commit}/{@code rollback}
   * are ignored so the session owner commits with the caller.
   *
   * @return data source bound to {@link SqlSessions#currentConnection()}
   */
  public static DataSource joiningCurrentSession() {
    return new SimpleDataSource() {
      @Override
      public Connection getConnection() throws SQLException {
        return proxyConnection(SqlSessions.currentConnection(), /*autocommit=*/ false);
      }
    };
  }

  private static Connection proxyConnection(Connection connection, boolean autocommit) {
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              String name = method.getName();
              if (autocommit) {
                if ("setAutoCommit".equals(name)) {
                  return null;
                }
                if ("getAutoCommit".equals(name)) {
                  return true;
                }
              } else if (JOIN_SESSION_NOOPS.contains(name)) {
                return null;
              } else if ("getAutoCommit".equals(name)) {
                return false;
              }
              try {
                return method.invoke(connection, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }

  private static final class AutocommitDataSource extends SimpleDataSource {

    private final DataSource delegate;

    private AutocommitDataSource(DataSource delegate) {
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
      connection.setAutoCommit(true);
      return proxyConnection(connection, /*autocommit=*/ true);
    }
  }

  /** Minimal {@link DataSource}; subclasses override connection borrowing. */
  private abstract static class SimpleDataSource implements DataSource {

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {}

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {}

    @Override
    public int getLoginTimeout() throws SQLException {
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
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return iface.isInstance(this);
    }
  }
}
