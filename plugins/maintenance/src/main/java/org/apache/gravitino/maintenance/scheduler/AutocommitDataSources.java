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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import javax.sql.DataSource;

/**
 * Wraps the entity-store {@link DataSource} so db-scheduler {@code SchedulerClient} calls commit
 * even when the pool disables JDBC autocommit.
 */
public final class AutocommitDataSources {

  private AutocommitDataSources() {}

  /**
   * @param delegate entity-store pool
   * @return proxy that forces {@code autoCommit=true} on borrowed connections
   */
  public static DataSource wrap(DataSource delegate) {
    return new AutocommitDataSource(delegate);
  }

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
