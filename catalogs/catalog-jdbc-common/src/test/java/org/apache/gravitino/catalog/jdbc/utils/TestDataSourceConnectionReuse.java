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
package org.apache.gravitino.catalog.jdbc.utils;

import com.google.common.collect.Maps;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test for the catalog datasource opening a new physical connection on every borrow when
 * the JDBC URL has no default database.
 *
 * <p>{@link BindingDriver} reproduces the MySQL Connector/J behavior that triggers the defect: a
 * statement belongs to the database that was current when the statement was created, so executing
 * it after the connection switched to another database fails with SQLState 3D000 / error 1046 ("No
 * database selected"), while {@code Connection.isValid(int)} is evaluated against the current
 * catalog. DBCP2 caches the validation statement on the physical connection, so configuring a
 * validation query made every validation after a catalog switch fail: DBCP2 destroyed the
 * connection and opened a new one on every borrow.
 */
public class TestDataSourceConnectionReuse {

  private static final String CATALOG_SWITCHED_TO = "perf_tables";
  private static final int BORROW_ITERATIONS = 20;

  @BeforeEach
  public void resetDriver() {
    BindingDriver.reset();
  }

  @Test
  public void testConnectionIsReusedAfterCatalogSwitch() throws SQLException {
    DataSource dataSource =
        Assertions.assertDoesNotThrow(() -> DataSourceUtils.createDataSource(catalogProperties()));
    try {
      // DBCP2 verifies the pool configuration by creating, validating and destroying one connection
      // when the pool initializes, so the pool is warmed up before the reuse is measured.
      try (Connection warmUp = dataSource.getConnection()) {
        warmUp.setCatalog(CATALOG_SWITCHED_TO);
      }
      BindingDriver.reset();

      for (int i = 0; i < BORROW_ITERATIONS; i++) {
        try (Connection connection = dataSource.getConnection()) {
          connection.setCatalog(CATALOG_SWITCHED_TO);
          try (Statement statement = connection.createStatement();
              ResultSet resultSet = statement.executeQuery("SELECT 1")) {
            Assertions.assertTrue(resultSet.next());
          }
        } catch (SQLException e) {
          throw new AssertionError("borrow " + i + " failed", e);
        }
      }

      Assertions.assertEquals(
          0,
          BindingDriver.connections(),
          "switching the catalog must not make the pool open another physical connection");
      Assertions.assertEquals(
          0,
          BindingDriver.bindingFailures(),
          "no statement may be executed for a database other than the one it was created for");
      Assertions.assertTrue(
          BindingDriver.validations() >= BORROW_ITERATIONS,
          "each borrow must validate the connection, but only "
              + BindingDriver.validations()
              + " validations were recorded");
    } finally {
      DataSourceUtils.closeDataSource(dataSource);
    }
  }

  private static Map<String, String> catalogProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), BindingDriver.class.getName());
    properties.put(JdbcConfig.JDBC_URL.getKey(), BindingDriver.URL);
    properties.put(JdbcConfig.USERNAME.getKey(), "user");
    properties.put(JdbcConfig.PASSWORD.getKey(), "password");
    return properties;
  }

  /**
   * A JDBC driver that mimics MySQL Connector/J's statement-to-database binding: a statement is
   * bound to the catalog that was current when it was created and every execution against a
   * connection that switched catalogs fails with SQLState 3D000 / error 1046 ("No database
   * selected").
   */
  public static class BindingDriver implements Driver {

    static final String URL = "jdbc:gravitino-connection-binding:test";

    private static final AtomicInteger CONNECTIONS = new AtomicInteger();
    private static final AtomicInteger VALIDATIONS = new AtomicInteger();
    private static final AtomicInteger BINDING_FAILURES = new AtomicInteger();

    static void reset() {
      CONNECTIONS.set(0);
      VALIDATIONS.set(0);
      BINDING_FAILURES.set(0);
    }

    static int connections() {
      return CONNECTIONS.get();
    }

    static int validations() {
      return VALIDATIONS.get();
    }

    static int bindingFailures() {
      return BINDING_FAILURES.get();
    }

    @Override
    public Connection connect(String url, Properties info) {
      if (!acceptsURL(url)) {
        return null;
      }
      CONNECTIONS.incrementAndGet();
      return (Connection)
          Proxy.newProxyInstance(
              BindingDriver.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              new BindingConnection());
    }

    @Override
    public boolean acceptsURL(String url) {
      return URL.equals(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
      return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
      return 1;
    }

    @Override
    public int getMinorVersion() {
      return 0;
    }

    @Override
    public boolean jdbcCompliant() {
      return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }
  }

  /** Connection proxy tracking the catalog and handing out catalog-bound statements. */
  private static class BindingConnection implements InvocationHandler {

    private String catalog = "";
    private boolean closed;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      switch (method.getName()) {
        case "getCatalog":
          return catalog;
        case "setCatalog":
          catalog = (String) args[0];
          return null;
        case "isValid":
          BindingDriver.VALIDATIONS.incrementAndGet();
          return !closed;
        case "createStatement":
          return newStatement(Statement.class);
        case "prepareStatement":
          return newStatement(PreparedStatement.class);
        case "isClosed":
          return closed;
        case "close":
          closed = true;
          return null;
        case "getAutoCommit":
          return true;
        case "isReadOnly":
          return false;
        case "getTransactionIsolation":
          return Connection.TRANSACTION_READ_UNCOMMITTED;
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == args[0];
        case "toString":
          return "BindingConnection";
        default:
          return defaultValueOf(method.getReturnType());
      }
    }

    private Object newStatement(Class<?> type) {
      return Proxy.newProxyInstance(
          BindingDriver.class.getClassLoader(),
          new Class<?>[] {type},
          new BindingStatement(this, catalog));
    }
  }

  /** Statement proxy bound to the catalog that was current when the statement was created. */
  private static class BindingStatement implements InvocationHandler {

    private final BindingConnection connection;
    private final String boundCatalog;

    private BindingStatement(BindingConnection connection, String boundCatalog) {
      this.connection = connection;
      this.boundCatalog = boundCatalog;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      switch (method.getName()) {
        case "executeQuery":
          verifyBoundCatalog();
          return resultSet();
        case "execute":
          verifyBoundCatalog();
          return true;
        case "close":
          return null;
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == args[0];
        case "toString":
          return "BindingStatement";
        default:
          return defaultValueOf(method.getReturnType());
      }
    }

    private void verifyBoundCatalog() throws SQLException {
      if (!boundCatalog.equals(connection.catalog)) {
        BindingDriver.BINDING_FAILURES.incrementAndGet();
        throw new SQLException("No database selected", "3D000", 1046);
      }
    }

    private Object resultSet() {
      return Proxy.newProxyInstance(
          BindingDriver.class.getClassLoader(),
          new Class<?>[] {ResultSet.class},
          (proxy, method, args) -> {
            switch (method.getName()) {
              case "next":
                return true;
              case "hashCode":
                return System.identityHashCode(proxy);
              case "equals":
                return proxy == args[0];
              case "toString":
                return "BindingResultSet";
              default:
                return defaultValueOf(method.getReturnType());
            }
          });
    }
  }

  private static Object defaultValueOf(Class<?> returnType) {
    if (!returnType.isPrimitive() || returnType == void.class) {
      return null;
    }
    if (returnType == boolean.class) {
      return false;
    }
    if (returnType == int.class) {
      return 0;
    }
    if (returnType == long.class) {
      return 0L;
    }
    if (returnType == double.class) {
      return 0d;
    }
    if (returnType == float.class) {
      return 0f;
    }
    if (returnType == short.class) {
      return (short) 0;
    }
    if (returnType == byte.class) {
      return (byte) 0;
    }
    return (char) 0;
  }
}
