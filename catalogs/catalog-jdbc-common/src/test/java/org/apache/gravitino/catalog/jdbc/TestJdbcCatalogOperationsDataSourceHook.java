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
package org.apache.gravitino.catalog.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.SqliteDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.SqliteTableOperations;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.metrics.MetricsSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests {@link JdbcCatalogOperations#configureDataSource(DataSource, JdbcConfig)}. */
public class TestJdbcCatalogOperationsDataSourceHook {

  @Test
  void testHookRunsBeforeFirstBorrowAndItsDriverIsUsed() throws Exception {
    AtomicInteger connects = new AtomicInteger();
    AtomicReference<BasicDataSource> configured = new AtomicReference<>();
    try (JdbcCatalogOperations operations =
        new HookedOperations(
            (dataSource, config) -> {
              BasicDataSource pool = (BasicDataSource) dataSource;
              // Nothing has been borrowed yet, so the driver can still be replaced.
              Assertions.assertEquals(0, pool.getNumActive() + pool.getNumIdle());
              pool.setDriver(
                  new CountingDriver(DataSourceUtils.configuredDriver(config), connects));
              configured.set(pool);
            })) {
      operations.initialize(properties(), catalogInfo(), new TestJdbcCatalog());
      Assertions.assertNotNull(configured.get());
      try (Connection connection = configured.get().getConnection()) {
        Assertions.assertTrue(connection.isValid(1));
      }
      Assertions.assertTrue(connects.get() > 0, "The installed driver must open connections");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testHookFailureClosesDataSourceAndPropagates(boolean metricsEnabled) throws Exception {
    IllegalStateException failure = new IllegalStateException("cannot configure");
    AtomicReference<BasicDataSource> configured = new AtomicReference<>();
    GravitinoEnv environment = GravitinoEnv.getInstance();
    MetricsSystem previousMetrics =
        (MetricsSystem) FieldUtils.readField(environment, "metricsSystem", true);
    try (MetricsSystem metricsSystem = metricsEnabled ? new MetricsSystem() : null;
        JdbcCatalogOperations operations =
            new HookedOperations(
                (dataSource, config) -> {
                  configured.set((BasicDataSource) dataSource);
                  throw failure;
                })) {
      FieldUtils.writeField(environment, "metricsSystem", metricsSystem, true);
      IllegalStateException thrown =
          Assertions.assertThrows(
              IllegalStateException.class,
              () -> operations.initialize(properties(), catalogInfo(), new TestJdbcCatalog()));
      Assertions.assertSame(failure, thrown);
      Assertions.assertTrue(configured.get().isClosed());
    } finally {
      FieldUtils.writeField(environment, "metricsSystem", previousMetrics, true);
    }
  }

  @Test
  void testDefaultHookKeepsTheConfiguredDriver() throws Exception {
    AtomicReference<BasicDataSource> created = new AtomicReference<>();
    try (JdbcCatalogOperations operations =
        new JdbcCatalogOperations(
            new SqliteExceptionConverter(),
            new SqliteTypeConverter(),
            new SqliteDatabaseOperations(":memory:"),
            new SqliteTableOperations(),
            new SqliteColumnDefaultValueConverter()) {
          @Override
          protected void configureDataSource(DataSource dataSource, JdbcConfig config) {
            created.set((BasicDataSource) dataSource);
            super.configureDataSource(dataSource, config);
          }
        }) {
      operations.initialize(properties(), catalogInfo(), new TestJdbcCatalog());
      Assertions.assertNull(created.get().getDriver());
    }
  }

  private interface Hook {
    void configure(DataSource dataSource, JdbcConfig config);
  }

  private static final class HookedOperations extends JdbcCatalogOperations {
    private final Hook hook;

    private HookedOperations(Hook hook) {
      super(
          new SqliteExceptionConverter(),
          new SqliteTypeConverter(),
          new SqliteDatabaseOperations(":memory:"),
          new SqliteTableOperations(),
          new SqliteColumnDefaultValueConverter());
      this.hook = hook;
    }

    @Override
    protected void configureDataSource(DataSource dataSource, JdbcConfig config) {
      hook.configure(dataSource, config);
    }

    /** Borrows a connection like connectors that read the driver version do. */
    @Override
    public void checkJDBCDriverVersion() {
      try (Connection connection =
          ((DataSource) FieldUtils.readField(this, "dataSource", true)).getConnection()) {
        Assertions.assertTrue(connection.isValid(1));
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private static final class CountingDriver implements Driver {
    private final Driver delegate;
    private final AtomicInteger connects;

    private CountingDriver(Driver delegate, AtomicInteger connects) {
      this.delegate = delegate;
      this.connects = connects;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
      connects.incrementAndGet();
      return delegate.connect(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
      return delegate.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
      return delegate.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
      return delegate.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
      return delegate.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
      return delegate.jdbcCompliant();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return delegate.getParentLogger();
    }
  }

  private static Map<String, String> properties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.USERNAME.getKey(), "user");
    properties.put(JdbcConfig.PASSWORD.getKey(), "password");
    return properties;
  }

  private static CatalogInfo catalogInfo() {
    return new CatalogInfo(
        1L,
        "jdbc_catalog",
        Catalog.Type.RELATIONAL,
        "jdbc-test",
        "test",
        properties(),
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build(),
        Namespace.of("metalake"));
  }

  private static class TestJdbcCatalog extends JdbcCatalog {
    @Override
    public String shortName() {
      return "jdbc-test";
    }

    @Override
    protected JdbcTypeConverter createJdbcTypeConverter() {
      return new SqliteTypeConverter();
    }

    @Override
    protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
      return new SqliteDatabaseOperations(":memory:");
    }

    @Override
    protected JdbcTableOperations createJdbcTableOperations() {
      return new SqliteTableOperations();
    }

    @Override
    protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
      return new SqliteColumnDefaultValueConverter();
    }
  }
}
