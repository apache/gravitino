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

import java.sql.Driver;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.utils.JdbcUrlUtils;

/**
 * Utility class for creating a {@link DataSource} from a {@link JdbcConfig}. It is mainly
 * responsible for creating connection pool management of data sources and configuring some
 * connection pools. The apache-dbcp2 connection pool is used here.
 */
public class DataSourceUtils {

  // DBCP2 connection-pool properties that must never come from catalog configuration. The whole
  // config map is handed to BasicDataSourceFactory, so allowing these would either run arbitrary
  // code or let a raw property override the validated canonical connection fields:
  //   - connectionFactoryClassName / evictionPolicyClassName / driverClassName: their values are
  //     class names the factory loads and instantiates via reflection (Class.forName + newInstance)
  //     when the pool creates connections (remote code execution). The legitimate driver is set
  //     separately from the "jdbc-driver" property via an explicit setter, so the raw
  //     "driverClassName" key is never needed.
  //   - url / username / password: the connection identity is validated (JdbcUrlUtils, the H2
  //     guard) and applied from the canonical "jdbc-url"/"jdbc-user"/"jdbc-password" properties via
  //     explicit setters. A raw DBCP key here would reach the factory unvalidated and, once the
  //     pool initializes, take effect before those setters run — e.g. a raw "url" could smuggle an
  //     H2 or otherwise-denied URL past the guards.
  //   - initialSize: defense in depth. A value > 0 makes the factory eagerly open a connection
  //     during createDataSource; blocking it keeps pool creation lazy so our explicit url/driver
  //     setters, not factory-time eager init, control how connections are created.
  private static final List<String> UNSAFE_POOL_PROPERTIES =
      List.of(
          "connectionFactoryClassName",
          "evictionPolicyClassName",
          "driverClassName",
          "url",
          "username",
          "password",
          "initialSize");

  public static DataSource createDataSource(Map<String, String> properties) {
    return createDataSource(new JdbcConfig(properties));
  }

  public static DataSource createDataSource(JdbcConfig jdbcConfig)
      throws GravitinoRuntimeException {
    validateConfiguration(jdbcConfig);
    try {
      return createDBCPDataSource(jdbcConfig);
    } catch (Exception exception) {
      throw new GravitinoRuntimeException(exception, "Error creating datasource");
    }
  }

  /**
   * Validates JDBC settings without opening a connection or retaining a pool.
   *
   * @param jdbcConfig the effective JDBC configuration
   * @throws IllegalArgumentException if a boolean, pool bound, driver or URL is invalid
   * @throws GravitinoRuntimeException if a dangerous URL or driver setting is rejected
   */
  public static void validateConfiguration(JdbcConfig jdbcConfig) {
    // H2 is bundled as an embedded backend and must not be used through user-facing catalog
    // configuration. Its INIT parameter allows arbitrary SQL (and Java code via CREATE ALIAS)
    // to execute at connection time, and the H2 driver class must also be blocked to prevent
    // bypassing this check via a mismatched driver and URL combination.
    String lowerUrl = jdbcConfig.getJdbcUrl().toLowerCase(Locale.ROOT);
    boolean isH2Url =
        JdbcUrlUtils.decodedFormsForScan(lowerUrl).stream()
            .anyMatch(form -> form.startsWith("jdbc:h2"));
    if (isH2Url) {
      throw new GravitinoRuntimeException("H2 JDBC URL is not allowed in catalog configuration");
    }
    if (jdbcConfig.getJdbcDriver().toLowerCase(Locale.ROOT).startsWith("org.h2.")) {
      throw new GravitinoRuntimeException("H2 JDBC driver is not allowed in catalog configuration");
    }
    // Reject DBCP2 pool properties that load arbitrary classes via reflection before handing the
    // config to the factory. Kept outside the try below so the specific reason surfaces directly
    // instead of being wrapped as "Error creating datasource".
    rejectUnsafePoolProperties(jdbcConfig.getAllConfig());
    // Validate user configuration before the data-source builder wraps infrastructure failures.
    jdbcConfig.getVerifyOnCreate();
    jdbcConfig.getTestOnBorrow();
    jdbcConfig.getConnectTimeoutMs();
    jdbcConfig.getMaxWaitMs();
    jdbcConfig.getPoolMaxIdle();
    int minSize = jdbcConfig.getPoolMinSize();
    int maxSize = jdbcConfig.getPoolMaxSize();
    if (minSize > maxSize) {
      throw new IllegalArgumentException("jdbc.pool.min-size must not exceed jdbc.pool.max-size");
    }
    try {
      JdbcUrlUtils.validateJdbcConfig(
          jdbcConfig.getJdbcDriver(), jdbcConfig.getJdbcUrl(), jdbcConfig.getAllConfig());
    } catch (RuntimeException e) {
      throw new GravitinoRuntimeException(e, "Error creating datasource");
    }
    configuredDriver(jdbcConfig);
    Properties properties = getProperties(jdbcConfig);
    try {
      validateNativePoolProperties(properties);
    } catch (Exception invalidProperties) {
      // Identify a malformed native alias without printing unrelated settings or credentials.
      for (String key : properties.stringPropertyNames()) {
        Properties singleProperty = new Properties();
        singleProperty.setProperty(key, properties.getProperty(key));
        try {
          validateNativePoolProperties(singleProperty);
        } catch (Exception invalidProperty) {
          throw new IllegalArgumentException(
              "Invalid native JDBC connection-pool property '"
                  + key
                  + "': "
                  + invalidProperty.getMessage(),
              invalidProperty);
        }
      }
      throw new IllegalArgumentException(
          "Invalid native JDBC connection-pool properties; check gravitino.bypass.* settings",
          invalidProperties);
    }
  }

  private static void validateNativePoolProperties(Properties properties) throws Exception {
    // initialSize and class-loading properties were rejected before this method. The factory
    // therefore only parses an uninitialized configuration bean; no connections are opened.
    try (BasicDataSource ignored = BasicDataSourceFactory.createDataSource(properties)) {
      // Always close the temporary bean, including on the diagnostic path for individual keys.
    }
  }

  /**
   * Creates the configured driver and verifies that it accepts the configured JDBC URL.
   *
   * @param config the JDBC catalog configuration
   * @return the configured JDBC driver
   * @throws IllegalArgumentException if the driver is absent, invalid, or incompatible with the URL
   */
  public static Driver configuredDriver(JdbcConfig config) {
    String name = config.getJdbcDriver();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    if (loader == null) {
      loader = DataSourceUtils.class.getClassLoader();
    }
    try {
      Class<?> type = Class.forName(name, false, loader);
      if (!Driver.class.isAssignableFrom(type)) {
        throw new IllegalArgumentException(
            "jdbc-driver '" + name + "' must implement java.sql.Driver");
      }
      Driver driver = type.asSubclass(Driver.class).getDeclaredConstructor().newInstance();
      if (!driver.acceptsURL(config.getJdbcUrl())) {
        throw new IllegalArgumentException("jdbc-driver '" + name + "' does not accept jdbc-url");
      }
      return driver;
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "JDBC driver class '"
              + name
              + "' was not found on the catalog classpath. Install the driver JAR in the catalog's libs directory.",
          e);
    } catch (ReflectiveOperationException | SQLException | LinkageError e) {
      throw new IllegalArgumentException(
          "Invalid jdbc-driver '" + name + "': driver must be installed and usable", e);
    }
  }

  private static DataSource createDBCPDataSource(JdbcConfig jdbcConfig) throws Exception {
    // Keep DBCP's default Connection.isValid() validation unless a validationQuery is explicitly
    // configured. DBCP caches a validation query as a prepared statement, and MySQL Connector/J
    // switches back to the database captured at prepare time when executing it. That switch fails
    // when the captured database is empty, so healthy connections would be discarded.
    BasicDataSource basicDataSource =
        BasicDataSourceFactory.createDataSource(getProperties(jdbcConfig));
    String jdbcUrl = jdbcConfig.getJdbcUrl();
    basicDataSource.setUrl(jdbcUrl);
    String driverClassName = jdbcConfig.getJdbcDriver();
    basicDataSource.setDriverClassName(driverClassName);
    String userName = jdbcConfig.getUsername();
    basicDataSource.setUsername(userName);
    String password = jdbcConfig.getPassword();
    basicDataSource.setPassword(password);
    basicDataSource.setMaxTotal(jdbcConfig.getPoolMaxSize());
    // Preserve maxIdle supplied through the existing DBCP bypass unless the canonical setting is
    // explicitly configured. The canonical default applies to catalogs without either setting.
    if (jdbcConfig.getAllConfig().containsKey(JdbcConfig.POOL_MAX_IDLE.getKey())
        || !jdbcConfig.getAllConfig().containsKey("maxIdle")) {
      basicDataSource.setMaxIdle(jdbcConfig.getPoolMaxIdle());
    }
    int maxIdle = basicDataSource.getMaxIdle();
    basicDataSource.setMinIdle(
        maxIdle >= 0
            ? Math.min(jdbcConfig.getPoolMinSize(), maxIdle)
            : jdbcConfig.getPoolMinSize());
    // Validate connections on borrow when enabled.
    basicDataSource.setTestOnBorrow(jdbcConfig.getTestOnBorrow());
    // Same precedence as maxIdle: keep a maxWaitMillis supplied through the DBCP bypass unless the
    // canonical setting is explicitly configured.
    if (jdbcConfig.getAllConfig().containsKey(JdbcConfig.POOL_MAX_WAIT_MS.getKey())
        || !jdbcConfig.getAllConfig().containsKey("maxWaitMillis")) {
      basicDataSource.setMaxWait(Duration.ofMillis(jdbcConfig.getMaxWaitMs()));
    }
    // Apply the driver-aware connect/socket timeout as DBCP connection properties (not pool keys,
    // so no collision with UNSAFE_POOL_PROPERTIES and the pool stays lazy). These bound a single
    // connection and take effect on the first real connection — i.e. create-time verification — so
    // an unreachable backend fails fast instead of hanging. Operator-set values are preserved.
    JdbcConnectionTimeouts.connectionProperties(jdbcConfig, jdbcUrl)
        .forEach(basicDataSource::addConnectionProperty);
    return basicDataSource;
  }

  private static Properties getProperties(JdbcConfig jdbcConfig) {
    Properties properties = new Properties();
    properties.putAll(jdbcConfig.getAllConfig());
    return properties;
  }

  /**
   * Rejects DBCP2 connection-pool properties that would let catalog configuration run arbitrary
   * classes on the server ({@code connectionFactoryClassName}, {@code evictionPolicyClassName},
   * {@code driverClassName}) or override the validated canonical connection identity ({@code url},
   * {@code username}, {@code password}), plus {@code initialSize} (blocked as defense in depth to
   * keep pool creation lazy).
   *
   * @param config the JDBC configuration properties forwarded to the DBCP2 factory
   * @throws GravitinoRuntimeException if an unsafe connection-pool property is present
   */
  private static void rejectUnsafePoolProperties(Map<String, String> config) {
    if (config == null) {
      return;
    }
    for (String key : config.keySet()) {
      for (String unsafe : UNSAFE_POOL_PROPERTIES) {
        if (unsafe.equalsIgnoreCase(key)) {
          throw new GravitinoRuntimeException(
              "Unsafe JDBC connection pool property '%s' is not allowed in catalog configuration",
              key);
        }
      }
    }
  }

  public static void closeDataSource(DataSource dataSource) {
    if (null != dataSource) {
      try {
        if (dataSource instanceof BasicDataSource) {
          ((BasicDataSource) dataSource).close();
        } else {
          throw new UnsupportedOperationException(
              "close operation can only be called in BasicDataSource.");
        }
      } catch (SQLException ignore) {
        // no op
      }
    }
  }

  private DataSourceUtils() {}
}
