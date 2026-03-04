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
package org.apache.gravitino.stats.storage;

import com.google.common.base.Preconditions;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.utils.jdbc.JdbcDataSourceConfig;
import org.apache.gravitino.utils.jdbc.JdbcDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating {@link JdbcPartitionStatisticStorage} instances.
 *
 * <p>This factory creates a JDBC-based partition statistics storage using Apache Commons DBCP2 for
 * connection pooling. It supports multiple database backends (MySQL, PostgreSQL, H2) and configures
 * the connection pool with appropriate settings for partition statistics workloads.
 *
 * <p>Configuration properties:
 *
 * <ul>
 *   <li>jdbcUrl (required): JDBC connection URL (e.g., jdbc:mysql://host:port/db,
 *       jdbc:postgresql://host:port/db)
 *   <li>jdbcUser (required): Database username
 *   <li>jdbcPassword (required): Database password
 *   <li>jdbcDriver (optional): JDBC driver class name (defaults to com.mysql.cj.jdbc.Driver)
 *   <li>poolMaxSize (optional): Maximum connection pool size (default: 10)
 *   <li>poolMinIdle (optional): Minimum idle connections (default: 2)
 *   <li>connectionTimeoutMs (optional): Connection timeout in milliseconds (default: 30000)
 *   <li>testOnBorrow (optional): Test connections before use (default: true)
 * </ul>
 */
public class JdbcPartitionStatisticStorageFactory implements PartitionStatisticStorageFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(JdbcPartitionStatisticStorageFactory.class);

  // Configuration keys
  private static final String JDBC_URL = "jdbcUrl";
  private static final String JDBC_USER = "jdbcUser";
  private static final String JDBC_PASSWORD = "jdbcPassword";
  private static final String JDBC_DRIVER = "jdbcDriver";
  private static final String POOL_MAX_SIZE = "poolMaxSize";
  private static final String POOL_MIN_IDLE = "poolMinIdle";
  private static final String CONNECTION_TIMEOUT_MS = "connectionTimeoutMs";
  private static final String TEST_ON_BORROW = "testOnBorrow";

  // Default values
  private static final String DEFAULT_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

  // Keep reference to DataSource for cleanup
  private BasicDataSource dataSource;

  @Override
  public PartitionStatisticStorage create(Map<String, String> properties) {
    LOG.info(
        "Creating JdbcPartitionStatisticStorage with properties: {}",
        maskSensitiveProperties(properties));

    validateRequiredProperties(properties);

    try {
      dataSource = createDataSource(properties);
      return new JdbcPartitionStatisticStorage(dataSource);
    } catch (Exception e) {
      if (dataSource != null) {
        try {
          dataSource.close();
        } catch (SQLException closeException) {
          LOG.error("Failed to close data source after creation error", closeException);
        }
      }
      throw new GravitinoRuntimeException(e, "Failed to create JdbcPartitionStatisticStorage");
    }
  }

  /**
   * Creates and configures a BasicDataSource from the provided properties.
   *
   * @param properties configuration properties
   * @return configured DataSource
   */
  private BasicDataSource createDataSource(Map<String, String> properties) {
    String jdbcUrl = properties.get(JDBC_URL);
    String jdbcUser = properties.get(JDBC_USER);
    String jdbcPassword = properties.get(JDBC_PASSWORD);

    // Optional properties with defaults
    String driverClassName = properties.getOrDefault(JDBC_DRIVER, DEFAULT_JDBC_DRIVER);

    int maxSize =
        Integer.parseInt(
            properties.getOrDefault(
                POOL_MAX_SIZE, String.valueOf(JdbcDataSourceFactory.DEFAULT_MAX_TOTAL)));

    int minIdle =
        Integer.parseInt(
            properties.getOrDefault(
                POOL_MIN_IDLE, String.valueOf(JdbcDataSourceFactory.DEFAULT_MIN_IDLE)));

    long timeoutMs =
        Long.parseLong(
            properties.getOrDefault(
                CONNECTION_TIMEOUT_MS,
                String.valueOf(JdbcDataSourceFactory.DEFAULT_MAX_WAIT_MILLIS)));

    boolean testOnBorrow =
        Boolean.parseBoolean(
            properties.getOrDefault(
                TEST_ON_BORROW, String.valueOf(JdbcDataSourceFactory.DEFAULT_TEST_ON_BORROW)));

    JdbcDataSourceConfig config =
        new JdbcDataSourceConfig(
            jdbcUrl,
            jdbcUser,
            jdbcPassword,
            driverClassName,
            maxSize,
            minIdle,
            timeoutMs,
            testOnBorrow,
            JdbcDataSourceFactory.DEFAULT_VALIDATION_QUERY);

    LOG.info(
        "Created JDBC DataSource: url={}, driver={}, maxPoolSize={}, minIdle={}, timeout={}ms",
        jdbcUrl,
        driverClassName,
        maxSize,
        minIdle,
        timeoutMs);

    return JdbcDataSourceFactory.create(config);
  }

  /**
   * Validates that all required properties are present and non-empty.
   *
   * @param properties configuration properties
   * @throws IllegalArgumentException if required properties are missing or empty
   */
  private void validateRequiredProperties(Map<String, String> properties) {
    String jdbcUrl = properties.get(JDBC_URL);
    Preconditions.checkArgument(
        jdbcUrl != null && !jdbcUrl.trim().isEmpty(), "Property %s must be non-empty", JDBC_URL);

    String jdbcUser = properties.get(JDBC_USER);
    Preconditions.checkArgument(
        jdbcUser != null && !jdbcUser.trim().isEmpty(), "Property %s must be non-empty", JDBC_USER);

    String jdbcPassword = properties.get(JDBC_PASSWORD);
    Preconditions.checkArgument(jdbcPassword != null, "Property %s must be present", JDBC_PASSWORD);
  }

  /**
   * Creates a masked copy of properties for logging (hides password).
   *
   * @param properties original properties
   * @return masked properties map
   */
  private Map<String, String> maskSensitiveProperties(Map<String, String> properties) {
    Map<String, String> masked = new HashMap<>(properties);
    if (masked.containsKey(JDBC_PASSWORD)) {
      masked.put(JDBC_PASSWORD, "***");
    }
    return masked;
  }

  /**
   * Closes the data source if it was created by this factory.
   *
   * @throws SQLException if closing fails
   */
  public void close() throws SQLException {
    if (dataSource != null) {
      LOG.info("Closing JDBC DataSource");
      dataSource.close();
      dataSource = null;
    }
  }
}
