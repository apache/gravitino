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

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating {@link MysqlPartitionStatisticStorage} instances.
 *
 * <p>This factory creates a MySQL-based partition statistics storage using Apache Commons DBCP2 for
 * connection pooling. It configures the connection pool with appropriate settings for partition
 * statistics workloads.
 *
 * <p>Configuration properties:
 *
 * <ul>
 *   <li>jdbc-url (required): JDBC connection URL for MySQL
 *   <li>jdbc-user (required): Database username
 *   <li>jdbc-password (required): Database password
 *   <li>jdbc-driver (optional): JDBC driver class name (defaults to com.mysql.cj.jdbc.Driver)
 *   <li>pool-max-size (optional): Maximum connection pool size (default: 10)
 *   <li>pool-min-idle (optional): Minimum idle connections (default: 2)
 *   <li>connection-timeout-ms (optional): Connection timeout in milliseconds (default: 30000)
 *   <li>test-on-borrow (optional): Test connections before use (default: true)
 * </ul>
 */
public class MysqlPartitionStatisticStorageFactory implements PartitionStatisticStorageFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(MysqlPartitionStatisticStorageFactory.class);

  // Configuration keys
  private static final String JDBC_URL = "jdbc-url";
  private static final String JDBC_USER = "jdbc-user";
  private static final String JDBC_PASSWORD = "jdbc-password";
  private static final String JDBC_DRIVER = "jdbc-driver";
  private static final String POOL_MAX_SIZE = "pool-max-size";
  private static final String POOL_MIN_IDLE = "pool-min-idle";
  private static final String CONNECTION_TIMEOUT_MS = "connection-timeout-ms";
  private static final String TEST_ON_BORROW = "test-on-borrow";

  // Default values
  private static final String DEFAULT_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
  private static final int DEFAULT_POOL_MAX_SIZE = 10;
  private static final int DEFAULT_POOL_MIN_IDLE = 2;
  private static final long DEFAULT_CONNECTION_TIMEOUT_MS = 30000L;
  private static final boolean DEFAULT_TEST_ON_BORROW = true;
  private static final String VALIDATION_QUERY = "SELECT 1";

  // Keep reference to DataSource for cleanup
  private BasicDataSource dataSource;

  @Override
  public PartitionStatisticStorage create(Map<String, String> properties) {
    LOG.info(
        "Creating MysqlPartitionStatisticStorage with properties: {}",
        maskSensitiveProperties(properties));

    validateRequiredProperties(properties);

    try {
      dataSource = createDataSource(properties);
      return new MysqlPartitionStatisticStorage(dataSource);
    } catch (Exception e) {
      if (dataSource != null) {
        try {
          dataSource.close();
        } catch (SQLException closeException) {
          LOG.error("Failed to close data source after creation error", closeException);
        }
      }
      throw new GravitinoRuntimeException(e, "Failed to create MysqlPartitionStatisticStorage");
    }
  }

  /**
   * Creates and configures a BasicDataSource from the provided properties.
   *
   * @param properties configuration properties
   * @return configured DataSource
   */
  private BasicDataSource createDataSource(Map<String, String> properties) {
    BasicDataSource ds = new BasicDataSource();

    // Required properties
    String jdbcUrl = properties.get(JDBC_URL);
    String jdbcUser = properties.get(JDBC_USER);
    String jdbcPassword = properties.get(JDBC_PASSWORD);

    ds.setUrl(jdbcUrl);
    ds.setUsername(jdbcUser);
    ds.setPassword(jdbcPassword);

    // Optional properties with defaults
    String driverClassName = properties.getOrDefault(JDBC_DRIVER, DEFAULT_JDBC_DRIVER);
    ds.setDriverClassName(driverClassName);

    int maxSize =
        Integer.parseInt(
            properties.getOrDefault(POOL_MAX_SIZE, String.valueOf(DEFAULT_POOL_MAX_SIZE)));
    ds.setMaxTotal(maxSize);

    int minIdle =
        Integer.parseInt(
            properties.getOrDefault(POOL_MIN_IDLE, String.valueOf(DEFAULT_POOL_MIN_IDLE)));
    ds.setMinIdle(minIdle);

    long timeoutMs =
        Long.parseLong(
            properties.getOrDefault(
                CONNECTION_TIMEOUT_MS, String.valueOf(DEFAULT_CONNECTION_TIMEOUT_MS)));
    ds.setMaxWait(Duration.ofMillis(timeoutMs));

    boolean testOnBorrow =
        Boolean.parseBoolean(
            properties.getOrDefault(TEST_ON_BORROW, String.valueOf(DEFAULT_TEST_ON_BORROW)));
    ds.setTestOnBorrow(testOnBorrow);

    if (testOnBorrow) {
      ds.setValidationQuery(VALIDATION_QUERY);
    }

    LOG.info(
        "Created MySQL DataSource: url={}, driver={}, maxPoolSize={}, minIdle={}, timeout={}ms",
        jdbcUrl,
        driverClassName,
        maxSize,
        minIdle,
        timeoutMs);

    return ds;
  }

  /**
   * Validates that all required properties are present.
   *
   * @param properties configuration properties
   * @throws IllegalArgumentException if required properties are missing
   */
  private void validateRequiredProperties(Map<String, String> properties) {
    if (!properties.containsKey(JDBC_URL)) {
      throw new IllegalArgumentException("Missing required property: " + JDBC_URL);
    }
    if (!properties.containsKey(JDBC_USER)) {
      throw new IllegalArgumentException("Missing required property: " + JDBC_USER);
    }
    if (!properties.containsKey(JDBC_PASSWORD)) {
      throw new IllegalArgumentException("Missing required property: " + JDBC_PASSWORD);
    }
  }

  /**
   * Creates a masked copy of properties for logging (hides password).
   *
   * @param properties original properties
   * @return masked properties map
   */
  private Map<String, String> maskSensitiveProperties(Map<String, String> properties) {
    Map<String, String> masked = new java.util.HashMap<>(properties);
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
      LOG.info("Closing MySQL DataSource");
      dataSource.close();
      dataSource = null;
    }
  }
}
