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
package org.apache.gravitino.integration.test.container;

import static java.lang.String.format;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

public class PostgreSQLContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(PostgreSQLContainer.class);
  public static final PGImageName DEFAULT_IMAGE = PGImageName.VERSION_13;
  public static final String HOST_NAME = "gravitino-ci-pg";
  public static final int PG_PORT = 5432;
  public static final String USER_NAME = "root";
  public static final String PASSWORD = "root";

  public static Builder builder() {
    return new Builder();
  }

  protected PostgreSQLContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network);
  }

  @Override
  protected void setupContainer() {
    super.setupContainer();
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "PostgreSQLContainer")));
  }

  @Override
  public void start() {
    super.start();
    Preconditions.check("PostgreSQL container startup failed!", checkContainerStatus(30));
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    int nRetry = 0;
    boolean isPostgreSQLContainerReady = false;
    int sleepTimeMillis = 20_00;
    while (nRetry++ < retryLimit) {
      try {
        String[] commandAndArgs =
            new String[] {
              "pg_isready", "-h", "localhost", "-U", getUsername(),
            };
        Container.ExecResult execResult = executeInContainer(commandAndArgs);
        if (execResult.getExitCode() != 0) {
          String message =
              format(
                  "Command [%s] exited with %s",
                  String.join(" ", commandAndArgs), execResult.getExitCode());
          LOG.error("{}", message);
          LOG.error("stderr: {}", execResult.getStderr());
          LOG.error("stdout: {}", execResult.getStdout());
        } else {
          LOG.info("PostgreSQL container startup success!");
          isPostgreSQLContainerReady = true;
          break;
        }
        LOG.info(
            "PostgreSQL container is not ready, recheck({}/{}) after {}ms",
            nRetry,
            retryLimit,
            sleepTimeMillis);
        await().atLeast(sleepTimeMillis, TimeUnit.MILLISECONDS);
      } catch (RuntimeException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    return isPostgreSQLContainerReady;
  }

  public void createDatabase(TestDatabaseName testDatabaseName) {
    // Retry connection with short backoff as fallback (container should already be ready)
    int maxRetries = 3;
    long retryDelayMs = 1000;

    for (int attempt = 0; attempt < maxRetries; attempt++) {
      try (Connection connection =
              DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
          Statement statement = connection.createStatement()) {

        String query = format("CREATE DATABASE \"%s\"", testDatabaseName);
        statement.execute(query);
        LOG.info(format("PostgreSQL container database %s has been created", testDatabaseName));
        return;
      } catch (SQLException e) {
        if (e.getMessage()
            .equals(String.format("ERROR: database \"%s\" already exists", testDatabaseName))) {
          LOG.info("PostgreSQL Database {} already exists, skipping", testDatabaseName);
          return;
        }

        // Retry only on connection errors with short delay
        if (attempt < maxRetries - 1 && isRetriableException(e)) {
          LOG.warn(
              "Failed to connect to PostgreSQL (attempt {}/{}), retrying in {}ms: {}",
              attempt + 1,
              maxRetries,
              retryDelayMs,
              e.getMessage());
          try {
            Thread.sleep(retryDelayMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting to retry database creation", ie);
          }
        } else {
          throw new RuntimeException(e);
        }
      }
    }
    throw new RuntimeException("Failed to create database after " + maxRetries + " attempts");
  }

  private boolean isRetriableException(SQLException e) {
    // Check for connection-related errors that might be transient
    String message = e.getMessage();
    return message != null
        && (message.contains("connection")
            || message.contains("Connection")
            || message.contains("timeout")
            || e.getCause() instanceof java.net.SocketTimeoutException
            || e.getCause() instanceof java.net.ConnectException);
  }

  public String getUsername() {
    return USER_NAME;
  }

  public String getPassword() {
    return PASSWORD;
  }

  /** getJdbcUrl without database name. */
  public String getJdbcUrl() {
    return format(
        "jdbc:postgresql://%s:%d/?connectTimeout=60&socketTimeout=60",
        container.getHost(), getMappedPort(PG_PORT));
  }

  /** getJdbcUrl with database name. */
  public String getJdbcUrl(TestDatabaseName testDatabaseName) {
    return format(
        "jdbc:postgresql://%s:%d/%s?connectTimeout=60&socketTimeout=60",
        container.getHost(), getMappedPort(PG_PORT), testDatabaseName);
  }

  public String getDriverClassName(TestDatabaseName testDatabaseName) throws SQLException {
    return DriverManager.getDriver(getJdbcUrl(testDatabaseName)).getClass().getName();
  }

  public static class Builder
      extends BaseContainer.Builder<PostgreSQLContainer.Builder, PostgreSQLContainer> {

    private Builder() {
      this.image = DEFAULT_IMAGE.toString();
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(PG_PORT);
    }

    @Override
    public PostgreSQLContainer build() {
      return new PostgreSQLContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
