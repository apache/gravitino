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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.awaitility.Awaitility;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

public class ClickHouseContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(ClickHouseContainer.class);

  public static final String DEFAULT_IMAGE = "clickhouse:24.8.14";
  public static final String HOST_NAME = "gravitino-ci-clickhouse";
  public static final int CLICKHOUSE_PORT = 8123;
  public static final int CLICKHOUSE_NATIVE_PORT = 9000;
  public static final String USER_NAME = "default";
  public static final String PASSWORD = "default";
  public static final String DEFAULT_CLUSTER_NAME = "gravitino_cluster";
  public static final String ZOOKEEPER_HOST_KEY = "ZOOKEEPER_HOST";
  public static final String ZOOKEEPER_PORT_KEY = "ZOOKEEPER_PORT";

  public static Builder builder() {
    return new Builder();
  }

  protected ClickHouseContainer(
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
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "clickHouseContainer")));
  }

  @Override
  public void start() {
    super.start();
    Preconditions.check("clickHouse container startup failed!", checkContainerStatus(5));
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    Awaitility.await()
        .atMost(java.time.Duration.ofMinutes(2))
        .pollInterval(Duration.ofSeconds(5))
        .until(
            () -> {
              try (Connection connection =
                      DriverManager.getConnection(getJdbcUrl(), USER_NAME, getPassword());
                  Statement statement = connection.createStatement()) {
                // Simple health check query; ClickHouse should respond if it is ready.
                statement.execute("SELECT 1");
                LOG.info("clickHouse container is healthy");
                return true;
              } catch (SQLException e) {
                LOG.warn(
                    "Failed to connect to clickHouse container during Awaitility health check: {}",
                    e.getMessage(),
                    e);
                return false;
              }
            });

    return true;
  }

  public void createDatabase(TestDatabaseName testDatabaseName) {
    String clickHouseJdbcUrl =
        StringUtils.substring(
            getJdbcUrl(testDatabaseName), 0, getJdbcUrl(testDatabaseName).lastIndexOf("/"));

    // Use the default username and password to connect and create the database;
    // any non-default password must be configured via Gravitino catalog properties.
    try (Connection connection =
            DriverManager.getConnection(clickHouseJdbcUrl, USER_NAME, getPassword());
        Statement statement = connection.createStatement()) {

      String query = String.format("CREATE DATABASE IF NOT EXISTS %s;", testDatabaseName);
      // FIXME: String, which is used in SQL, can be unsafe
      statement.execute(query);
      LOG.info(
          String.format("clickHouse container database %s has been created", testDatabaseName));
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create database on clickHouse container: " + e.getMessage(), e);
    }
  }

  public String getUsername() {
    return USER_NAME;
  }

  public String getPassword() {
    return PASSWORD;
  }

  public String getJdbcUrl() {
    return format("jdbc:clickhouse://%s:%d", getContainerIpAddress(), CLICKHOUSE_PORT);
  }

  public String getJdbcUrl(TestDatabaseName testDatabaseName) {
    return format(
        "jdbc:clickhouse://%s:%d/%s", getContainerIpAddress(), CLICKHOUSE_PORT, testDatabaseName);
  }

  public String getDriverClassName(TestDatabaseName testDatabaseName) throws SQLException {
    return DriverManager.getDriver(getJdbcUrl(testDatabaseName)).getClass().getName();
  }

  public void createDatabaseOnCluster(TestDatabaseName testDatabaseName, String clusterName) {
    String clickHouseJdbcUrl =
        StringUtils.substring(
            getJdbcUrl(testDatabaseName), 0, getJdbcUrl(testDatabaseName).lastIndexOf("/"));

    try (Connection connection =
            DriverManager.getConnection(clickHouseJdbcUrl, USER_NAME, getPassword());
        Statement statement = connection.createStatement()) {

      String query =
          String.format(
              "CREATE DATABASE IF NOT EXISTS %s ON CLUSTER %s;", testDatabaseName, clusterName);
      statement.execute(query);
      LOG.info(
          String.format(
              "clickHouse cluster database %s has been created with cluster %s",
              testDatabaseName, clusterName));
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create database on clickHouse cluster: " + e.getMessage(), e);
    }
  }

  public static class Builder extends BaseContainer.Builder<Builder, ClickHouseContainer> {

    private String remoteServersConfigPath;

    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(CLICKHOUSE_PORT);
    }

    public Builder withRemoteServersConfig(String configPath) {
      this.remoteServersConfigPath = configPath;
      return this;
    }

    @Override
    public ClickHouseContainer build() {
      ImmutableSet<Integer> ports =
          remoteServersConfigPath == null
              ? ImmutableSet.copyOf(exposePorts)
              : ImmutableSet.of(CLICKHOUSE_PORT, CLICKHOUSE_NATIVE_PORT);
      ImmutableMap<String, String> mountFiles =
          remoteServersConfigPath == null
              ? ImmutableMap.copyOf(filesToMount)
              : ImmutableMap.<String, String>builder()
                  .putAll(filesToMount)
                  .put(
                      "/etc/clickhouse-server/config.d/remote_servers.xml", remoteServersConfigPath)
                  .build();

      return new ClickHouseContainer(
          image, hostName, ports, extraHosts, mountFiles, envVars, network);
    }
  }
}
