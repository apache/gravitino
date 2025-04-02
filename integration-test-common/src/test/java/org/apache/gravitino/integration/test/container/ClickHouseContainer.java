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

import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

public class ClickHouseContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(ClickHouseContainer.class);

  public static final String DEFAULT_IMAGE = "clickhouse:24.9.3.128";
  public static final String HOST_NAME = "gravitino-ci-clickhouse";
  public static final int CLICKHOUSE_PORT = 8123;
  public static final String USER_NAME = "default";
  public static final String PASSWORD = "default";

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
    return true;
  }

  public void createDatabase(TestDatabaseName testDatabaseName) {
    String clickHouseJdbcUrl =
        StringUtils.substring(
            getJdbcUrl(testDatabaseName), 0, getJdbcUrl(testDatabaseName).lastIndexOf("/"));

    // change password for root user, Gravitino API must set password in catalog properties
    try (Connection connection =
            DriverManager.getConnection(clickHouseJdbcUrl, USER_NAME, getPassword());
        Statement statement = connection.createStatement()) {

      String query = String.format("CREATE DATABASE IF NOT EXISTS %s;", testDatabaseName);
      // FIXME: String, which is used in SQL, can be unsafe
      statement.execute(query);
      LOG.info(
          String.format("clickHouse container database %s has been created", testDatabaseName));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
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

  public static class Builder
      extends BaseContainer.Builder<ClickHouseContainer.Builder, ClickHouseContainer> {

    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(CLICKHOUSE_PORT);
    }

    @Override
    public ClickHouseContainer build() {
      return new ClickHouseContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
