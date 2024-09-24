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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

public class OceanBaseContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(OceanBaseContainer.class);

  public static final String DEFAULT_IMAGE = "oceanbase/oceanbase-ce:4.2.1-lts";
  public static final String HOST_NAME = "gravitino-ci-oceanbase";
  public static final int OCEANBASE_PORT = 2881;
  public static final String USER_NAME = "root@test";
  public static final String PASSWORD = "123456";

  public static Builder builder() {
    return new Builder();
  }

  protected OceanBaseContainer(
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
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "OceanBaseContainer")));
    waitingForLog();
    withStartupTimeout(Duration.ofMinutes(6));
  }

  @Override
  public void start() {
    super.start();
    Preconditions.check("OceanBase container startup failed!", checkContainerStatus(5));
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    String oceanBaseJdbcUrl = format("jdbc:mysql://%s:%d", "localhost", getMappedPort());
    LOG.info("OceanBase url is {}", oceanBaseJdbcUrl);

    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(30 / retryLimit, TimeUnit.SECONDS)
        .until(
            () -> {
              try (Connection connection =
                      DriverManager.getConnection(oceanBaseJdbcUrl, "root@sys", PASSWORD);
                  Statement statement = connection.createStatement()) {

                // check if OceanBase server is ready
                String query = "SELECT stop_time,status FROM oceanbase.DBA_OB_SERVERS;";
                try (ResultSet resultSet = statement.executeQuery(query)) {
                  while (resultSet.next()) {
                    String stopTime = resultSet.getString("stop_time");
                    String status = resultSet.getString("status");

                    if (Objects.isNull(stopTime) && "ACTIVE".equals(status)) {
                      LOG.info("OceanBase container startup success!");
                      return true;
                    }
                  }
                }
                LOG.info("OceanBase container is not ready yet!");
              } catch (Exception e) {
                LOG.error(e.getMessage(), e);
              }
              return false;
            });

    return true;
  }

  public void waitingForLog() {
    super.container.waitingFor(Wait.forLogMessage(".*boot success!.*", 1));
  }

  public String getUsername() {
    return USER_NAME;
  }

  public String getPassword() {
    return PASSWORD;
  }

  public int getMappedPort() {
    return getMappedPort(OCEANBASE_PORT);
  }

  public String getJdbcUrl() {
    return format("jdbc:mysql://%s:%d", getContainerIpAddress(), OCEANBASE_PORT);
  }

  public String getJdbcUrl(String testDatabaseName) {
    return format(
        "jdbc:mysql://%s:%d/%s", getContainerIpAddress(), OCEANBASE_PORT, testDatabaseName);
  }

  public String getDriverClassName(String testDatabaseName) throws SQLException {
    return DriverManager.getDriver(getJdbcUrl(testDatabaseName)).getClass().getName();
  }

  public static class Builder extends BaseContainer.Builder<Builder, OceanBaseContainer> {

    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(OCEANBASE_PORT);
    }

    @Override
    public OceanBaseContainer build() {
      return new OceanBaseContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
