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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

public class OceanBaseContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(OceanBaseContainer.class);

  public static final String DEFAULT_IMAGE = TestImages.OCEANBASE;
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
  }

  // The boot log line comes first; the server must also be active in DBA_OB_SERVERS.
  @Override
  protected WaitStrategy waitStrategy() {
    return new WaitAllStrategy()
        .withStrategy(Wait.forLogMessage(".*boot success!.*", 1))
        .withStrategy(
            new ProbeWaitStrategy(
                "OceanBase to report an active server",
                ProbeWaitStrategy.jdbcQuery(
                    target ->
                        format(
                            "jdbc:mysql://%s:%d",
                            target.getHost(), target.getMappedPort(OCEANBASE_PORT)),
                    "root@sys",
                    PASSWORD,
                    "SELECT stop_time,status FROM oceanbase.DBA_OB_SERVERS;",
                    OceanBaseContainer::hasActiveServer)));
  }

  // The boot log's six minutes plus the 30 seconds the server check had before.
  @Override
  protected Duration startupTimeout() {
    return Duration.ofMinutes(6).plusSeconds(30);
  }

  private static boolean hasActiveServer(ResultSet servers) throws SQLException {
    while (servers.next()) {
      if (servers.getString("stop_time") == null && "ACTIVE".equals(servers.getString("status"))) {
        return true;
      }
    }
    return false;
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
