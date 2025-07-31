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
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

public class StarRocksContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(StarRocksContainer.class);

  public static final String DEFAULT_IMAGE = "starrocks/allin1-ubuntu:3.3-latest";
  public static final String HOST_NAME = "gravitino-ci-starrocks";
  public static final String USER_NAME = "root";
  public static final String PASSWORD = "";
  public static final int FE_HTTP_PORT = 8030;
  public static final int FE_MYSQL_PORT = 9030;

  public static Builder builder() {
    return new Builder();
  }

  protected StarRocksContainer(
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
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "StarRocksContainer")));
    withStartupTimeout(Duration.ofMinutes(5));
  }

  @Override
  public void start() {
    super.start();
    Preconditions.check("StarRocks container startup failed!", checkContainerStatus(5));
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    String starRocksJdbcUrl = format("jdbc:mysql://%s:%d/", getContainerIpAddress(), FE_MYSQL_PORT);
    LOG.info("StarRocks url is " + starRocksJdbcUrl);

    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(30 / retryLimit, TimeUnit.SECONDS)
        .until(
            () -> {
              try (Connection connection =
                      DriverManager.getConnection(starRocksJdbcUrl, USER_NAME, "");
                  Statement statement = connection.createStatement()) {

                // execute `SHOW PROC '/backends';` to check if backends is ready
                String query = "SHOW PROC '/backends';";
                try (ResultSet resultSet = statement.executeQuery(query)) {
                  while (resultSet.next()) {
                    String alive = resultSet.getString("Alive");
                    String totalCapacity = resultSet.getString("TotalCapacity");
                    float totalCapacityFloat = Float.parseFloat(totalCapacity.split(" ")[0]);

                    // alive should be true and totalCapacity should not be 0.000
                    if (alive.equalsIgnoreCase("true") && totalCapacityFloat > 0.0f) {
                      LOG.info("StarRocks container startup success!");
                      return true;
                    }
                  }
                }
                LOG.info("StarRocks container is not ready yet!");
              } catch (Exception e) {
                LOG.error(e.getMessage(), e);
              }
              return false;
            });

    return true;
  }

  public static class Builder
      extends BaseContainer.Builder<StarRocksContainer.Builder, StarRocksContainer> {
    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(FE_HTTP_PORT, FE_MYSQL_PORT);
    }

    @Override
    public StarRocksContainer build() {
      return new StarRocksContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
