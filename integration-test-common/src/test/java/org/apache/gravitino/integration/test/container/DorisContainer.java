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
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Docker-compose based Doris container for integration tests. This uses Testcontainers' {@link
 * ComposeContainer} to orchestrate separate FE and BE containers, enabling proper multi-node
 * cluster testing and compatibility with Doris 2.1.x features.
 *
 * <p>Although this class extends {@link BaseContainer} to preserve the existing contract, it
 * overrides all lifecycle methods and delegates to a {@link ComposeContainer} internally. The
 * parent class's {@link org.testcontainers.containers.GenericContainer} is not used.
 */
public class DorisContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(DorisContainer.class);

  public static final String DEFAULT_IMAGE = System.getenv("GRAVITINO_CI_DORIS_DOCKER_IMAGE");
  public static final String HOST_NAME = "gravitino-ci-doris";
  public static final String USER_NAME = "root";
  public static final String PASSWORD = "root";
  public static final int FE_HTTP_PORT = 8030;
  public static final int FE_MYSQL_PORT = 9030;

  private static final String FE_SERVICE = "doris-fe";
  private static final String BE_SERVICE = "doris-be";
  private static final int BE_HEARTBEAT_PORT = 9050;
  private static final int BE_WEBSERVER_PORT = 8040;
  private static final int BE_SCALE = 3;
  private static final String DORIS_FE_PATH = "/opt/apache-doris/fe/log";
  private static final String DORIS_BE_PATH = "/opt/apache-doris/be/log";

  private final ComposeContainer composeContainer;
  private String feIpAddress;

  public static Builder builder() {
    return new Builder();
  }

  protected DorisContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network);

    String dir = System.getenv("GRAVITINO_ROOT_DIR");
    if (dir == null || dir.isEmpty()) {
      throw new RuntimeException("GRAVITINO_ROOT_DIR is not set");
    }
    String composeFile =
        ITUtils.joinPath(
            dir, "integration-test-common", "doris-docker-script", "docker-compose.yaml");

    composeContainer =
        new ComposeContainer(new File(composeFile))
            .withEnv("GRAVITINO_CI_DORIS_DOCKER_IMAGE", DEFAULT_IMAGE)
            .withExposedService(
                FE_SERVICE,
                FE_MYSQL_PORT,
                Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)))
            .withExposedService(FE_SERVICE, FE_HTTP_PORT)
            .withScaledService(BE_SERVICE, BE_SCALE)
            .withStartupTimeout(Duration.ofMinutes(5))
            .withTailChildContainers(true)
            .withLocalCompose(true);

    // Expose ports for each BE instance (1-indexed)
    for (int i = 1; i <= BE_SCALE; i++) {
      composeContainer
          .withExposedService(
              BE_SERVICE,
              i,
              BE_HEARTBEAT_PORT,
              Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)))
          .withExposedService(BE_SERVICE, i, BE_WEBSERVER_PORT);
    }
  }

  @Override
  public void start() {
    composeContainer.start();
    resolveFeIpAddress();
    Preconditions.check("Doris container startup failed!", checkContainerStatus(5));
    Preconditions.check("Doris container password change failed!", changePassword());
  }

  @Override
  public void close() {
    copyDorisLog();
    if (composeContainer != null) {
      composeContainer.stop();
    }
  }

  private void copyDorisLog() {
    try {
      String destPath = System.getenv("IT_PROJECT_DIR");
      if (destPath == null || destPath.isEmpty()) {
        LOG.warn("IT_PROJECT_DIR is not set, skipping Doris log copy");
        return;
      }
      LOG.info("Copy doris log files to {}", destPath);
      copyContainerLog(FE_SERVICE, DORIS_FE_PATH, destPath, "doris-fe");
      for (int i = 1; i <= BE_SCALE; i++) {
        copyContainerLog(BE_SERVICE + "-" + i, DORIS_BE_PATH, destPath, "doris-be-" + i);
      }
    } catch (Exception e) {
      LOG.error("Failed to copy Doris container logs to local", e);
    }
  }

  private void copyContainerLog(String serviceName, String logPath, String destPath, String tarName)
      throws Exception {
    Optional<ContainerState> container = composeContainer.getContainerByServiceName(serviceName);
    if (container.isPresent()) {
      String tarPath = "/" + tarName + ".tar";
      container.get().execInContainer("tar", "cf", tarPath, logPath);
      container.get().copyFileFromContainer(tarPath, destPath + File.separator + tarName + ".tar");
    } else {
      LOG.warn("Doris {} container not found, skipping log copy", serviceName);
    }
  }

  @Override
  public String getContainerIpAddress() {
    return feIpAddress;
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    String dorisJdbcUrl = format("jdbc:mysql://%s:%d/", feIpAddress, FE_MYSQL_PORT);
    LOG.info("Doris JDBC url is {}", dorisJdbcUrl);

    await()
        .atMost(120, TimeUnit.SECONDS)
        .pollInterval(120 / retryLimit, TimeUnit.SECONDS)
        .until(
            () -> {
              try (Connection connection =
                      DriverManager.getConnection(dorisJdbcUrl, USER_NAME, "");
                  Statement statement = connection.createStatement()) {

                // execute `SHOW PROC '/backends';` to check if all backends are ready
                String query = "SHOW PROC '/backends';";
                int aliveCount = 0;
                try (ResultSet resultSet = statement.executeQuery(query)) {
                  while (resultSet.next()) {
                    LOG.info("Test " + resultSet.toString());
                    String alive = resultSet.getString("Alive");
                    String totalCapacity = resultSet.getString("TotalCapacity");
                    float totalCapacityFloat = Float.parseFloat(totalCapacity.split(" ")[0]);

                    if (alive.equalsIgnoreCase("true") && totalCapacityFloat > 0.0f) {
                      aliveCount++;
                    }
                  }
                }

                if (aliveCount >= BE_SCALE) {
                  LOG.info(
                      "Doris docker-compose cluster startup success! All {} backends are alive.",
                      aliveCount);
                  return true;
                }
                LOG.info(
                    "Doris docker-compose cluster is not ready yet! {}/{} backends alive.",
                    aliveCount,
                    BE_SCALE);
              } catch (Exception e) {
                LOG.error(e.getMessage(), e);
              }
              return false;
            });

    return true;
  }

  private void resolveFeIpAddress() {
    // Use the host-accessible address from Testcontainers (works on macOS/Linux).
    // The internal Docker network IP is not reachable from the host on macOS.
    feIpAddress = composeContainer.getServiceHost(FE_SERVICE, FE_MYSQL_PORT);
    LOG.info("Doris FE host address: {}", feIpAddress);
  }

  private boolean changePassword() {
    String dorisJdbcUrl = format("jdbc:mysql://%s:%d/", feIpAddress, FE_MYSQL_PORT);

    // change password for root user, Gravitino API must set password in catalog properties
    try (Connection connection = DriverManager.getConnection(dorisJdbcUrl, USER_NAME, "");
        Statement statement = connection.createStatement()) {

      String query = String.format("SET PASSWORD FOR '%s' = PASSWORD('%s');", USER_NAME, PASSWORD);
      statement.execute(query);
      LOG.info("Doris docker-compose cluster password has been changed");
      return true;
    } catch (Exception e) {
      LOG.error("Failed to change Doris password: {}", e.getMessage(), e);
    }
    return false;
  }

  public static class Builder
      extends BaseContainer.Builder<DorisContainer.Builder, DorisContainer> {
    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(FE_HTTP_PORT, FE_MYSQL_PORT);
    }

    @Override
    public DorisContainer build() {
      return new DorisContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
