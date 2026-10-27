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
import java.io.File;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
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

  public static final String DEFAULT_IMAGE = TestImages.DORIS;
  public static final String HOST_NAME = "gravitino-ci-doris";
  public static final String USER_NAME = "root";
  public static final String PASSWORD = "root";
  public static final int FE_HTTP_PORT = 8030;
  public static final int FE_MYSQL_PORT = 9030;
  public static final long STARTUP_TIMEOUT_SECONDS =
      Long.parseLong(
          System.getenv().getOrDefault("GRAVITINO_CI_DORIS_STARTUP_TIMEOUT_SECONDS", "120"));

  private static final String FE_SERVICE = "doris-fe";
  private static final String BE_SERVICE = "doris-be";
  private static final int BE_HEARTBEAT_PORT = 9050;
  private static final int BE_WEBSERVER_PORT = 8040;
  private static final int BE_SCALE = 3;
  private static final String DORIS_FE_PATH = "/opt/apache-doris/fe/log";
  private static final String DORIS_BE_PATH = "/opt/apache-doris/be/log";

  private static final String BASE_COMPOSE_FILE = "docker-compose.yaml";
  private static final String DISPOSABLE_COMPOSE_FILE = "docker-compose-disposable.yaml";

  private final String image;
  private final String composeDir;
  @Nullable private ComposeContainer composeContainer;
  private String feIpAddress;
  private int feMysqlPort;

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
    this.composeDir = ITUtils.joinPath(dir, "integration-test-common", "doris-docker-script");

    Preconditions.check(
        "Doris Docker image must be configured via GRAVITINO_CI_DORIS_DOCKER_IMAGE "
            + "environment variable or DorisContainer builder withImage()",
        image != null && !image.isEmpty());
    this.image = image;
  }

  /**
   * Returns the Compose files the cluster starts from: the base file, plus {@value
   * #DISPOSABLE_COMPOSE_FILE} when the cluster is {@link Durability#DISPOSABLE}.
   *
   * @return the Compose files, base file first
   */
  List<File> composeFiles() {
    File base = new File(composeDir, BASE_COMPOSE_FILE);
    return durability() == Durability.DISPOSABLE
        ? List.of(base, new File(composeDir, DISPOSABLE_COMPOSE_FILE))
        : List.of(base);
  }

  // Built at start rather than in the constructor, because the Compose files depend on the
  // durability, which is set after the container is built.
  private ComposeContainer newComposeContainer() {
    // Support both all-in-one images (e.g. apache/gravitino-ci:doris-0.1.5) and
    // split FE/BE images (e.g. apache/doris:fe-3.0.6.2 / apache/doris:be-3.0.6.2).
    // The compose file uses FE_IMAGE/BE_IMAGE with fallback to DOCKER_IMAGE.
    String feImage = image;
    String beImage = image;
    int feIdx = image.indexOf(":fe-");
    if (feIdx > 0) {
      String base = image.substring(0, feIdx);
      String version = image.substring(feIdx + 4);
      feImage = base + ":fe-" + version;
      beImage = base + ":be-" + version;
    }

    ComposeContainer compose =
        new ComposeContainer(RunNames.composeIdentifier("doris"), composeFiles())
            .withEnv("GRAVITINO_CI_DORIS_DOCKER_IMAGE", image)
            .withEnv("GRAVITINO_CI_DORIS_FE_IMAGE", feImage)
            .withEnv("GRAVITINO_CI_DORIS_BE_IMAGE", beImage)
            .withExposedService(
                FE_SERVICE,
                FE_MYSQL_PORT,
                Wait.forListeningPort()
                    .withStartupTimeout(Duration.ofSeconds(STARTUP_TIMEOUT_SECONDS)))
            .withExposedService(FE_SERVICE, FE_HTTP_PORT)
            // Runs after the FE port checks; the FE answers before the BEs have registered.
            .waitingFor(
                FE_SERVICE,
                new ProbeWaitStrategy(
                        format("Doris to report %d live backends with capacity", BE_SCALE),
                        ProbeWaitStrategy.jdbcQuery(
                            target ->
                                format(
                                    "jdbc:mysql://%s:%d/",
                                    target.getHost(), target.getMappedPort(FE_MYSQL_PORT)),
                            USER_NAME,
                            "",
                            "SHOW PROC '/backends';",
                            backends -> StarRocksContainer.readyBackends(backends) >= BE_SCALE))
                    .withStartupTimeout(Duration.ofSeconds(STARTUP_TIMEOUT_SECONDS)))
            .withScaledService(BE_SERVICE, BE_SCALE)
            .withStartupTimeout(Duration.ofSeconds(STARTUP_TIMEOUT_SECONDS))
            .withTailChildContainers(true);

    // Expose ports for each BE instance (1-indexed)
    for (int i = 1; i <= BE_SCALE; i++) {
      compose
          .withExposedService(
              BE_SERVICE,
              i,
              BE_HEARTBEAT_PORT,
              Wait.forListeningPort()
                  .withStartupTimeout(Duration.ofSeconds(STARTUP_TIMEOUT_SECONDS)))
          .withExposedService(BE_SERVICE, i, BE_WEBSERVER_PORT);
    }
    return compose;
  }

  @Override
  public void start() {
    LOG.info("Starting Doris from {}", composeFiles());
    composeContainer = newComposeContainer();
    timedStart(composeContainer);
    resolveFeIpAddress();
    Preconditions.check("Doris container password change failed!", changePassword());
  }

  @Override
  public void close() {
    if (composeContainer != null) {
      copyDorisLog();
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
      ContainerLogs.copyArchive(
          container.get(),
          Paths.get(destPath, ContainerLogs.fileName(tarName + ".tar", container.get())),
          logPath);
    } else {
      LOG.warn("Doris {} container not found, skipping log copy", serviceName);
    }
  }

  @Override
  public String getContainerIpAddress() {
    return feIpAddress;
  }

  /** Returns the host-mapped MySQL port (random, not the container-internal 9030). */
  public int getFeMysqlPort() {
    return feMysqlPort;
  }

  private void resolveFeIpAddress() {
    // Use the host-accessible address from Testcontainers (works on macOS/Linux).
    // The internal Docker network IP is not reachable from the host on macOS.
    feIpAddress = composeContainer.getServiceHost(FE_SERVICE, FE_MYSQL_PORT);
    feMysqlPort = composeContainer.getServicePort(FE_SERVICE, FE_MYSQL_PORT);
    LOG.info("Doris FE host address: {}:{}", feIpAddress, feMysqlPort);
  }

  private boolean changePassword() {
    String dorisJdbcUrl = format("jdbc:mysql://%s:%d/", feIpAddress, feMysqlPort);

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
