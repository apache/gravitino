/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.MountableFile;

/**
 * A Trino coordinator with its workers and the Hive, MySQL and PostgreSQL its tests query, all set
 * up from {@code integration-test-common/docker-script/init}, which the Trino test tool patches per
 * Trino version; the files are copied in at start.
 *
 * <p>The cluster owns these services: they are configured for Trino (a {@code trino} database user,
 * case-insensitive MySQL table names, Hive ACID tables, seed data) and differ from the run's shared
 * containers. They start in dependency order, each with a readiness check, and stop with the
 * cluster. Container logs and the Hive and Trino server logs land in {@code
 * integration-test-common/build/trino-ci-container-log}, which CI uploads.
 *
 * <p>This object is the coordinator; {@link #serviceUrls()} returns the addresses tests use.
 */
public final class TrinoCluster extends BaseContainer {

  /** Which services start next to Trino. */
  public enum Services {
    /** Hive, MySQL and PostgreSQL. */
    ALL,
    /** MySQL only, for the MySQL-only test sets. */
    MYSQL
  }

  public static final int TRINO_PORT = 8080;
  public static final int DEFAULT_TRINO_VERSION = 478;

  private static final String COORDINATOR_HOST = "trino";
  private static final String HIVE_HOST = "hive";
  private static final String MYSQL_HOST = "mysql";
  private static final String POSTGRESQL_HOST = "postgresql";
  private static final String DB_PASSWORD = "ds123";
  // Every Trino node loads the Gravitino connector, which calls the server on the host.
  private static final Map<String, String> GRAVITINO_HOST =
      ImmutableMap.of("host.docker.internal", "host-gateway");

  private final List<BaseContainer> services;
  private final List<BaseContainer> workers;
  @Nullable private final Network ownNetwork;
  private final Path logDir;

  private TrinoCluster(
      GenericContainer<?> coordinator,
      Map<String, String> env,
      Optional<Network> network,
      @Nullable Network ownNetwork,
      List<BaseContainer> services,
      List<BaseContainer> workers,
      Path logDir) {
    super(
        coordinator,
        COORDINATOR_HOST,
        ImmutableSet.of(TRINO_PORT),
        GRAVITINO_HOST,
        ImmutableMap.of(),
        env,
        network);
    this.services = services;
    this.workers = workers;
    this.ownNetwork = ownNetwork;
    this.logDir = logDir;
  }

  public static Builder builder() {
    return new Builder();
  }

  // The coordinator's own wait strategy (set by the builder) checks for the gravitino catalog.
  @Override
  @Nullable
  protected WaitStrategy waitStrategy() {
    return null;
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    return true;
  }

  /**
   * Starts the services, then the workers, then the coordinator, and waits until every worker has
   * joined. Workers start first and keep retrying the coordinator until it is up.
   */
  @Override
  public void start() {
    for (BaseContainer service : services) {
      SharedContainers.startTimed(service);
    }
    for (BaseContainer worker : workers) {
      SharedContainers.startTimed(worker);
    }
    super.start();
    if (!workers.isEmpty()) {
      await()
          .atMost(Duration.ofMinutes(2))
          .pollDelay(Duration.ZERO)
          .pollInterval(Duration.ofSeconds(1))
          .until(() -> activeWorkers() >= workers.size());
    }
  }

  private int activeWorkers() {
    String out =
        executeInContainer(
                "trino",
                "--execute",
                "SELECT count(*) FROM system.runtime.nodes WHERE coordinator = false")
            .getStdout()
            .replace("\"", "")
            .trim();
    return out.isEmpty() ? 0 : Integer.parseInt(out);
  }

  /**
   * Returns the addresses tests use, under the keys the compose scripts printed: {@code trino_uri},
   * {@code hive_uri}, {@code hdfs_uri}, {@code mysql_uri} and {@code postgresql_uri}. Services the
   * cluster did not start are missing.
   *
   * @return the service addresses
   */
  public Map<String, String> serviceUrls() {
    ImmutableMap.Builder<String, String> urls = ImmutableMap.builder();
    urls.put("trino_uri", coordinatorUri());
    for (BaseContainer service : services) {
      String ip = service.getContainerIpAddress();
      switch (service.hostName) {
        case HIVE_HOST -> {
          urls.put(
              "hive_uri", String.format("thrift://%s:%d", ip, HiveContainer.HIVE_METASTORE_PORT));
          urls.put(
              "hdfs_uri", String.format("hdfs://%s:%d", ip, HiveContainer.HDFS_DEFAULTFS_PORT));
        }
        case MYSQL_HOST -> urls.put("mysql_uri", String.format("jdbc:mysql://%s:3306", ip));
        case POSTGRESQL_HOST -> urls.put("postgresql_uri", "jdbc:postgresql://" + ip);
        default -> {}
      }
    }
    return urls.build();
  }

  /**
   * Returns the coordinator's HTTP address.
   *
   * @return {@code http://<ip>:8080}
   */
  public String coordinatorUri() {
    return String.format("http://%s:%d", getContainerIpAddress(), TRINO_PORT);
  }

  // Copies the server logs for CI first; a failed copy must not keep containers running.
  @Override
  public void close() {
    copyServerLogs();
    try {
      super.close();
    } finally {
      for (BaseContainer worker : workers) {
        worker.close();
      }
      for (int i = services.size() - 1; i >= 0; i--) {
        services.get(i).close();
      }
      if (ownNetwork != null) {
        ownNetwork.close();
      }
    }
  }

  private void copyServerLogs() {
    copyQuietly(this, "/tmp/trino.log", logDir.resolve("trino.log"));
    for (BaseContainer service : services) {
      if (HIVE_HOST.equals(service.hostName)) {
        try {
          service.executeInContainer(
              "tar", "czf", "/tmp/hive-logs.tgz", "/usr/local/hadoop/logs", "/tmp/root");
          copyQuietly(service, "/tmp/hive-logs.tgz", logDir.resolve("hive-logs.tgz"));
        } catch (RuntimeException e) {
          LOG.warn("Could not pack the Hive logs for CI", e);
        }
      }
    }
  }

  private static void copyQuietly(BaseContainer container, String from, Path to) {
    try {
      if (container.getContainer().isRunning()) {
        container.getContainer().copyFileFromContainer(from, to.toString());
      }
    } catch (RuntimeException e) {
      LOG.warn("Could not copy {} for the CI logs", from, e);
    }
  }

  /** Builds a {@link TrinoCluster}; {@link #withGravitinoServerPort(int)} is required. */
  public static final class Builder {
    private int trinoVersion = DEFAULT_TRINO_VERSION;
    private int workers;
    private String hiveRuntimeVersion = HiveContainer.HIVE2;
    private boolean connectorTest;
    @Nullable private String connectorDir;
    private Services services = Services.ALL;
    private int gravitinoServerPort = -1;
    private String metalake = "test";

    private Builder() {}

    public Builder withTrinoVersion(int trinoVersion) {
      this.trinoVersion = trinoVersion;
      return this;
    }

    public Builder withWorkers(int workers) {
      this.workers = workers;
      return this;
    }

    public Builder withHiveRuntimeVersion(String hiveRuntimeVersion) {
      this.hiveRuntimeVersion = hiveRuntimeVersion;
      return this;
    }

    /**
     * Makes Hive create the ACID tables the Trino connector test sets read.
     *
     * @param connectorTest whether this cluster runs the Trino connector test sets
     * @return this builder
     */
    public Builder withConnectorTest(boolean connectorTest) {
      this.connectorTest = connectorTest;
      return this;
    }

    /**
     * Sets the directory holding the Gravitino Trino connector jar for this Trino version.
     *
     * @param connectorDir the directory; the 473-478 connector build output by default
     * @return this builder
     */
    public Builder withConnectorDir(String connectorDir) {
      this.connectorDir = connectorDir;
      return this;
    }

    public Builder withServices(Services services) {
      this.services = services;
      return this;
    }

    public Builder withGravitinoServerPort(int gravitinoServerPort) {
      this.gravitinoServerPort = gravitinoServerPort;
      return this;
    }

    public Builder withMetalake(String metalake) {
      this.metalake = metalake;
      return this;
    }

    public TrinoCluster build() {
      if (gravitinoServerPort <= 0) {
        throw new IllegalStateException("The Gravitino server port is required");
      }
      String root = System.getenv("GRAVITINO_ROOT_DIR");
      if (root == null || root.isEmpty()) {
        throw new IllegalStateException("GRAVITINO_ROOT_DIR is not set");
      }
      String init = ITUtils.joinPath(root, "integration-test-common", "docker-script", "init");
      String connector =
          connectorDir != null
              ? connectorDir
              : ITUtils.joinPath(
                  root, "trino-connector", "trino-connector-473-478", "build", "libs");
      checkConnectorDir(connector);
      Path logDir = Path.of(root, "integration-test-common", "build", "trino-ci-container-log");
      try {
        // Start from an empty directory, so CI uploads only this run's logs.
        if (Files.isDirectory(logDir)) {
          try (java.util.stream.Stream<Path> old = Files.list(logDir)) {
            for (Path file : (Iterable<Path>) old::iterator) {
              if (Files.isRegularFile(file)) {
                Files.deleteIfExists(file);
              }
            }
          }
        }
        Files.createDirectories(logDir);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      Network shared = SharedContainers.getInstance().getNetwork();
      Network ownNetwork = shared == null ? Network.newNetwork() : null;
      Optional<Network> network = Optional.of(shared != null ? shared : ownNetwork);

      List<BaseContainer> serviceList = new ArrayList<>();
      if (services == Services.ALL) {
        serviceList.add(hive(init, network, logDir));
        serviceList.add(postgresql(init, network, logDir));
      }
      serviceList.add(mysql(init, network, logDir));

      String image = "trinodb/trino:" + trinoVersion;
      List<BaseContainer> workerList = new ArrayList<>();
      for (int i = 1; i <= workers; i++) {
        workerList.add(
            new Service(
                trino(image, init, connector, logDir, "trino-worker-" + i),
                "trino-worker-" + i,
                GRAVITINO_HOST,
                trinoEnv("worker"),
                network));
      }

      GenericContainer<?> coordinator =
          trino(image, init, connector, logDir, COORDINATOR_HOST)
              .waitingFor(
                  Wait.forSuccessfulCommand(
                          "trino --execute \"SHOW CATALOGS LIKE 'gravitino'\" | grep -q gravitino")
                      .withStartupTimeout(Duration.ofMinutes(5)));
      return new TrinoCluster(
          coordinator,
          trinoEnv("coordinator"),
          network,
          ownNetwork,
          List.copyOf(serviceList),
          List.copyOf(workerList),
          logDir);
    }

    private Map<String, String> trinoEnv(String role) {
      return ImmutableMap.<String, String>builder()
          .put("HADOOP_USER_NAME", "anonymous")
          .put("GRAVITINO_HOST_IP", "host.docker.internal")
          .put("GRAVITINO_HOST_PORT", String.valueOf(gravitinoServerPort))
          .put("GRAVITINO_METALAKE_NAME", metalake)
          .put("HIVE_HOST_IP", HIVE_HOST)
          .put("TRINO_WORKER_NUM", String.valueOf(workers))
          .put("TRINO_ROLE", role)
          .build();
    }

    private static GenericContainer<?> trino(
        String image, String init, String connector, Path logDir, String name) {
      return new GenericContainer<>(image)
          .withCopyFileToContainer(
              MountableFile.forHostPath(ITUtils.joinPath(init, "trino")), "/tmp/trino")
          .withCopyFileToContainer(
              MountableFile.forHostPath(connector),
              TrinoContainer.TRINO_CONTAINER_PLUGIN_GRAVITINO_DIR)
          .withCreateContainerCmdModifier(
              cmd -> cmd.withEntrypoint("/bin/bash", "/tmp/trino/init.sh"))
          .withLogConsumer(fileLog(logDir, name));
    }

    private BaseContainer hive(String init, Optional<Network> network, Path logDir) {
      GenericContainer<?> hive =
          new GenericContainer<>(TestImages.HIVE)
              .withCopyFileToContainer(
                  MountableFile.forHostPath(ITUtils.joinPath(init, "hive")), "/tmp/hive")
              .withCreateContainerCmdModifier(
                  cmd -> cmd.withEntrypoint("/bin/bash", "/tmp/hive/init.sh"))
              .withLogConsumer(fileLog(logDir, HIVE_HOST))
              // The metastore answers first; Trino also needs a live HDFS data node.
              .waitingFor(
                  Wait.forSuccessfulCommand(
                          "/tmp/check-status.sh"
                              + " && hdfs dfsadmin -report 2>/dev/null"
                              + " | grep -q 'Live datanodes ([1-9]'")
                      .withStartupTimeout(Duration.ofMinutes(5)));
      return new Service(
          hive,
          HIVE_HOST,
          ImmutableMap.of(
              "HADOOP_USER_NAME",
              "anonymous",
              HiveContainer.HIVE_RUNTIME_VERSION,
              hiveRuntimeVersion,
              "TRINO_CONNECTOR_TEST",
              String.valueOf(connectorTest)),
          network);
    }

    private static BaseContainer postgresql(String init, Optional<Network> network, Path logDir) {
      GenericContainer<?> pg =
          new GenericContainer<>(TestImages.POSTGRESQL_13)
              .withCopyFileToContainer(
                  MountableFile.forHostPath(ITUtils.joinPath(init, "postgres")),
                  "/docker-entrypoint-initdb.d")
              .withLogConsumer(fileLog(logDir, POSTGRESQL_HOST))
              // Once for the init-script server, once for the real one.
              .waitingFor(
                  Wait.forLogMessage(".*database system is ready to accept connections.*", 2)
                      .withStartupTimeout(Duration.ofMinutes(2)));
      return new Service(
          pg,
          POSTGRESQL_HOST,
          ImmutableMap.of(
              "POSTGRES_USER", "trino",
              "POSTGRES_PASSWORD", DB_PASSWORD,
              "ALLOW_IP_RANGE", "0.0.0.0/0"),
          network);
    }

    private static BaseContainer mysql(String init, Optional<Network> network, Path logDir) {
      GenericContainer<?> mysql =
          new GenericContainer<>(TestImages.MYSQL)
              .withCopyFileToContainer(
                  MountableFile.forHostPath(ITUtils.joinPath(init, "mysql")),
                  "/docker-entrypoint-initdb.d")
              .withCommand(
                  "--default-authentication-plugin=mysql_native_password",
                  "--character-set-server=utf8mb4",
                  "--collation-server=utf8mb4_general_ci",
                  "--explicit_defaults_for_timestamp=true",
                  "--lower_case_table_names=1")
              .withLogConsumer(fileLog(logDir, MYSQL_HOST))
              // The init-script server listens on port 0; the real one on 3306.
              .waitingFor(
                  Wait.forLogMessage(".*ready for connections.*port: 3306.*", 1)
                      .withStartupTimeout(Duration.ofMinutes(2)));
      return new Service(
          mysql,
          MYSQL_HOST,
          ImmutableMap.of(
              "MYSQL_ROOT_PASSWORD",
              DB_PASSWORD,
              "MYSQL_USER",
              "trino",
              "MYSQL_PASSWORD",
              DB_PASSWORD,
              "MYSQL_DATABASE",
              "gt_db"),
          network);
    }

    private static void checkConnectorDir(String dir) {
      String[] files = new File(dir).list();
      if (files == null || files.length == 0) {
        throw new IllegalStateException(
            "Gravitino Trino connector directory " + dir + " does not exist or is empty");
      }
    }

    // Appends a container's output to <logDir>/<name>.log for CI.
    private static java.util.function.Consumer<OutputFrame> fileLog(Path logDir, String name) {
      Path file = logDir.resolve(name + ".log");
      return frame -> {
        try {
          Files.writeString(
              file,
              frame.getUtf8String(),
              StandardCharsets.UTF_8,
              StandardOpenOption.CREATE,
              StandardOpenOption.APPEND);
        } catch (IOException e) {
          // Logging must not break the test.
        }
      };
    }
  }

  // A service or worker of the cluster: a prepared container with its own readiness check.
  private static final class Service extends BaseContainer {
    Service(
        GenericContainer<?> container,
        String hostName,
        Map<String, String> env,
        Optional<Network> network) {
      this(container, hostName, ImmutableMap.of(), env, network);
    }

    Service(
        GenericContainer<?> container,
        String hostName,
        Map<String, String> extraHosts,
        Map<String, String> env,
        Optional<Network> network) {
      super(container, hostName, ImmutableSet.of(), extraHosts, ImmutableMap.of(), env, network);
      // Docker answers a reverse lookup of a container's IP with its name, and Hadoop clients in
      // Trino canonicalize hdfs://<ip> through that lookup. Generated names contain an underscore,
      // which is not valid in a hostname, so name each container like the old Compose setup did.
      String name = "trino-ci-" + hostName + "-" + UUID.randomUUID().toString().substring(0, 8);
      this.container.withCreateContainerCmdModifier(cmd -> cmd.withName(name));
    }

    @Override
    @Nullable
    protected WaitStrategy waitStrategy() {
      return null;
    }

    @Override
    protected boolean checkContainerStatus(int retryLimit) {
      return true;
    }
  }
}
