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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.utility.MountableFile;

/**
 * A Trino coordinator with its workers and the Hive, MySQL and PostgreSQL its tests query, all set
 * up from {@code integration-test-common/docker-script/init}, which the Trino test tool patches per
 * Trino version; the files are copied in at start.
 *
 * <p>The cluster owns these services: they are configured for Trino (a {@code trino} database user,
 * case-insensitive MySQL table names, Hive ACID tables, seed data) and differ from the run's shared
 * containers. They start in dependency order, each with a readiness check, and stop with the
 * cluster. Readiness is checked from the test JVM: Trino over its HTTP API, Hive with the probes of
 * {@link HiveContainer}. Container logs and the Hive and Trino server logs land in {@code
 * integration-test-common/build/trino-ci-container-log}, which CI uploads; a private cluster writes
 * to a subdirectory named after its coordinator.
 *
 * <p>Inside the cluster only the workers look up a name, the coordinator's, so a private cluster
 * gives its coordinator a per-run host name and can run beside the shared one on the run's network.
 * Tests reach every node by IP address.
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
  private static final String GRAVITINO_CATALOG = "gravitino";
  private static final String HIVE_HOST = "hive";
  private static final String MYSQL_HOST = "mysql";
  private static final String POSTGRESQL_HOST = "postgresql";
  private static final String DB_PASSWORD = "ds123";
  private static final String TRINO_USER = "gravitino";
  private static final String HIVE_HADOOP_USER = "anonymous";
  private static final Pattern NEXT_URI = Pattern.compile("\"nextUri\"\\s*:\\s*\"([^\"]+)\"");
  private static final Pattern SINGLE_VALUE =
      Pattern.compile("\"data\"\\s*:\\s*\\[\\s*\\[\\s*(\\d+)\\s*]\\s*]");
  private static final Pattern STARTED = Pattern.compile("\"starting\"\\s*:\\s*false");
  // The Trino CLI check this replaces ran for at most five minutes.
  private static final Duration COORDINATOR_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration WORKERS_TIMEOUT = Duration.ofMinutes(2);
  private static final Duration HIVE_TIMEOUT = Duration.ofMinutes(5);
  // Every Trino node loads the Gravitino connector, which calls the server on the host.
  private static final Map<String, String> GRAVITINO_HOST =
      ImmutableMap.of("host.docker.internal", "host-gateway");

  private final List<BaseContainer> services;
  private final List<BaseContainer> workers;
  @Nullable private final Network ownNetwork;
  private final Path logDir;

  private TrinoCluster(
      GenericContainer<?> coordinator,
      String coordinatorHost,
      Map<String, String> env,
      Optional<Network> network,
      @Nullable Network ownNetwork,
      List<BaseContainer> services,
      List<BaseContainer> workers,
      Path logDir) {
    super(
        coordinator,
        coordinatorHost,
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

  // The coordinator's own wait strategy, set by the builder from the worker count.
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
   * Starts the services, then the workers, then the coordinator, whose wait strategy also waits
   * until every worker has joined. Workers start first and keep retrying the coordinator until it
   * is up.
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
  }

  /**
   * Returns how to tell the coordinator is ready: it has finished starting, lists the {@code
   * gravitino} catalog, and sees every worker as active. Each check runs from the test JVM over the
   * Trino HTTP API.
   *
   * @param workers the number of workers to wait for
   * @return the wait strategy
   */
  static WaitStrategy coordinatorReadiness(int workers) {
    WaitAllStrategy readiness =
        new WaitAllStrategy(WaitAllStrategy.Mode.WITH_INDIVIDUAL_TIMEOUTS_ONLY)
            .withStrategy(
                new ProbeWaitStrategy(
                        "the Trino coordinator to finish starting",
                        target -> hasStarted(get(baseUri(target) + "/v1/info")))
                    .withStartupTimeout(COORDINATOR_TIMEOUT))
            .withStrategy(
                new ProbeWaitStrategy(
                        "Trino to list the " + GRAVITINO_CATALOG + " catalog",
                        target ->
                            queryCount(
                                    baseUri(target),
                                    "SELECT count(*) FROM system.metadata.catalogs"
                                        + " WHERE catalog_name = '"
                                        + GRAVITINO_CATALOG
                                        + "'")
                                > 0)
                    .withStartupTimeout(COORDINATOR_TIMEOUT));
    if (workers > 0) {
      readiness.withStrategy(
          new ProbeWaitStrategy(
                  String.format("%d Trino workers to join", workers),
                  target ->
                      queryCount(
                              baseUri(target),
                              "SELECT count(*) FROM system.runtime.nodes"
                                  + " WHERE coordinator = false AND state = 'active'")
                          >= workers)
              .withStartupTimeout(WORKERS_TIMEOUT));
    }
    return readiness;
  }

  private static String baseUri(WaitStrategyTarget target) {
    return String.format("http://%s:%d", target.getHost(), target.getMappedPort(TRINO_PORT));
  }

  private static String get(String uri) throws IOException, InterruptedException {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(uri)).timeout(Duration.ofSeconds(10)).GET().build();
    return httpClient().send(request, HttpResponse.BodyHandlers.ofString()).body();
  }

  // Runs a one-number query the way the Trino CLI would, over the HTTP protocol the CLI speaks,
  // following nextUri until the query finishes. A failed query has no number, so it counts as 0.
  private static int queryCount(String baseUri, String sql)
      throws IOException, InterruptedException {
    HttpClient http = httpClient();
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(baseUri + "/v1/statement"))
            .header("X-Trino-User", TRINO_USER)
            .timeout(Duration.ofSeconds(10))
            .POST(HttpRequest.BodyPublishers.ofString(sql))
            .build();
    int count = 0;
    for (int page = 0; request != null && page < 100; page++) {
      String body = http.send(request, HttpResponse.BodyHandlers.ofString()).body();
      Optional<Integer> value = firstValue(body);
      if (value.isPresent()) {
        count = value.get();
      }
      Optional<String> next = nextUri(body);
      request =
          next.isEmpty()
              ? null
              : HttpRequest.newBuilder(URI.create(next.get()))
                  .header("X-Trino-User", TRINO_USER)
                  .timeout(Duration.ofSeconds(10))
                  .GET()
                  .build();
    }
    return count;
  }

  private static HttpClient httpClient() {
    return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
  }

  /** Returns whether a Trino {@code /v1/info} response says the node has finished starting. */
  static boolean hasStarted(String infoBody) {
    return STARTED.matcher(infoBody).find();
  }

  /** Returns the {@code nextUri} of a Trino statement response, if the query has more pages. */
  static Optional<String> nextUri(String body) {
    Matcher matcher = NEXT_URI.matcher(body);
    return matcher.find() ? Optional.of(matcher.group(1)) : Optional.empty();
  }

  /** Returns the single integer of a one-row, one-column Trino statement response page. */
  static Optional<Integer> firstValue(String body) {
    Matcher matcher = SINGLE_VALUE.matcher(body);
    return matcher.find() ? Optional.of(Integer.parseInt(matcher.group(1))) : Optional.empty();
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
          if (service.getContainer().isRunning()) {
            ContainerLogs.copyArchive(
                service.getContainer(),
                logDir.resolve("hive-logs.tgz"),
                "/usr/local/hadoop/logs",
                "/tmp/root");
          }
        } catch (IOException | RuntimeException e) {
          LOG.warn("Could not copy the Hive logs for CI", e);
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
    private boolean privateNames;

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

    /**
     * Gives the coordinator a per-run host name, which the workers look up, and the logs a
     * directory of their own, so that this cluster can run beside the shared one.
     *
     * @param privateNames whether this is a private cluster
     * @return this builder
     */
    public Builder withPrivateNames(boolean privateNames) {
      this.privateNames = privateNames;
      return this;
    }

    /**
     * Returns the coordinator's host name: {@code trino}, or a per-run name for a private cluster.
     *
     * @return the host name
     */
    String coordinatorHost() {
      return privateNames ? RunNames.containerName(COORDINATOR_HOST) : COORDINATOR_HOST;
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
      String coordinatorHost = coordinatorHost();
      Path sharedLogDir =
          Path.of(root, "integration-test-common", "build", "trino-ci-container-log");
      Path logDir = privateNames ? sharedLogDir.resolve(coordinatorHost) : sharedLogDir;
      try {
        // Start from an empty directory, so CI uploads only this run's logs.
        if (Files.isDirectory(logDir)) {
          try (Stream<Path> old = Files.list(logDir)) {
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
      Network ownNetwork =
          shared == null
              ? Network.builder()
                  .createNetworkCmdModifier(cmd -> cmd.withName(RunNames.networkName("trino")))
                  .build()
              : null;
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
                trinoEnv("worker", coordinatorHost),
                network));
      }

      GenericContainer<?> coordinator =
          trino(image, init, connector, logDir, COORDINATOR_HOST)
              .waitingFor(coordinatorReadiness(workers));
      return new TrinoCluster(
          coordinator,
          coordinatorHost,
          trinoEnv("coordinator", coordinatorHost),
          network,
          ownNetwork,
          List.copyOf(serviceList),
          List.copyOf(workerList),
          logDir);
    }

    private Map<String, String> trinoEnv(String role, String coordinatorHost) {
      return ImmutableMap.<String, String>builder()
          .put("HADOOP_USER_NAME", "anonymous")
          .put("GRAVITINO_HOST_IP", "host.docker.internal")
          .put("GRAVITINO_HOST_PORT", String.valueOf(gravitinoServerPort))
          .put("GRAVITINO_METALAKE_NAME", metalake)
          .put("HIVE_HOST_IP", HIVE_HOST)
          .put("TRINO_WORKER_NUM", String.valueOf(workers))
          .put("TRINO_ROLE", role)
          // The workers' discovery.uri; init.sh writes it into their config.properties.
          .put("TRINO_COORDINATOR_HOST", coordinatorHost)
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
              // Trino needs the metastore and a live HDFS DataNode.
              .waitingFor(
                  HiveContainer.readiness(HIVE_HADOOP_USER).withStartupTimeout(HIVE_TIMEOUT));
      return new Service(
          hive,
          HIVE_HOST,
          ImmutableMap.of(
              HiveContainer.HADOOP_USER_NAME,
              HIVE_HADOOP_USER,
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
    private static Consumer<OutputFrame> fileLog(Path logDir, String name) {
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
      // BaseContainer already gives a per-run name; this one also says the container belongs to
      // the Trino cluster, apart from the run's shared Hive or MySQL.
      String name =
          RunNames.containerName(hostName.startsWith("trino") ? hostName : "trino-" + hostName);
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
