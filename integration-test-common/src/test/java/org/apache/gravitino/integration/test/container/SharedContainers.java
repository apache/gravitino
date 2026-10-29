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

import static org.apache.gravitino.integration.test.container.RangerContainer.DOCKER_ENV_RANGER_SERVER_URL;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Info;
import com.github.dockerjava.api.model.Network.Ipam.Config;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.integration.test.util.CloseableGroup;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

/**
 * The run's shared containers: one per spec key, on the run's network, closed when the run ends.
 *
 * <p>Only specs and {@link ContainerExtension} use this class. Tests get containers through fixture
 * annotations, and code that cannot carry an annotation through {@link Fixtures}, so a container
 * started either way is the same container.
 */
// Not final: the spec unit tests mock it.
class SharedContainers {
  private static final Logger LOG = LoggerFactory.getLogger(SharedContainers.class);
  private static volatile SharedContainers instance = null;
  private static volatile boolean initialized = false;

  // The subnet must match the configuration in
  // `dev/docker/tools/mac-docker-connector.conf`
  static final String CONTAINER_NETWORK_SUBNET = "10.20.30.0/28";
  private static final String CONTAINER_NETWORK_GATEWAY = "10.20.30.1";
  private static final String CONTAINER_NETWORK_IPRANGE = "10.20.30.0/28";
  private static final int CLICKHOUSE_CLUSTER_SIZE = 3;
  private static final String CLICKHOUSE_CLUSTER_HOST_PREFIX = "gravitino-ci-clickhouse-cluster-";
  private static final String CLICKHOUSE_TEST_IMAGE_ENV = "GRAVITINO_CLICKHOUSE_TEST_IMAGE";

  private static Network network = null;
  // Keyed by Hive version: Hive2 and Hive3 tests share a JVM, and each gets its own version.
  private static final ContainerSlot<String, HiveContainer> hiveContainer = new ContainerSlot<>();

  // Enable the Ranger plugin in the Hive container
  private static volatile HiveContainer hiveRangerContainer;
  // One cluster per Gravitino server: a test class with another server replaces it.
  private static final ContainerSlot<TrinoSpec, TrinoCluster> trinoCluster = new ContainerSlot<>();
  // One standalone Trino node per Gravitino server and metalake.
  private static final ContainerSlot<TrinoNodeSpec, TrinoContainer> trinoNode =
      new ContainerSlot<>();
  private static volatile RangerContainer rangerContainer;
  private static volatile KafkaContainer kafkaContainer;
  private static final ContainerSlot<String, HiveContainer> kerberosHiveContainer =
      new ContainerSlot<>();
  private static volatile StarRocksContainer starRocksContainer;
  private static volatile MySQLContainer mySQLContainer;
  private static volatile MySQLContainer mySQLVersion5Container;
  private static volatile Map<PGImageName, PostgreSQLContainer> pgContainerMap =
      new EnumMap<>(PGImageName.class);
  private static volatile Map<DorisImageName, DorisContainer> dorisContainerMap =
      new EnumMap<>(DorisImageName.class);
  private static volatile OceanBaseContainer oceanBaseContainer;
  private static volatile ClickHouseContainer clickHouseContainer;
  private static volatile ClickHouseContainer clickHouseClusterContainer;
  private static volatile List<ClickHouseContainer> clickHouseClusterContainers = List.of();
  private static volatile ZooKeeperContainer zooKeeperContainer;

  private static volatile GravitinoLocalStackContainer gravitinoLocalStackContainer;

  private static volatile RustFSContainer rustFSContainer;

  private static volatile GravitinoMotoContainer motoContainer;

  /**
   * We can share the same Hive container as Hive container with S3 contains the following
   * differences: 1. Configuration of S3 and corresponding environment variables 2. The Hive
   * container with S3 is Hive3 container and the Hive container is Hive2 container. There is
   * something wrong with the hive2 container to access the S3.
   */
  // A Hive whose tables live in cloud storage, keyed by storage and version; one at a time.
  private static final ContainerSlot<String, HiveContainer> storageHiveContainer =
      new ContainerSlot<>();

  private static final CloseableGroup closer = CloseableGroup.create();

  private SharedContainers() {}

  private static void initIfNecessary() {
    if (!initialized) {
      synchronized (SharedContainers.class) {
        if (!initialized) {
          try {
            if (ExternalEndpoints.allExternal()) {
              // Every service is external, so the run needs no Docker.
              LOG.info("All test containers are external; not checking Docker");
              initialized = true;
              return;
            }
            // Check if docker is available and you should never close the global DockerClient!
            DockerClient dockerClient = DockerClientFactory.instance().client();
            Info info = dockerClient.infoCmd().exec();
            LOG.info("Docker info: {}", info);
            LOG.info("Test container run id: {}", RunNames.runId());

            if ("true".equalsIgnoreCase(System.getenv("NEED_CREATE_DOCKER_NETWORK"))) {
              network = createDockerNetwork();
            }
            initialized = true;
          } catch (Exception e) {
            throw new RuntimeException("Failed to initialize the shared containers", e);
          }
        }
      }
    }
  }

  static SharedContainers getInstance() {
    if (instance == null) {
      synchronized (SharedContainers.class) {
        if (instance == null) {
          instance = new SharedContainers();
        }
      }
    }
    return instance;
  }

  // Null when the run has no shared network: it exists only where NEED_CREATE_DOCKER_NETWORK is
  // set (the macOS mac-docker-connector setup), and only after the first shared container starts.
  @Nullable
  Network getNetwork() {
    return network;
  }

  void startHiveContainer(Map<String, String> env) {
    synchronized (SharedContainers.class) {
      hiveContainer.getOrStart(
          hiveVersion(env),
          () -> {
            initIfNecessary();
            HiveContainer container =
                closer.register(hiveBuilder(env).withNetwork(network).build());
            start(container);
            return container;
          });
    }
  }

  /**
   * Starts, or returns the running, Hive whose tables live in the given cloud storage. Only one
   * runs at a time: asking for another storage or version replaces it.
   *
   * @param storage the storage, other than {@link HiveStorage#HDFS}
   * @param env the container environment, including {@link HiveContainer#HIVE_RUNTIME_VERSION}
   * @param filesToMount host files to mount, keyed by container path
   * @return the running Hive
   */
  HiveContainer startStorageHiveContainer(
      HiveStorage storage, Map<String, String> env, Map<String, String> filesToMount) {
    synchronized (SharedContainers.class) {
      return storageHiveContainer.getOrStart(
          storage + "/" + hiveVersion(env),
          () -> {
            initIfNecessary();
            HiveContainer container =
                closer.register(
                    hiveBuilder(env).withFilesToMount(filesToMount).withNetwork(network).build());
            start(container);
            return container;
          });
    }
  }

  /**
   * To start and enable Ranger plugin in Hive container, <br>
   * you can specify environment variables: <br>
   * 1. HIVE_RUNTIME_VERSION: Hive version, currently only support `hive3`, We can support `hive2`
   * in the future <br>
   * 2. DOCKER_ENV_RANGER_SERVER_URL: Ranger server URL <br>
   * 3. DOCKER_ENV_RANGER_HIVE_REPOSITORY_NAME: Ranger Hive repository name <br>
   * 4. DOCKER_ENV_RANGER_HDFS_REPOSITORY_NAME: Ranger HDFS repository name <br>
   */
  void startHiveRangerContainer(Map<String, String> envVars) {
    // If you want to enable Hive Ranger plugin, you need both set the `RANGER_SERVER_URL` and
    // `RANGER_HIVE_REPOSITORY_NAME` environment variables.
    // If you want to enable HDFS Ranger plugin, you need both set the `RANGER_SERVER_URL` and
    // `RANGER_HDFS_REPOSITORY_NAME` environment variables.
    if (envVars == null
        || (!Objects.equals(envVars.get(HiveContainer.HIVE_RUNTIME_VERSION), HiveContainer.HIVE3))
        || (!envVars.containsKey(DOCKER_ENV_RANGER_SERVER_URL)
            || (!envVars.containsKey(RangerContainer.DOCKER_ENV_RANGER_HIVE_REPOSITORY_NAME)
                && !envVars.containsKey(RangerContainer.DOCKER_ENV_RANGER_HDFS_REPOSITORY_NAME)))) {
      throw new IllegalArgumentException("Error environment variables for Hive Ranger container");
    }

    if (hiveRangerContainer == null) {
      synchronized (SharedContainers.class) {
        if (hiveRangerContainer == null) {
          HiveContainer container =
              closer.register(hiveRangerBuilder(envVars).withNetwork(network).build());
          start(container);
          hiveRangerContainer = container;
        }
      }
    }
  }

  void startKerberosHiveContainer(Map<String, String> envVars) {
    synchronized (SharedContainers.class) {
      kerberosHiveContainer.getOrStart(
          hiveVersion(envVars),
          () -> {
            initIfNecessary();
            HiveContainer container =
                closer.register(kerberosHiveBuilder(envVars).withNetwork(network).build());
            start(container);
            return container;
          });
    }
  }

  /**
   * Starts every dependency of {@code spec}, deepest first, and then returns its running shared
   * container.
   *
   * @param spec the spec to start
   * @param shared the run's shared containers
   * @param <C> the container type
   * @return the running container
   * @throws IllegalStateException if the dependencies form a cycle
   */
  static <C extends BaseContainer> C startWithDependencies(
      ContainerSpec<C> spec, SharedContainers shared) {
    startDependencies(spec, shared);
    return spec.getOrStart(shared);
  }

  /**
   * Starts every dependency of {@code spec}, deepest first, but not {@code spec} itself; for
   * private containers, whose own start happens elsewhere.
   *
   * @param spec the spec whose dependencies to start
   * @param shared the run's shared containers
   * @throws IllegalStateException if the dependencies form a cycle
   */
  static void startDependencies(ContainerSpec<?> spec, SharedContainers shared) {
    startDependencies(spec, shared, new ArrayDeque<>());
  }

  private static void startDependencies(
      ContainerSpec<?> spec, SharedContainers shared, Deque<ContainerSpec<?>> path) {
    if (path.contains(spec)) {
      path.addLast(spec);
      throw new IllegalStateException("Container dependencies form a cycle: " + path);
    }
    path.addLast(spec);
    for (ContainerSpec<?> dependency : spec.dependencies()) {
      startDependencies(dependency, shared, path);
      dependency.getOrStart(shared);
    }
    path.removeLast();
  }

  // The shared Hive and a private one (HiveSpec.startPrivate) are built the same way.
  static HiveContainer.Builder hiveBuilder(Map<String, String> env) {
    return HiveContainer.builder()
        .withHostReachableHdfs()
        .withHostName("gravitino-ci-hive")
        .withEnvVars(
            ImmutableMap.<String, String>builder()
                .putAll(env)
                .put("HADOOP_USER_NAME", "anonymous")
                .build());
  }

  static HiveContainer.Builder kerberosHiveBuilder(Map<String, String> env) {
    return HiveContainer.builder()
        .withHostName("gravitino-ci-kerberos-hive")
        .withKerberosEnabled(true)
        .withEnvVars(env);
  }

  static HiveContainer.Builder hiveRangerBuilder(Map<String, String> env) {
    return HiveContainer.builder().withHostName("gravitino-ci-hive-ranger").withEnvVars(env);
  }

  static MySQLContainer.Builder mySQLBuilder(MySQLVersion version) {
    MySQLContainer.Builder builder =
        MySQLContainer.builder().withExposePorts(ImmutableSet.of(MySQLContainer.MYSQL_PORT));
    return switch (version) {
      case MYSQL_8_0 -> builder.withHostName("gravitino-ci-mysql");
      case MYSQL_5_7 -> builder
          .withImage(TestImages.MYSQL_5_7)
          .withHostName("gravitino-ci-mysql-v5");
    };
  }

  static PostgreSQLContainer.Builder postgreSQLBuilder(PGImageName version) {
    return PostgreSQLContainer.builder()
        .withImage(version.toString())
        .withHostName(PostgreSQLContainer.HOST_NAME)
        .withExposePorts(ImmutableSet.of(PostgreSQLContainer.PG_PORT));
  }

  static DorisContainer.Builder dorisBuilder(DorisImageName version) {
    return DorisContainer.builder()
        .withHostName("gravitino-ci-doris-" + version.name().toLowerCase().replace("_", ""))
        .withImage(version.toString());
  }

  // Each call makes a new data directory, so a private OceanBase never shares the shared one's.
  static OceanBaseContainer.Builder oceanBaseBuilder() {
    return OceanBaseContainer.builder()
        .withHostName("gravitino-ci-oceanbase")
        // The module sets MODE (mini) and OB_TENANT_PASSWORD.
        .withEnvVars(
            ImmutableMap.of(
                "OB_SYS_PASSWORD",
                OceanBaseContainer.PASSWORD,
                "OB_DATAFILE_SIZE",
                "1G",
                "OB_LOG_DISK_SIZE",
                "8G",
                "OB_MEMORY_LIMIT",
                "6G"))
        .withFilesToMount(ImmutableMap.of(oceanBaseDataDir(), "/root/ob"))
        .withExposePorts(ImmutableSet.of(OceanBaseContainer.OCEANBASE_PORT));
  }

  // The single server; the cluster nodes differ in host name, ports and remote servers.
  static ClickHouseContainer.Builder clickHouseBuilder() {
    return ClickHouseContainer.builder()
        .withImage(TestImages.CLICKHOUSE)
        .withFilesToMount(resolveClickHouseTestFiles())
        .withHostName("gravitino-ci-clickhouse")
        .withEnvVars(ImmutableMap.of("CLICKHOUSE_PASSWORD", ClickHouseContainer.PASSWORD))
        .withExposePorts(ImmutableSet.of(ClickHouseContainer.CLICKHOUSE_PORT));
  }

  static ZooKeeperContainer.Builder zooKeeperBuilder() {
    return ZooKeeperContainer.builder()
        .withHostName(ZooKeeperContainer.HOST_NAME)
        .withExposePorts(ImmutableSet.of(ZooKeeperContainer.ZK_PORT));
  }

  /**
   * Starts a container that only the caller uses, for fixtures with {@link Scope#CLASS} or {@link
   * Scope#METHOD}. It is built from the builder a shared container uses, on the shared network, and
   * starts through the same timed path, but no field keeps it, so it never replaces a shared
   * container. The caller closes it; the run's closer also closes it at the end of the run in case
   * the caller does not.
   *
   * @param builder the builder of the shared container
   * @param durability whether the container keeps its durability work
   * @param <C> the container type
   * @return the running container
   */
  <C extends BaseContainer> C startPrivate(
      BaseContainer.Builder<?, C> builder, Durability durability) {
    return startPrivate(builder, durability, container -> {});
  }

  /**
   * Starts a container like {@link #startPrivate(BaseContainer.Builder, Durability)} and then sets
   * it up, such as by creating a test database; a failed set-up closes the container.
   *
   * @param builder the builder of the shared container
   * @param durability whether the container keeps its durability work
   * @param setUp the work on the running container
   * @param <C> the container type
   * @return the running, set-up container
   */
  <C extends BaseContainer> C startPrivate(
      BaseContainer.Builder<?, C> builder, Durability durability, Consumer<? super C> setUp) {
    initIfNecessary();
    return startPrivate(network, privateBuild(builder, durability), SharedContainers::start, setUp);
  }

  static <C extends BaseContainer> Function<Network, C> privateBuild(
      BaseContainer.Builder<?, C> builder, Durability durability) {
    return network -> {
      C container = builder.withNetwork(network).build();
      container.setDurability(durability);
      return container;
    };
  }

  // Tests pass their own start so that no Docker and no disk cleanup run.
  static <C extends BaseContainer> C startPrivate(
      @Nullable Network network,
      Function<Network, C> build,
      Consumer<BaseContainer> start,
      Consumer<? super C> setUp) {
    C container = build.apply(network);
    synchronized (SharedContainers.class) {
      closer.register(container);
    }
    start.accept(container);
    try {
      setUp.accept(container);
    } catch (RuntimeException | Error e) {
      closeFailedStart(container, e);
      throw e;
    }
    return container;
  }

  void startDorisContainer(DorisImageName imageName) {
    if (!dorisContainerMap.containsKey(imageName)) {
      synchronized (SharedContainers.class) {
        if (!dorisContainerMap.containsKey(imageName)) {
          initIfNecessary();
          DorisContainer container =
              closer.register(dorisBuilder(imageName).withNetwork(network).build());
          start(container);
          dorisContainerMap.put(imageName, container);
        }
      }
    }
  }

  void startMySQLContainer(TestDatabaseName testDatabaseName) {
    if (mySQLContainer == null) {
      synchronized (SharedContainers.class) {
        if (mySQLContainer == null) {
          initIfNecessary();
          MySQLContainer container =
              closer.register(mySQLBuilder(MySQLVersion.MYSQL_8_0).withNetwork(network).build());
          start(container);
          mySQLContainer = container;
        }
      }
    }
    synchronized (MySQLContainer.class) {
      mySQLContainer.createDatabase(testDatabaseName);
    }
  }

  void startMySQLVersion5Container(TestDatabaseName testDatabaseName) {
    if (mySQLVersion5Container == null) {
      synchronized (SharedContainers.class) {
        if (mySQLVersion5Container == null) {
          initIfNecessary();
          MySQLContainer container =
              closer.register(mySQLBuilder(MySQLVersion.MYSQL_5_7).withNetwork(network).build());
          start(container);
          mySQLVersion5Container = container;
        }
      }
    }
    synchronized (MySQLContainer.class) {
      mySQLVersion5Container.createDatabase(testDatabaseName);
    }
  }

  void startPostgreSQLContainer(TestDatabaseName testDatabaseName, PGImageName pgImageName) {
    if (!pgContainerMap.containsKey(pgImageName)) {
      synchronized (SharedContainers.class) {
        if (!pgContainerMap.containsKey(pgImageName)) {
          initIfNecessary();
          PostgreSQLContainer container =
              closer.register(postgreSQLBuilder(pgImageName).withNetwork(network).build());
          start(container);
          pgContainerMap.put(pgImageName, container);
        }
      }
    }
    synchronized (PostgreSQLContainer.class) {
      pgContainerMap.get(pgImageName).createDatabase(testDatabaseName);
    }
  }

  void startOceanBaseContainer() {
    if (oceanBaseContainer == null) {
      synchronized (SharedContainers.class) {
        if (oceanBaseContainer == null) {
          OceanBaseContainer container =
              closer.register(oceanBaseBuilder().withNetwork(network).build());
          start(container);
          oceanBaseContainer = container;
        }
      }
    }
  }

  void startKafkaContainer() {
    if (kafkaContainer == null) {
      synchronized (SharedContainers.class) {
        if (kafkaContainer == null) {
          initIfNecessary();
          KafkaContainer.Builder builder = KafkaContainer.builder().withNetwork(network);
          KafkaContainer container = closer.register(builder.build());
          try {
            start(container);
          } catch (Exception e) {
            LOG.error("Failed to start Kafka container", e);
            throw new RuntimeException("Failed to start Kafka container", e);
          }
          kafkaContainer = container;
        }
      }
    }
  }

  void startLocalStackContainer() {
    if (gravitinoLocalStackContainer == null) {
      synchronized (SharedContainers.class) {
        if (gravitinoLocalStackContainer == null) {
          GravitinoLocalStackContainer.Builder builder =
              GravitinoLocalStackContainer.builder().withNetwork(network);
          GravitinoLocalStackContainer container = closer.register(builder.build());
          try {
            start(container);
          } catch (Exception e) {
            LOG.error("Failed to start LocalStack container", e);
            throw new RuntimeException("Failed to start LocalStack container", e);
          }
          gravitinoLocalStackContainer = container;
        }
      }
    }
  }

  /** Starts the shared Moto server, the AWS mock behind the Glue catalog tests. */
  void startMotoContainer() {
    if (motoContainer == null) {
      synchronized (SharedContainers.class) {
        if (motoContainer == null) {
          initIfNecessary();
          GravitinoMotoContainer.Builder builder =
              GravitinoMotoContainer.builder().withNetwork(network);
          GravitinoMotoContainer container = closer.register(builder.build());
          try {
            start(container);
          } catch (Exception e) {
            LOG.error("Failed to start Moto container", e);
            throw new RuntimeException("Failed to start Moto container", e);
          }
          motoContainer = container;
        }
      }
    }
  }

  void startStarRocksContainer() {
    if (starRocksContainer == null) {
      synchronized (SharedContainers.class) {
        if (starRocksContainer == null) {
          initIfNecessary();
          // Start StarRocks container
          StarRocksContainer.Builder starRocksBuilder =
              StarRocksContainer.builder().withNetwork(network);
          StarRocksContainer container = closer.register(starRocksBuilder.build());
          try {
            start(container);
          } catch (Exception e) {
            LOG.error("Failed to start StarRocks container", e);
            throw new RuntimeException("Failed to start StarRocks container", e);
          }
          starRocksContainer = container;
        }
      }
    }
  }

  void startClickHouseContainer(TestDatabaseName testDatabaseName) {
    if (clickHouseContainer == null) {
      synchronized (SharedContainers.class) {
        if (clickHouseContainer == null) {
          initIfNecessary();
          ClickHouseContainer container =
              closer.register(clickHouseBuilder().withNetwork(network).build());
          start(container);
          clickHouseContainer = container;
        }
      }
    }
    synchronized (ClickHouseContainer.class) {
      clickHouseContainer.createDatabase(testDatabaseName);
    }
  }

  void startClickHouseClusterContainer(
      TestDatabaseName testDatabaseName, String remoteServersTemplatePath) {
    if (clickHouseClusterContainers.isEmpty()) {
      synchronized (SharedContainers.class) {
        if (clickHouseClusterContainers.isEmpty()) {
          initIfNecessary();
          startZooKeeperContainer();
          String zkHost = zooKeeperContainer.getContainerIpAddress();
          String resolvedConfigPath = prepareRemoteServersConfig(remoteServersTemplatePath, zkHost);
          List<ClickHouseContainer> containers = new ArrayList<>(CLICKHOUSE_CLUSTER_SIZE);
          for (int node = 1; node <= CLICKHOUSE_CLUSTER_SIZE; node++) {
            ClickHouseContainer.Builder clickHouseBuilder =
                ClickHouseContainer.builder()
                    .withImage(TestImages.CLICKHOUSE)
                    .withFilesToMount(resolveClickHouseTestFiles())
                    .withHostName(CLICKHOUSE_CLUSTER_HOST_PREFIX + node)
                    .withEnvVars(
                        ImmutableMap.<String, String>builder()
                            .put("CLICKHOUSE_PASSWORD", ClickHouseContainer.PASSWORD)
                            .build())
                    .withRemoteServersConfig(resolvedConfigPath)
                    .withExposePorts(
                        ImmutableSet.of(
                            ClickHouseContainer.CLICKHOUSE_PORT,
                            ClickHouseContainer.CLICKHOUSE_NATIVE_PORT))
                    .withNetwork(network);

            ClickHouseContainer container = closer.register(clickHouseBuilder.build());
            start(container);
            containers.add(container);
          }
          clickHouseClusterContainers = List.copyOf(containers);
          clickHouseClusterContainer = clickHouseClusterContainers.get(0);
        }
      }
    }
    synchronized (ClickHouseContainer.class) {
      clickHouseClusterContainer.createDatabaseOnCluster(
          testDatabaseName, ClickHouseContainer.DEFAULT_CLUSTER_NAME);
    }
  }

  private String prepareRemoteServersConfig(String templatePath, String zkHost) {
    try {
      String content =
          new String(Files.readAllBytes(Paths.get(templatePath)), StandardCharsets.UTF_8);
      String replaced =
          content
              .replace("${ZOOKEEPER_HOST}", zkHost)
              .replace("${ZOOKEEPER_PORT}", String.valueOf(ZooKeeperContainer.ZK_PORT));
      java.nio.file.Path tempFile = Files.createTempFile("remote_servers", ".xml").toAbsolutePath();
      Files.writeString(tempFile, replaced);
      // Change the access mode of temFile to 644 to avoid permission issues when ClickHouse
      // container read the file.
      if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
        Files.setPosixFilePermissions(
            tempFile,
            Set.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.GROUP_READ,
                PosixFilePermission.OTHERS_READ));
      }
      return tempFile.toString();
    } catch (IOException e) {
      throw new RuntimeException("Failed to prepare remote_servers.xml", e);
    }
  }

  void startZooKeeperContainer() {
    if (zooKeeperContainer == null) {
      synchronized (SharedContainers.class) {
        if (zooKeeperContainer == null) {
          initIfNecessary();
          ZooKeeperContainer container =
              closer.register(zooKeeperBuilder().withNetwork(network).build());
          start(container);
          zooKeeperContainer = container;
        }
      }
    }
  }

  ClickHouseContainer getClickHouseContainer() {
    return clickHouseContainer;
  }

  ClickHouseContainer getClickHouseClusterContainer() {
    return clickHouseClusterContainer;
  }

  /**
   * Returns every ClickHouse node in the repository-managed cluster fixture.
   *
   * @return an immutable list whose first element is also returned by {@link
   *     #getClickHouseClusterContainer()}
   */
  List<ClickHouseContainer> getClickHouseClusterContainers() {
    return clickHouseClusterContainers;
  }

  ZooKeeperContainer getZooKeeperContainer() {
    return zooKeeperContainer;
  }

  GravitinoLocalStackContainer getLocalStackContainer() {
    return gravitinoLocalStackContainer;
  }

  /** Returns the shared Moto server, or null before {@link #startMotoContainer()} runs. */
  GravitinoMotoContainer getMotoContainer() {
    return motoContainer;
  }

  /** Starts the shared RustFS object store for S3 credential-vending tests. */
  void startRustFSContainer() {
    if (rustFSContainer == null) {
      synchronized (SharedContainers.class) {
        if (rustFSContainer == null) {
          RustFSContainer.Builder builder = RustFSContainer.builder().withNetwork(network);
          RustFSContainer container = closer.register(builder.build());
          try {
            start(container);
          } catch (Exception e) {
            LOG.error("Failed to start RustFS container", e);
            throw new RuntimeException("Failed to start RustFS container", e);
          }
          rustFSContainer = container;
        }
      }
    }
  }

  /**
   * Returns the shared RustFS fixture after {@link #startRustFSContainer()}.
   *
   * @return the RustFS fixture
   */
  RustFSContainer getRustFSContainer() {
    return rustFSContainer;
  }

  KafkaContainer getKafkaContainer() {
    return kafkaContainer;
  }

  /**
   * Starts, or returns the running, Trino cluster for the spec. A spec for another Gravitino server
   * or shape replaces the running cluster.
   *
   * @param spec the cluster to run, with the Gravitino server port set
   * @return the running cluster
   */
  TrinoCluster startTrinoCluster(TrinoSpec spec) {
    synchronized (SharedContainers.class) {
      return trinoCluster.getOrStart(
          spec,
          () -> {
            initIfNecessary();
            TrinoCluster cluster = closer.register(trinoClusterBuilder(spec).build());
            start(cluster);
            return cluster;
          });
    }
  }

  /**
   * Starts a Trino cluster for the spec that only the caller uses. Its coordinator has a per-run
   * host name, so it runs beside the shared cluster; the caller closes it.
   *
   * @param spec the cluster to run, with the Gravitino server port set
   * @return the running private cluster
   */
  TrinoCluster startPrivateTrinoCluster(TrinoSpec spec) {
    initIfNecessary();
    // The builder joins the run's network itself, or makes one of its own when there is none.
    return startPrivate(
        network,
        unused -> trinoClusterBuilder(spec).withPrivateNames(true).build(),
        SharedContainers::start,
        cluster -> {});
  }

  static TrinoCluster.Builder trinoClusterBuilder(TrinoSpec spec) {
    return TrinoCluster.builder()
        .withTrinoVersion(spec.version())
        .withWorkers(spec.workers())
        .withHiveRuntimeVersion(spec.hive().runtimeVersion())
        .withGravitinoServerPort(spec.gravitinoServerPort());
  }

  /**
   * Starts, or returns the running, standalone Trino node for the spec. A spec for another server
   * or metalake replaces the running node.
   *
   * @param spec the node to run
   * @param builder builds the node once its dependencies run
   * @return the running node
   */
  TrinoContainer startTrinoNode(TrinoNodeSpec spec, Supplier<TrinoContainer.Builder> builder) {
    synchronized (SharedContainers.class) {
      return trinoNode.getOrStart(
          spec,
          () -> {
            initIfNecessary();
            TrinoContainer node = closer.register(builder.get().withNetwork(network).build());
            start(node);
            return node;
          });
    }
  }

  HiveContainer getHiveContainer() {
    return hiveContainer.get();
  }

  HiveContainer getHiveRangerContainer() {
    return hiveRangerContainer;
  }

  void startRangerContainer() {
    if (rangerContainer == null) {
      synchronized (SharedContainers.class) {
        if (rangerContainer == null) {
          initIfNecessary();
          // Ranger keeps its policies in the run's MySQL, which also gets disposable data.
          MySQLContainer mysql = RangerSpec.DATABASE.getOrStart(this);
          RangerContainer container =
              closer.register(
                  RangerContainer.builder()
                      .withNetwork(network)
                      .withDatabase(mysql, RangerSpec.DATABASE.database())
                      .build());
          try {
            start(container);
          } catch (Exception e) {
            LOG.error("Failed to start Ranger container", e);
            throw new RuntimeException("Failed to start Ranger container", e);
          }
          rangerContainer = container;
        }
      }
    }
  }

  RangerContainer getRangerContainer() {
    return rangerContainer;
  }

  HiveContainer getKerberosHiveContainer() {
    return kerberosHiveContainer.get();
  }

  DorisContainer getDorisContainer(DorisImageName imageName) {
    return dorisContainerMap.get(imageName);
  }

  MySQLContainer getMySQLContainer() {
    return mySQLContainer;
  }

  MySQLContainer getMySQLVersion5Container() {
    return mySQLVersion5Container;
  }

  StarRocksContainer getStarRocksContainer() {
    return starRocksContainer;
  }

  PostgreSQLContainer getPostgreSQLContainer(PGImageName pgImageName)
      throws NoSuchElementException {
    if (!pgContainerMap.containsKey(pgImageName)) {
      throw new NoSuchElementException(
          String.format(
              "PostgreSQL container %s not found, please create it by calling startPostgreSQLContainer() first",
              pgImageName));
    }
    return pgContainerMap.get(pgImageName);
  }

  OceanBaseContainer getOceanBaseContainer() {
    return oceanBaseContainer;
  }

  // Let containers assign addresses in a fixed subnet to avoid
  // `mac-docker-connector` needing to
  // refresh the configuration
  // Disk cleanup runs here, so it happens only when a container actually starts.
  private static void start(BaseContainer container) {
    ITUtils.cleanDisk();
    startTimed(container);
  }

  // Logs one greppable line per start, so CI logs show where container time goes: containerMs is
  // the Testcontainers start with its wait strategy, which holds the readiness checks; readyMs is
  // the work after it. Failed starts are logged too, with ok=false.
  //
  // A failed start is closed here because Testcontainers leaves the container running when its
  // readiness check times out, and the caller never stores it, so the next start* call would start
  // another one beside it. The closer still holds it; closing it again at run end is a no-op.
  static void startTimed(BaseContainer container) {
    if (container.isExternal()) {
      // Nothing to pull, start or time: the service runs elsewhere.
      container.start();
      return;
    }
    long startNanos = System.nanoTime();
    long pullMs = 0;
    boolean ok = false;
    try {
      pullMs = pullImage(container);
      container.start();
      ok = true;
    } catch (Throwable e) {
      closeFailedStart(container, e);
      throw e;
    } finally {
      long totalMs = (System.nanoTime() - startNanos) / 1_000_000;
      long containerMs = container.lastContainerStartMs();
      LOG.info(
          "[CONTAINER-TIMING] container={} image={} hostName={} ok={} startMs={} pullMs={}"
              + " containerMs={} readyMs={}",
          container.getClass().getSimpleName(),
          container.getContainer().getDockerImageName(),
          container.hostName,
          ok,
          totalMs,
          pullMs,
          containerMs,
          Math.max(0, totalMs - pullMs - containerMs));
    }
  }

  // Resolves (and if needed pulls) the image before start(), so the timing line can report the pull
  // apart from the start: CI runners pull every image fresh, a warm Mac does not.
  private static long pullImage(BaseContainer container) {
    long pullNanos = System.nanoTime();
    Future<String> image = container.getContainer().getImage();
    if (image != null) {
      try {
        image.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while pulling " + container.hostName, e);
      } catch (ExecutionException e) {
        throw new IllegalStateException("Failed to pull the image for " + container.hostName, e);
      }
    }
    return (System.nanoTime() - pullNanos) / 1_000_000;
  }

  private static void closeFailedStart(BaseContainer container, Throwable startFailure) {
    LOG.warn(
        "Closing {} (image={}, hostName={}) after its start failed",
        container.getClass().getSimpleName(),
        container.getContainer().getDockerImageName(),
        container.hostName);
    try {
      container.close();
    } catch (Throwable closeFailure) {
      // The start failure is the one the test must report; the close failure only rides along.
      LOG.warn("Failed to close {}", container.getClass().getSimpleName(), closeFailure);
      if (closeFailure != startFailure) {
        startFailure.addSuppressed(closeFailure);
      }
    }
  }

  // Hive2 and Hive3 run from one image; HIVE_RUNTIME_VERSION picks the version, Hive2 by default.
  static String hiveVersion(Map<String, String> env) {
    return env.getOrDefault(HiveContainer.HIVE_RUNTIME_VERSION, HiveContainer.HIVE2);
  }

  private static Network createDockerNetwork() {
    DockerClient dockerClient = DockerClientFactory.instance().client();
    String name = RunNames.networkName("ci-network");

    // The subnet is fixed by mac-docker-connector, so a second run on this host, or any other
    // network in that range, fails here instead of removing a network some other run may use.
    List<com.github.dockerjava.api.model.Network> networks = dockerClient.listNetworksCmd().exec();

    for (com.github.dockerjava.api.model.Network network : networks) {
      List<Config> ipamConfigs = network.getIpam().getConfig();
      if (ipamConfigs == null) {
        continue;
      }
      for (Config ipamConfig : ipamConfigs) {
        try {
          if (ipRangesOverlap(ipamConfig.getSubnet(), CONTAINER_NETWORK_SUBNET)) {
            LOG.error(
                "The Docker network {} subnet {} conflicts with the subnet {} of {}. It may belong to"
                    + " another Gravitino test run on this host; wait for that run, remove {} from"
                    + " Docker, or modify the `SharedContainers.CONTAINER_NETWORK_SUBNET` variable",
                network.getName(),
                ipamConfig.getSubnet(),
                CONTAINER_NETWORK_SUBNET,
                name,
                network.getName());
            throw new RuntimeException(
                "The Docker of the network "
                    + network.getName()
                    + " subnet "
                    + ipamConfig.getSubnet()
                    + " conflicts with the subnet "
                    + CONTAINER_NETWORK_SUBNET
                    + " of "
                    + name);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    Config ipamConfig = new Config();
    ipamConfig
        .withSubnet(CONTAINER_NETWORK_SUBNET)
        .withGateway(CONTAINER_NETWORK_GATEWAY)
        .withIpRange(CONTAINER_NETWORK_IPRANGE);

    return closer.register(
        Network.builder()
            .createNetworkCmdModifier(
                cmd ->
                    cmd.withName(name)
                        .withIpam(
                            new com.github.dockerjava.api.model.Network.Ipam()
                                .withConfig(ipamConfig)))
            .build());
  }

  // Per run, not a shared /tmp/obdata: the directory is copied into the container at start.
  private static String oceanBaseDataDir() {
    try {
      Path dir = Files.createTempDirectory("gravitino-obdata-" + RunNames.runId() + "-");
      dir.toFile().deleteOnExit();
      return dir.toString();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create the OceanBase data directory", e);
    }
  }

  static boolean ipRangesOverlap(String cidr1, String cidr2) throws Exception {
    long[] net1 = cidrToRange(cidr1);
    long[] net2 = cidrToRange(cidr2);

    long startIp1 = net1[0];
    long endIp1 = net1[1];
    long startIp2 = net2[0];
    long endIp2 = net2[1];

    LOG.info("Subnet1: {} allocate IP ranger [{} ~ {}]", cidr1, long2Ip(startIp1), long2Ip(endIp1));
    LOG.info("Subnet2: {} allocate IP ranger [{} ~ {}]", cidr2, long2Ip(startIp2), long2Ip(endIp2));

    if (startIp1 > endIp2 || endIp1 < startIp2) {
      return false;
    } else {
      return true;
    }
  }

  private static String long2Ip(final long ip) {
    final StringBuilder result = new StringBuilder(15);
    result.append(ip >> 24 & 0xff).append(".");
    result.append(ip >> 16 & 0xff).append(".");
    result.append(ip >> 8 & 0xff).append(".");
    result.append(ip & 0xff);

    return result.toString();
  }

  // Classless Inter-Domain Routing (CIDR)
  // https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing
  private static long[] cidrToRange(String cidr) throws Exception {
    String[] parts = cidr.split("/");
    InetAddress inetAddress = InetAddress.getByName(parts[0]);
    int prefixLength = Integer.parseInt(parts[1]);

    ByteBuffer buffer = ByteBuffer.wrap(inetAddress.getAddress());
    long ip =
        (inetAddress.getAddress().length == 4) ? buffer.getInt() & 0xFFFFFFFFL : buffer.getLong();
    long mask = -(1L << (32 - prefixLength));

    long startIp = ip & mask;

    long endIp;
    if (inetAddress.getAddress().length == 4) {
      // IPv4
      endIp = startIp + ((1L << (32 - prefixLength)) - 1);
    } else {
      // IPv6
      endIp = startIp + ((1L << (128 - prefixLength)) - 1);
    }

    return new long[] {startIp, endIp};
  }

  /**
   * Closes every shared container when the JUnit run ends, through a resource in the root store.
   * Calling it again is harmless; the store keeps one resource. {@link ContainerExtension} and
   * {@code CloseContainerExtension} call it before every class, so the run's containers stop once,
   * after the last class.
   *
   * @param context any extension context of the run
   */
  static void closeWhenRunEnds(ExtensionContext context) {
    // Store.computeIfAbsent is atomic, so concurrent classes register the closer once.
    context.getRoot().getStore(Namespace.GLOBAL).computeIfAbsent(RunCloser.class);
  }

  private static final class RunCloser implements AutoCloseable {
    @Override
    public void close() {
      getInstance().close();
      LOG.info("Containers were closed at the end of the run");
    }
  }

  // Stops every container and resets the state; logs instead of failing, so a teardown error does
  // not fail a green run. Tests cannot close the shared containers: later classes in the same JVM
  // would have to restart every container. Only the run's closer above and this package's unit
  // tests call it.
  void close() {
    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Failed to close ContainerEnvironment", e);
    } finally {
      resetState();
    }
  }

  // The closer has stopped every container and removed the network. Drop every reference so a
  // later start* call starts a new container instead of returning a stopped one.
  private static void resetState() {
    network = null;
    initialized = false;
    hiveContainer.clear();
    hiveRangerContainer = null;
    trinoCluster.clear();
    rangerContainer = null;
    kafkaContainer = null;
    kerberosHiveContainer.clear();
    starRocksContainer = null;
    mySQLContainer = null;
    mySQLVersion5Container = null;
    pgContainerMap.clear();
    dorisContainerMap.clear();
    oceanBaseContainer = null;
    clickHouseContainer = null;
    clickHouseClusterContainer = null;
    clickHouseClusterContainers = List.of();
    zooKeeperContainer = null;
    gravitinoLocalStackContainer = null;
    rustFSContainer = null;
    motoContainer = null;
    storageHiveContainer.clear();
  }

  private static Map<String, String> resolveClickHouseTestFiles() {
    // The feature-specific ClickHouse test image is 25.8+, where Time types require this setting.
    String configuredImage = System.getenv(CLICKHOUSE_TEST_IMAGE_ENV);
    if (configuredImage == null || configuredImage.isBlank()) {
      return ImmutableMap.of();
    }

    try (InputStream settings =
        Objects.requireNonNull(
            SharedContainers.class.getResourceAsStream("/clickhouse-test-users.xml"))) {
      Path settingsPath = Files.createTempFile("gravitino-clickhouse-test-users-", ".xml");
      Files.copy(settings, settingsPath, StandardCopyOption.REPLACE_EXISTING);
      settingsPath.toFile().deleteOnExit();
      return ImmutableMap.of(
          "/etc/clickhouse-server/users.d/gravitino-test-settings.xml", settingsPath.toString());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to resolve ClickHouse test user settings", e);
    }
  }
}
