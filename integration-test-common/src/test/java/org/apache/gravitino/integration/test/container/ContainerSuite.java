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
import com.github.dockerjava.api.command.RemoveNetworkCmd;
import com.github.dockerjava.api.model.Info;
import com.github.dockerjava.api.model.Network.Ipam.Config;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
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
import javax.annotation.Nullable;
import org.apache.gravitino.integration.test.container.DatabaseContainer.Durability;
import org.apache.gravitino.integration.test.util.CloseableGroup;
import org.apache.gravitino.integration.test.util.CommandExecutor;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.ProcessData;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

public class ContainerSuite {
  public static final Logger LOG = LoggerFactory.getLogger(ContainerSuite.class);
  private static volatile ContainerSuite instance = null;
  private static volatile boolean initialized = false;

  // The subnet must match the configuration in
  // `dev/docker/tools/mac-docker-connector.conf`
  public static final String CONTAINER_NETWORK_SUBNET = "10.20.30.0/28";
  private static final String CONTAINER_NETWORK_GATEWAY = "10.20.30.1";
  private static final String CONTAINER_NETWORK_IPRANGE = "10.20.30.0/28";
  private static final String NETWORK_NAME = "gravitino-ci-network";
  private static final int CLICKHOUSE_CLUSTER_SIZE = 3;
  private static final String CLICKHOUSE_CLUSTER_HOST_PREFIX = "gravitino-ci-clickhouse-cluster-";
  private static final String CLICKHOUSE_TEST_IMAGE_ENV = "GRAVITINO_CLICKHOUSE_TEST_IMAGE";

  private static Network network = null;
  // Keyed by Hive version: Hive2 and Hive3 tests share a JVM, and each gets its own version.
  private static final ContainerSlot<String, HiveContainer> hiveContainer = new ContainerSlot<>();

  // Enable the Ranger plugin in the Hive container
  private static volatile HiveContainer hiveRangerContainer;
  private static volatile TrinoContainer trinoContainer;
  private static volatile TrinoITContainers trinoITContainers;
  private static volatile RangerContainer rangerContainer;
  private static volatile KafkaContainer kafkaContainer;
  private static volatile DorisContainer dorisContainer;
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

  protected static final CloseableGroup closer = CloseableGroup.create();

  private static void initIfNecessary() {
    if (!initialized) {
      synchronized (ContainerSuite.class) {
        if (!initialized) {
          try {
            // Check if docker is available and you should never close the global DockerClient!
            DockerClient dockerClient = DockerClientFactory.instance().client();
            Info info = dockerClient.infoCmd().exec();
            LOG.info("Docker info: {}", info);

            if ("true".equalsIgnoreCase(System.getenv("NEED_CREATE_DOCKER_NETWORK"))) {
              network = createDockerNetwork();
            }
            initialized = true;
          } catch (Exception e) {
            throw new RuntimeException("Failed to initialize ContainerSuite", e);
          }
        }
      }
    }
  }

  public static ContainerSuite getInstance() {
    if (instance == null) {
      synchronized (ContainerSuite.class) {
        if (instance == null) {
          instance = new ContainerSuite();
        }
      }
    }
    return instance;
  }

  public Network getNetwork() {
    return network;
  }

  public void startHiveContainer(Map<String, String> env) {
    synchronized (ContainerSuite.class) {
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
  public HiveContainer startStorageHiveContainer(
      HiveStorage storage, Map<String, String> env, Map<String, String> filesToMount) {
    synchronized (ContainerSuite.class) {
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

  public void startHiveContainer() {
    startHiveContainer(ImmutableMap.of());
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
  public void startHiveRangerContainer(Map<String, String> envVars) {
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
      synchronized (ContainerSuite.class) {
        if (hiveRangerContainer == null) {
          // Start Hive container
          HiveContainer.Builder hiveBuilder =
              HiveContainer.builder()
                  .withHostName("gravitino-ci-hive-ranger")
                  .withEnvVars(envVars)
                  .withNetwork(network);
          HiveContainer container = closer.register(hiveBuilder.build());
          start(container);
          hiveRangerContainer = container;
        }
      }
    }
  }

  public void startKerberosHiveContainer(Map<String, String> envVars) {
    synchronized (ContainerSuite.class) {
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

  // The shared Hive and a private one (HiveSpec.startPrivate) are built the same way.
  static HiveContainer.Builder hiveBuilder(Map<String, String> env) {
    return HiveContainer.builder()
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

  /**
   * Starts a container that only the caller uses, for fixtures with {@link Scope#CLASS} or {@link
   * Scope#METHOD}. It starts like a shared container, on the shared network, but no field keeps it,
   * so it never replaces a shared container. The caller closes it; the suite also closes it at the
   * end of the run in case the caller does not.
   *
   * @param build builds the container on the given network, which is null when there is none
   * @param <C> the container type
   * @return the running container
   */
  public <C extends BaseContainer> C startPrivate(Function<Network, C> build) {
    initIfNecessary();
    return startPrivate(network, build, ContainerSuite::start);
  }

  // Tests pass their own start so that no Docker and no disk cleanup run.
  static <C extends BaseContainer> C startPrivate(
      @Nullable Network network, Function<Network, C> build, Consumer<BaseContainer> start) {
    C container = build.apply(network);
    synchronized (ContainerSuite.class) {
      closer.register(container);
    }
    start.accept(container);
    return container;
  }

  public void startKerberosHiveContainer() {
    startKerberosHiveContainer(ImmutableMap.of());
  }

  /**
   * Files copied into the Gravitino plugin directory of the Trino container: the connector lib
   * directory, plus every jar in the directory named by {@code
   * GRAVITINO_TRINO_CONNECTOR_EXTRA_LIBS}, which lets a build add plugin jars that are not part of
   * the connector itself.
   */
  private static Map<String, String> trinoPluginFilesToMount(String trinoConnectorLibDir) {
    ImmutableMap.Builder<String, String> files = ImmutableMap.builder();
    files.put(TrinoContainer.TRINO_CONTAINER_PLUGIN_GRAVITINO_DIR, trinoConnectorLibDir);
    String extraLibDir = System.getenv("GRAVITINO_TRINO_CONNECTOR_EXTRA_LIBS");
    if (extraLibDir != null && !extraLibDir.isEmpty()) {
      File[] jars = new File(extraLibDir).listFiles((dir, name) -> name.endsWith(".jar"));
      for (File jar : jars == null ? new File[0] : jars) {
        files.put(
            TrinoContainer.TRINO_CONTAINER_PLUGIN_GRAVITINO_DIR + "/" + jar.getName(),
            jar.getAbsolutePath());
      }
    }
    return files.build();
  }

  public void startTrinoContainer(
      String trinoConfDir,
      String trinoConnectorLibDir,
      int gravitinoServerPort,
      String metalakeName) {
    if (trinoContainer == null) {
      synchronized (ContainerSuite.class) {
        if (trinoContainer == null) {
          initIfNecessary();
          // Start Trino container
          String hiveContainerIp = hiveContainer.get().getContainerIpAddress();
          TrinoContainer.Builder trinoBuilder =
              TrinoContainer.builder()
                  .withEnvVars(
                      ImmutableMap.<String, String>builder()
                          .put("HADOOP_USER_NAME", "anonymous")
                          .put("GRAVITINO_HOST_IP", "host.docker.internal")
                          .put("GRAVITINO_HOST_PORT", String.valueOf(gravitinoServerPort))
                          .put("GRAVITINO_METALAKE_NAME", metalakeName)
                          .build())
                  .withNetwork(getNetwork())
                  .withExtraHosts(
                      ImmutableMap.<String, String>builder()
                          .put("host.docker.internal", "host-gateway")
                          .put(HiveContainer.HOST_NAME, hiveContainerIp)
                          .build())
                  .withFilesToMount(trinoPluginFilesToMount(trinoConnectorLibDir))
                  .withExposePorts(ImmutableSet.of(TrinoContainer.TRINO_PORT))
                  .withTrinoConfDir(trinoConfDir)
                  .withMetalakeName(metalakeName)
                  .withHiveContainerIP(hiveContainerIp);

          TrinoContainer container = closer.register(trinoBuilder.build());
          start(container);
          trinoContainer = container;
        }
      }
    }
  }

  public void startDorisContainer() {
    startDorisContainer(DorisImageName.VERSION_1_2);
  }

  public void startDorisContainer(DorisImageName imageName) {
    if (!dorisContainerMap.containsKey(imageName)) {
      synchronized (ContainerSuite.class) {
        if (!dorisContainerMap.containsKey(imageName)) {
          initIfNecessary();
          DorisContainer.Builder dorisBuilder =
              DorisContainer.builder()
                  .withHostName(
                      "gravitino-ci-doris-" + imageName.name().toLowerCase().replace("_", ""))
                  .withImage(imageName.toString())
                  .withNetwork(network);
          DorisContainer container = closer.register(dorisBuilder.build());
          start(container);
          dorisContainerMap.put(imageName, container);
          // Keep backward-compatible reference for getDorisContainer()
          if (imageName == DorisImageName.VERSION_1_2) {
            dorisContainer = container;
          }
        }
      }
    }
  }

  public void startMySQLContainer(TestDatabaseName testDatabaseName) {
    if (mySQLContainer == null) {
      synchronized (ContainerSuite.class) {
        if (mySQLContainer == null) {
          initIfNecessary();
          // Start MySQL container
          MySQLContainer.Builder mysqlBuilder =
              MySQLContainer.builder()
                  .withHostName("gravitino-ci-mysql")
                  .withDurability(Durability.fromSystemProperty())
                  .withExposePorts(ImmutableSet.of(MySQLContainer.MYSQL_PORT))
                  .withNetwork(network);

          MySQLContainer container = closer.register(mysqlBuilder.build());
          start(container);
          mySQLContainer = container;
        }
      }
    }
    synchronized (MySQLContainer.class) {
      mySQLContainer.createDatabase(testDatabaseName);
    }
  }

  public void startMySQLVersion5Container(TestDatabaseName testDatabaseName) {
    if (mySQLVersion5Container == null) {
      synchronized (ContainerSuite.class) {
        if (mySQLVersion5Container == null) {
          initIfNecessary();
          // Start MySQL container
          MySQLContainer.Builder mysqlBuilder =
              MySQLContainer.builder()
                  .withImage(TestImages.MYSQL_5_7)
                  .withHostName("gravitino-ci-mysql-v5")
                  .withDurability(Durability.fromSystemProperty())
                  .withExposePorts(ImmutableSet.of(MySQLContainer.MYSQL_PORT))
                  .withNetwork(network);

          MySQLContainer container = closer.register(mysqlBuilder.build());
          start(container);
          mySQLVersion5Container = container;
        }
      }
    }
    synchronized (MySQLContainer.class) {
      mySQLVersion5Container.createDatabase(testDatabaseName);
    }
  }

  public void startPostgreSQLContainer(TestDatabaseName testDatabaseName, PGImageName pgImageName) {
    if (!pgContainerMap.containsKey(pgImageName)) {
      synchronized (ContainerSuite.class) {
        if (!pgContainerMap.containsKey(pgImageName)) {
          initIfNecessary();
          // Start PostgreSQL container
          PostgreSQLContainer.Builder pgBuilder =
              PostgreSQLContainer.builder()
                  .withImage(pgImageName.toString())
                  .withHostName(PostgreSQLContainer.HOST_NAME)
                  .withDurability(Durability.fromSystemProperty())
                  .withExposePorts(ImmutableSet.of(PostgreSQLContainer.PG_PORT))
                  .withNetwork(network);

          PostgreSQLContainer container = closer.register(pgBuilder.build());
          start(container);
          pgContainerMap.put(pgImageName, container);
        }
      }
    }
    synchronized (PostgreSQLContainer.class) {
      pgContainerMap.get(pgImageName).createDatabase(testDatabaseName);
    }
  }

  public void startPostgreSQLContainer(TestDatabaseName testDatabaseName) {
    // Apply default image
    startPostgreSQLContainer(testDatabaseName, PGImageName.VERSION_13);
  }

  public void startOceanBaseContainer() {
    // create dir /tmp/obdata by CommandExecutor
    Object o =
        CommandExecutor.executeCommandLocalHost(
            "mkdir -p /tmp/obdata", false, ProcessData.TypesOfData.STREAMS_MERGED);
    LOG.info("Command mkdir -p /tmp/obdata output:\n{}", o);

    if (oceanBaseContainer == null) {
      synchronized (ContainerSuite.class) {
        if (oceanBaseContainer == null) {
          // Start OceanBase container
          OceanBaseContainer.Builder oceanBaseBuilder =
              OceanBaseContainer.builder()
                  .withHostName("gravitino-ci-oceanbase")
                  .withEnvVars(
                      ImmutableMap.of(
                          "MODE",
                          "MINI",
                          "OB_SYS_PASSWORD",
                          OceanBaseContainer.PASSWORD,
                          "OB_TENANT_PASSWORD",
                          OceanBaseContainer.PASSWORD,
                          "OB_DATAFILE_SIZE",
                          "1G",
                          "OB_LOG_DISK_SIZE",
                          "8G",
                          "OB_MEMORY_LIMIT",
                          "6G"))
                  .withNetwork(network)
                  .withFilesToMount(
                      ImmutableMap.<String, String>builder().put("/tmp/obdata", "/root/ob").build())
                  .withExposePorts(ImmutableSet.of(OceanBaseContainer.OCEANBASE_PORT));
          OceanBaseContainer container = closer.register(oceanBaseBuilder.build());
          start(container);
          oceanBaseContainer = container;
        }
      }
    }
  }

  public void startKafkaContainer() {
    if (kafkaContainer == null) {
      synchronized (ContainerSuite.class) {
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

  public void startLocalStackContainer() {
    if (gravitinoLocalStackContainer == null) {
      synchronized (ContainerSuite.class) {
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
  public void startMotoContainer() {
    if (motoContainer == null) {
      synchronized (ContainerSuite.class) {
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

  public void startStarRocksContainer() {
    if (starRocksContainer == null) {
      synchronized (ContainerSuite.class) {
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

  public void startClickHouseContainer(TestDatabaseName testDatabaseName) {
    if (clickHouseContainer == null) {
      synchronized (ContainerSuite.class) {
        if (clickHouseContainer == null) {
          initIfNecessary();
          // Start ClickHouse container
          ClickHouseContainer.Builder clickHouseBuilder =
              ClickHouseContainer.builder()
                  .withImage(TestImages.CLICKHOUSE)
                  .withFilesToMount(resolveClickHouseTestFiles())
                  .withHostName("gravitino-ci-clickhouse")
                  .withEnvVars(
                      ImmutableMap.<String, String>builder()
                          .put("CLICKHOUSE_PASSWORD", ClickHouseContainer.PASSWORD)
                          .build())
                  .withExposePorts(ImmutableSet.of(ClickHouseContainer.CLICKHOUSE_PORT))
                  .withNetwork(network);

          ClickHouseContainer container = closer.register(clickHouseBuilder.build());
          start(container);
          clickHouseContainer = container;
        }
      }
    }
    synchronized (ClickHouseContainer.class) {
      clickHouseContainer.createDatabase(testDatabaseName);
    }
  }

  public void startClickHouseClusterContainer(
      TestDatabaseName testDatabaseName, String remoteServersTemplatePath) {
    if (clickHouseClusterContainers.isEmpty()) {
      synchronized (ContainerSuite.class) {
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

  public void startZooKeeperContainer() {
    if (zooKeeperContainer == null) {
      synchronized (ContainerSuite.class) {
        if (zooKeeperContainer == null) {
          initIfNecessary();
          ZooKeeperContainer.Builder zkBuilder =
              ZooKeeperContainer.builder()
                  .withHostName(ZooKeeperContainer.HOST_NAME)
                  .withExposePorts(ImmutableSet.of(ZooKeeperContainer.ZK_PORT))
                  .withNetwork(network);
          ZooKeeperContainer container = closer.register(zkBuilder.build());
          start(container);
          zooKeeperContainer = container;
        }
      }
    }
  }

  public ClickHouseContainer getClickHouseContainer() {
    return clickHouseContainer;
  }

  public ClickHouseContainer getClickHouseClusterContainer() {
    return clickHouseClusterContainer;
  }

  /**
   * Returns every ClickHouse node in the repository-managed cluster fixture.
   *
   * @return an immutable list whose first element is also returned by {@link
   *     #getClickHouseClusterContainer()}
   */
  public List<ClickHouseContainer> getClickHouseClusterContainers() {
    return clickHouseClusterContainers;
  }

  public ZooKeeperContainer getZooKeeperContainer() {
    return zooKeeperContainer;
  }

  public GravitinoLocalStackContainer getLocalStackContainer() {
    return gravitinoLocalStackContainer;
  }

  /** Returns the shared Moto server, or null before {@link #startMotoContainer()} runs. */
  public GravitinoMotoContainer getMotoContainer() {
    return motoContainer;
  }

  /** Starts the shared RustFS object store for S3 credential-vending tests. */
  public void startRustFSContainer() {
    if (rustFSContainer == null) {
      synchronized (ContainerSuite.class) {
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
  public RustFSContainer getRustFSContainer() {
    return rustFSContainer;
  }

  public KafkaContainer getKafkaContainer() {
    return kafkaContainer;
  }

  public TrinoContainer getTrinoContainer() {
    return trinoContainer;
  }

  public static TrinoITContainers getTrinoITContainers() {
    if (trinoITContainers == null) {
      trinoITContainers = new TrinoITContainers();
      closer.register(trinoITContainers);
    }
    return trinoITContainers;
  }

  public HiveContainer getHiveContainer() {
    return hiveContainer.get();
  }

  public HiveContainer getHiveRangerContainer() {
    return hiveRangerContainer;
  }

  public void startRangerContainer() {
    if (rangerContainer == null) {
      synchronized (ContainerSuite.class) {
        if (rangerContainer == null) {
          initIfNecessary();
          // Start Ranger container
          RangerContainer.Builder rangerBuilder = RangerContainer.builder().withNetwork(network);
          RangerContainer container = closer.register(rangerBuilder.build());
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

  public RangerContainer getRangerContainer() {
    return rangerContainer;
  }

  public HiveContainer getKerberosHiveContainer() {
    return kerberosHiveContainer.get();
  }

  public DorisContainer getDorisContainer() {
    return dorisContainer;
  }

  public DorisContainer getDorisContainer(DorisImageName imageName) {
    return dorisContainerMap.get(imageName);
  }

  public MySQLContainer getMySQLContainer() {
    return mySQLContainer;
  }

  public MySQLContainer getMySQLVersion5Container() {
    return mySQLVersion5Container;
  }

  public StarRocksContainer getStarRocksContainer() {
    return starRocksContainer;
  }

  public PostgreSQLContainer getPostgreSQLContainer() throws NoSuchElementException {
    return getPostgreSQLContainer(PGImageName.VERSION_13);
  }

  public PostgreSQLContainer getPostgreSQLContainer(PGImageName pgImageName)
      throws NoSuchElementException {
    if (!pgContainerMap.containsKey(pgImageName)) {
      throw new NoSuchElementException(
          String.format(
              "PostgreSQL container %s not found, please create it by calling startPostgreSQLContainer() first",
              pgImageName));
    }
    return pgContainerMap.get(pgImageName);
  }

  public OceanBaseContainer getOceanBaseContainer() {
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

  // Logs one greppable line per start, including the container's own readiness check, so CI logs
  // show where container time goes. Failed starts are logged too, with ok=false.
  //
  // A failed start is closed here because Testcontainers leaves the container running when its
  // readiness check times out, and the caller never stores it, so the next start* call would start
  // another one beside it. The closer still holds it; closing it again at run end is a no-op.
  static void startTimed(BaseContainer container) {
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

    // Remove the `gravitino-ci-network` if it exists
    boolean networkExists =
        dockerClient.listNetworksCmd().withNameFilter(NETWORK_NAME).exec().stream()
            .anyMatch(network -> network.getName().equals(NETWORK_NAME));
    if (networkExists) {
      RemoveNetworkCmd removeNetworkCmd = dockerClient.removeNetworkCmd(NETWORK_NAME);
      removeNetworkCmd.exec();
    }

    // Check if the subnet of the network conflicts with `gravitino-ci-network`
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
                "The Docker of the network {} subnet {} conflicts with the `gravitino-ci-network` {}, "
                    + "You can either remove {} network from Docker, or modify the `ContainerSuite.CONTAINER_NETWORK_SUBNET` variable",
                network.getName(),
                ipamConfig.getSubnet(),
                CONTAINER_NETWORK_SUBNET,
                network.getName());
            throw new RuntimeException(
                "The Docker of the network "
                    + network.getName()
                    + " subnet "
                    + ipamConfig.getSubnet()
                    + " conflicts with the `gravitino-ci-network` "
                    + CONTAINER_NETWORK_SUBNET);
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
        .withIpRange(CONTAINER_NETWORK_IPRANGE)
        .setNetworkID("gravitino-ci-network");

    return closer.register(
        Network.builder()
            .createNetworkCmdModifier(
                cmd ->
                    cmd.withIpam(
                        new com.github.dockerjava.api.model.Network.Ipam().withConfig(ipamConfig)))
            .build());
  }

  public static boolean ipRangesOverlap(String cidr1, String cidr2) throws Exception {
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

  public static String long2Ip(final long ip) {
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
   * Closes the suite when the JUnit run ends, through a resource in the root store. Calling it
   * again is harmless; the store keeps one resource. {@code CloseContainerExtension} calls it
   * before every class, so the run's containers stop once, after the last class.
   *
   * @param context any extension context of the run
   */
  public static void closeWhenRunEnds(ExtensionContext context) {
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

  // Stops every container and resets the suite; logs instead of failing, so a teardown error does
  // not fail a green run. Package-private so tests cannot close the shared suite: later classes in
  // the same JVM would have to restart every container. Only the run's closer above and this
  // package's unit tests call it.
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
    trinoContainer = null;
    trinoITContainers = null;
    rangerContainer = null;
    kafkaContainer = null;
    dorisContainer = null;
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
            ContainerSuite.class.getResourceAsStream("/clickhouse-test-users.xml"))) {
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
