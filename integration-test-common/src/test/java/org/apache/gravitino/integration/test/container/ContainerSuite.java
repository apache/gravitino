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
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.gravitino.integration.test.util.CloseableGroup;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

public class ContainerSuite implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(ContainerSuite.class);
  private static volatile ContainerSuite instance = null;
  private static volatile boolean initialized = false;

  // The subnet must match the configuration in
  // `dev/docker/tools/mac-docker-connector.conf`
  public static final String CONTAINER_NETWORK_SUBNET = "10.20.30.0/28";
  private static final String CONTAINER_NETWORK_GATEWAY = "10.20.30.1";
  private static final String CONTAINER_NETWORK_IPRANGE = "10.20.30.0/28";
  private static final String NETWORK_NAME = "gravitino-ci-network";

  private static Network network = null;
  private static volatile HiveContainer hiveContainer;

  // Enable the Ranger plugin in the Hive container
  private static volatile HiveContainer hiveRangerContainer;
  private static volatile TrinoContainer trinoContainer;
  private static volatile TrinoITContainers trinoITContainers;
  private static volatile RangerContainer rangerContainer;
  private static volatile KafkaContainer kafkaContainer;
  private static volatile DorisContainer dorisContainer;
  private static volatile HiveContainer kerberosHiveContainer;
  private static volatile HiveContainer sqlBaseHiveContainer;
  private static volatile StarRocksContainer starRocksContainer;
  private static volatile MySQLContainer mySQLContainer;
  private static volatile MySQLContainer mySQLVersion5Container;
  private static volatile Map<PGImageName, PostgreSQLContainer> pgContainerMap =
      new EnumMap<>(PGImageName.class);
  private static volatile OceanBaseContainer oceanBaseContainer;

  private static volatile GravitinoLocalStackContainer gravitinoLocalStackContainer;

  /**
   * We can share the same Hive container as Hive container with S3 contains the following
   * differences: 1. Configuration of S3 and corresponding environment variables 2. The Hive
   * container with S3 is Hive3 container and the Hive container is Hive2 container. There is
   * something wrong with the hive2 container to access the S3.
   */
  private static volatile HiveContainer hiveContainerWithS3;

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

  public static boolean initialized() {
    return initialized;
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
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(env);
    builder.put("HADOOP_USER_NAME", "anonymous");

    if (hiveContainer == null) {
      synchronized (ContainerSuite.class) {
        if (hiveContainer == null) {
          initIfNecessary();
          // Start Hive container
          HiveContainer.Builder hiveBuilder =
              HiveContainer.builder()
                  .withHostName("gravitino-ci-hive")
                  .withEnvVars(builder.build())
                  .withNetwork(network);
          HiveContainer container = closer.register(hiveBuilder.build());
          container.start();
          hiveContainer = container;
        }
      }
    }
  }

  public void startHiveContainerWithS3(Map<String, String> env) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(env);
    builder.put("HADOOP_USER_NAME", "anonymous");

    if (hiveContainerWithS3 == null) {
      synchronized (ContainerSuite.class) {
        if (hiveContainerWithS3 == null) {
          initIfNecessary();
          // Start Hive container
          HiveContainer.Builder hiveBuilder =
              HiveContainer.builder()
                  .withHostName("gravitino-ci-hive")
                  .withEnvVars(builder.build())
                  .withNetwork(network);
          HiveContainer container = closer.register(hiveBuilder.build());
          container.start();
          hiveContainerWithS3 = container;
        }
      }
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
          container.start();
          hiveRangerContainer = container;
        }
      }
    }
  }

  public void startKerberosHiveContainer() {
    if (kerberosHiveContainer == null) {
      synchronized (ContainerSuite.class) {
        if (kerberosHiveContainer == null) {
          initIfNecessary();
          // Start Hive container
          HiveContainer.Builder hiveBuilder =
              HiveContainer.builder()
                  .withHostName("gravitino-ci-kerberos-hive")
                  .withKerberosEnabled(true)
                  .withNetwork(network);
          HiveContainer container = closer.register(hiveBuilder.build());
          container.start();
          kerberosHiveContainer = container;
        }
      }
    }
  }

  public void startSQLBaseAuthHiveContainer(Map<String, String> envVars) {
    // If you want to enable SQL based authorization, you need both set the
    // `ENABLE_SQL_BASE_AUTHORIZATION` environment.
    if (envVars == null
        || (!Objects.equals(envVars.get(HiveContainer.HIVE_RUNTIME_VERSION), HiveContainer.HIVE3))
        || (!envVars.containsKey(HiveContainer.ENABLE_SQL_BASE_AUTHORIZATION))) {
      throw new IllegalArgumentException(
          "Error environment variables for Hive SQL base authorization container");
    }

    if (sqlBaseHiveContainer == null) {
      synchronized (ContainerSuite.class) {
        if (sqlBaseHiveContainer == null) {
          initIfNecessary();
          // Start Hive container
          HiveContainer.Builder hiveBuilder =
              HiveContainer.builder()
                  .withHostName("gravitino-ci-hive")
                  .withNetwork(network)
                  .withEnvVars(envVars);
          HiveContainer container = closer.register(hiveBuilder.build());
          container.start();
          sqlBaseHiveContainer = container;
        }
      }
    }
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
          String hiveContainerIp = hiveContainer.getContainerIpAddress();
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
                  .withFilesToMount(
                      ImmutableMap.<String, String>builder()
                          .put(
                              TrinoContainer.TRINO_CONTAINER_PLUGIN_GRAVITINO_DIR,
                              trinoConnectorLibDir)
                          .build())
                  .withExposePorts(ImmutableSet.of(TrinoContainer.TRINO_PORT))
                  .withTrinoConfDir(trinoConfDir)
                  .withMetalakeName(metalakeName)
                  .withHiveContainerIP(hiveContainerIp);

          TrinoContainer container = closer.register(trinoBuilder.build());
          container.start();
          trinoContainer = container;
        }
      }
    }
  }

  public void startDorisContainer() {
    if (dorisContainer == null) {
      synchronized (ContainerSuite.class) {
        if (dorisContainer == null) {
          initIfNecessary();
          // Start Doris container
          DorisContainer.Builder dorisBuilder =
              DorisContainer.builder().withHostName("gravitino-ci-doris").withNetwork(network);
          DorisContainer container = closer.register(dorisBuilder.build());
          container.start();
          dorisContainer = container;
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
                  .withEnvVars(
                      ImmutableMap.<String, String>builder()
                          .put("MYSQL_ROOT_PASSWORD", "root")
                          .build())
                  .withExposePorts(ImmutableSet.of(MySQLContainer.MYSQL_PORT))
                  .withNetwork(network);

          MySQLContainer container = closer.register(mysqlBuilder.build());
          container.start();
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
                  .withImage("mysql:5.7")
                  .withHostName("gravitino-ci-mysql-v5")
                  .withEnvVars(
                      ImmutableMap.<String, String>builder()
                          .put("MYSQL_ROOT_PASSWORD", "root")
                          .build())
                  .withExposePorts(ImmutableSet.of(MySQLContainer.MYSQL_PORT))
                  .withNetwork(network);

          MySQLContainer container = closer.register(mysqlBuilder.build());
          container.start();
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
                  .withEnvVars(
                      ImmutableMap.<String, String>builder()
                          .put("POSTGRES_USER", PostgreSQLContainer.USER_NAME)
                          .put("POSTGRES_PASSWORD", PostgreSQLContainer.PASSWORD)
                          .build())
                  .withExposePorts(ImmutableSet.of(PostgreSQLContainer.PG_PORT))
                  .withNetwork(network);

          PostgreSQLContainer container = closer.register(pgBuilder.build());
          container.start();
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
                          "mini",
                          "OB_SYS_PASSWORD",
                          OceanBaseContainer.PASSWORD,
                          "OB_TENANT_PASSWORD",
                          OceanBaseContainer.PASSWORD,
                          "OB_DATAFILE_SIZE",
                          "2G",
                          "OB_LOG_DISK_SIZE",
                          "4G"))
                  .withNetwork(network)
                  .withExposePorts(ImmutableSet.of(OceanBaseContainer.OCEANBASE_PORT));
          OceanBaseContainer container = closer.register(oceanBaseBuilder.build());
          container.start();
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
            container.start();
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
            container.start();
          } catch (Exception e) {
            LOG.error("Failed to start LocalStack container", e);
            throw new RuntimeException("Failed to start LocalStack container", e);
          }
          gravitinoLocalStackContainer = container;
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
            container.start();
          } catch (Exception e) {
            LOG.error("Failed to start StarRocks container", e);
            throw new RuntimeException("Failed to start StarRocks container", e);
          }
          starRocksContainer = container;
        }
      }
    }
  }

  public GravitinoLocalStackContainer getLocalStackContainer() {
    return gravitinoLocalStackContainer;
  }

  public HiveContainer getHiveContainerWithS3() {
    return hiveContainerWithS3;
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
    return hiveContainer;
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
            container.start();
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
    return kerberosHiveContainer;
  }

  public HiveContainer getSQLBaseAuthHiveContainer() {
    return sqlBaseHiveContainer;
  }

  public DorisContainer getDorisContainer() {
    return dorisContainer;
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

    com.github.dockerjava.api.model.Network.Ipam.Config ipamConfig =
        new com.github.dockerjava.api.model.Network.Ipam.Config();
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

  @Override
  public void close() throws IOException {
    try {
      closer.close();
      mySQLContainer = null;
      mySQLVersion5Container = null;
      hiveContainer = null;
      hiveRangerContainer = null;
      trinoContainer = null;
      trinoITContainers = null;
      rangerContainer = null;
      kafkaContainer = null;
      dorisContainer = null;
      kerberosHiveContainer = null;
      sqlBaseHiveContainer = null;
      pgContainerMap.clear();
    } catch (Exception e) {
      LOG.error("Failed to close ContainerEnvironment", e);
    }
  }
}
