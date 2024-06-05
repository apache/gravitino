/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import com.datastrato.gravitino.integration.test.util.CloseableGroup;
import com.datastrato.gravitino.integration.test.util.TestDatabaseName;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

public class ContainerSuite implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(ContainerSuite.class);
  private static volatile ContainerSuite instance = null;

  // The subnet must match the configuration in
  // `dev/docker/tools/mac-docker-connector.conf`
  public static final String CONTAINER_NETWORK_SUBNET = "10.20.30.0/28";
  private static final String CONTAINER_NETWORK_GATEWAY = "10.20.30.1";
  private static final String CONTAINER_NETWORK_IPRANGE = "10.20.30.0/28";
  private static final String NETWORK_NAME = "gravitino-ci-network";

  private static Network network = null;
  private static volatile HiveContainer hiveContainer;
  private static volatile TrinoContainer trinoContainer;
  private static volatile TrinoITContainers trinoITContainers;
  private static volatile RangerContainer rangerContainer;
  private static volatile KafkaContainer kafkaContainer;
  private static volatile DorisContainer dorisContainer;
  private static volatile HiveContainer kerberosHiveContainer;

  private static volatile MySQLContainer mySQLContainer;
  private static volatile MySQLContainer mySQLVersion5Container;
  private static volatile Map<PGImageName, PostgreSQLContainer> pgContainerMap =
      new EnumMap<>(PGImageName.class);

  protected static final CloseableGroup closer = CloseableGroup.create();

  private static void init() {
    try {
      // Check if docker is available and you should never close the global DockerClient!
      DockerClient dockerClient = DockerClientFactory.instance().client();
      Info info = dockerClient.infoCmd().exec();
      LOG.info("Docker info: {}", info);

      if ("true".equalsIgnoreCase(System.getenv("NEED_CREATE_DOCKER_NETWORK"))) {
        network = createDockerNetwork();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize ContainerSuite", e);
    }
  }

  public static ContainerSuite getInstance() {
    if (instance == null) {
      synchronized (ContainerSuite.class) {
        if (instance == null) {
          init();
          instance = new ContainerSuite();
        }
      }
    }
    return instance;
  }

  public Network getNetwork() {
    return network;
  }

  public void startHiveContainer() {
    if (hiveContainer == null) {
      synchronized (ContainerSuite.class) {
        if (hiveContainer == null) {
          // Start Hive container
          HiveContainer.Builder hiveBuilder =
              HiveContainer.builder()
                  .withHostName("gravitino-ci-hive")
                  .withEnvVars(
                      ImmutableMap.<String, String>builder()
                          .put("HADOOP_USER_NAME", "datastrato")
                          .build())
                  .withNetwork(network);
          HiveContainer container = closer.register(hiveBuilder.build());
          container.start();
          hiveContainer = container;
        }
      }
    }
  }

  public void startKerberosHiveContainer() {
    if (kerberosHiveContainer == null) {
      synchronized (ContainerSuite.class) {
        if (kerberosHiveContainer == null) {
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

  public void startTrinoContainer(
      String trinoConfDir,
      String trinoConnectorLibDir,
      int gravitinoServerPort,
      String metalakeName) {
    if (trinoContainer == null) {
      synchronized (ContainerSuite.class) {
        if (trinoContainer == null) {
          // Start Trino container
          String hiveContainerIp = hiveContainer.getContainerIpAddress();
          TrinoContainer.Builder trinoBuilder =
              TrinoContainer.builder()
                  .withEnvVars(
                      ImmutableMap.<String, String>builder()
                          .put("HADOOP_USER_NAME", "datastrato")
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

  public void startKafkaContainer() {
    if (kafkaContainer == null) {
      synchronized (ContainerSuite.class) {
        if (kafkaContainer == null) {
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

  public void startRangerContainer() {
    if (rangerContainer == null) {
      synchronized (ContainerSuite.class) {
        if (rangerContainer == null) {
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

  public DorisContainer getDorisContainer() {
    return dorisContainer;
  }

  public MySQLContainer getMySQLContainer() {
    return mySQLContainer;
  }

  public MySQLContainer getMySQLVersion5Container() {
    return mySQLVersion5Container;
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
    } catch (Exception e) {
      LOG.error("Failed to close ContainerEnvironment", e);
    }
  }
}
