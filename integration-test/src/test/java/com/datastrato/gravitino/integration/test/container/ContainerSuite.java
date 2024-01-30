/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import com.datastrato.gravitino.integration.test.util.CloseableGroup;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.RemoveNetworkCmd;
import com.github.dockerjava.api.model.Info;
import com.github.dockerjava.api.model.Network.Ipam.Config;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

public class ContainerSuite implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(ContainerSuite.class);
  private static volatile ContainerSuite instance = null;

  // The subnet must match the configuration in `dev/docker/tools/mac-docker-connector.conf`
  public static final String CONTAINER_NETWORK_SUBNET = "10.20.30.0/28";
  private static final String CONTAINER_NETWORK_GATEWAY = "10.20.30.1";
  private static final String CONTAINER_NETWORK_IPRANGE = "10.20.30.0/28";
  private static final String NETWORK_NAME = "gravitino-ci-network";

  private static Network network = null;
  private static HiveContainer hiveContainer;
  private static TrinoContainer trinoContainer;
  private static TrinoITContainers trinoITContainers;

  protected static final CloseableGroup closer = CloseableGroup.create();

  private ContainerSuite() {
    try {
      // Check if docker is available
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
    if (hiveContainer != null) {
      return;
    }
    // Start Hive container
    HiveContainer.Builder hiveBuilder =
        HiveContainer.builder()
            .withHostName("gravitino-ci-hive")
            .withEnvVars(
                ImmutableMap.<String, String>builder()
                    .put("HADOOP_USER_NAME", "datastrato")
                    .build())
            .withNetwork(network);
    hiveContainer = closer.register(hiveBuilder.build());
    hiveContainer.start();
  }

  public void startTrinoContainer(
      String trinoConfDir,
      String trinoConnectorLibDir,
      InetSocketAddress gravitinoServerAddr,
      String metalakeName) {
    if (trinoContainer != null) {
      return;
    }

    // Start Trino container
    String hiveContainerIp = hiveContainer.getContainerIpAddress();
    TrinoContainer.Builder trinoBuilder =
        TrinoContainer.builder()
            .withEnvVars(
                ImmutableMap.<String, String>builder()
                    .put("HADOOP_USER_NAME", "root")
                    .put("GRAVITINO_HOST_IP", gravitinoServerAddr.getAddress().getHostAddress())
                    .put("GRAVITINO_HOST_PORT", String.valueOf(gravitinoServerAddr.getPort()))
                    .put("GRAVITINO_METALAKE_NAME", metalakeName)
                    .build())
            .withNetwork(getNetwork())
            .withExtraHosts(
                ImmutableMap.<String, String>builder()
                    .put(hiveContainerIp, hiveContainerIp)
                    .build())
            .withFilesToMount(
                ImmutableMap.<String, String>builder()
                    .put(TrinoContainer.TRINO_CONTAINER_PLUGIN_GRAVITINO_DIR, trinoConnectorLibDir)
                    .build())
            .withExposePorts(ImmutableSet.of(TrinoContainer.TRINO_PORT))
            .withTrinoConfDir(trinoConfDir)
            .withMetalakeName(metalakeName)
            .withGravitinoServerAddress(gravitinoServerAddr)
            .withHiveContainerIP(hiveContainerIp);

    trinoContainer = closer.register(trinoBuilder.build());
    trinoContainer.start();
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

  // Let containers assign addresses in a fixed subnet to avoid `mac-docker-connector` needing to
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
