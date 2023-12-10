/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import com.datastrato.gravitino.integration.test.util.CloseableGroup;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Info;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

public class ContainerSuite implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(ContainerSuite.class);
  private static volatile ContainerSuite instance = null;

  // The subnet must match the configuration in `dev/docker/tools/mac-docker-connector.conf`
  private static final String CONTAINER_NETWORK_SUBNET = "10.0.0.0/22";
  private static final String CONTAINER_NETWORK_GATEWAY = "10.0.0.1";
  private static final String CONTAINER_NETWORK_IPRANGE = "10.0.0.100/22";
  private static Network network = null;
  private static HiveContainer hiveContainer;
  private static TrinoContainer trinoContainer;

  protected static final CloseableGroup closer = CloseableGroup.create();

  private ContainerSuite() {
    try {
      // Check if docker is available
      DockerClient dockerClient = DockerClientFactory.instance().client();
      Info info = dockerClient.infoCmd().exec();
      LOG.info("Docker info: {}", info);

      network = createDockerNetwork();
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
                ImmutableMap.<String, String>builder().put("HADOOP_USER_NAME", "root").build())
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

  public HiveContainer getHiveContainer() {
    return hiveContainer;
  }

  // Let containers assign addresses in a fixed subnet to avoid `mac-docker-connector` needing to
  // refresh the configuration
  private static Network createDockerNetwork() {
    com.github.dockerjava.api.model.Network.Ipam.Config ipamConfig =
        new com.github.dockerjava.api.model.Network.Ipam.Config();
    ipamConfig
        .withSubnet(CONTAINER_NETWORK_SUBNET)
        .withGateway(CONTAINER_NETWORK_GATEWAY)
        .withIpRange(CONTAINER_NETWORK_IPRANGE);

    return closer.register(
        Network.builder()
            .createNetworkCmdModifier(
                cmd ->
                    cmd.withIpam(
                        new com.github.dockerjava.api.model.Network.Ipam().withConfig(ipamConfig)))
            .build());
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
