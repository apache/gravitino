/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.google.common.base.Preconditions;
import java.net.Socket;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.util.Strings;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

public class KafkaContainer extends BaseContainer {

  public static final int DEFAULT_BROKER_PORT = 9092;
  private static final String DEFAULT_KAFKA_IMAGE =
      System.getenv("GRAVITINO_CI_KAFKA_DOCKER_IMAGE");
  private static final String DEFAULT_HOST_NAME = "gravitino-ci-kafka";
  private static final String KAFKA_LOGS_DIR = "/opt/kafka/logs";

  protected KafkaContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void start() {
    try {
      super.start();
      Preconditions.checkArgument(checkContainerStatus(5), "Kafka container startup failed!");
    } finally {
      copyKafkaLogs();
    }
  }

  private void copyKafkaLogs() {
    try {
      String destPath = System.getenv("IT_PROJECT_DIR");
      LOG.info("Copy Kafka logs file from {} to {}", KAFKA_LOGS_DIR, destPath);

      String kafkaLogJarPath = "/home/appuser/kafka-logs.tar";

      GenericContainer<?> kafkaContainer = getContainer();
      kafkaContainer.execInContainer("tar", "cf", kafkaLogJarPath, KAFKA_LOGS_DIR);
      kafkaContainer.copyFileFromContainer(kafkaLogJarPath, destPath + "/kafka-logs.tar");
    } catch (Exception e) {
      LOG.error("Failed to package Kafka logs", e);
    }
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    String address = getContainerIpAddress();
    Preconditions.checkArgument(
        Strings.isNotBlank(address), "Kafka container IP address is not available.");
    String broker = String.format("%s:%d", address, DEFAULT_BROKER_PORT);

    await()
        .atMost(1, TimeUnit.MINUTES)
        .pollInterval(60 / retryLimit, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                Container.ExecResult result =
                    executeInContainer(
                        "sh",
                        "/opt/kafka/bin/kafka-cluster.sh",
                        "cluster-id",
                        "--bootstrap-server",
                        broker);
                if (result.getStdout().startsWith("Cluster ID:")) {
                  LOG.info("Kafka server has started." + result.getStdout());
                  return true;
                }
              } catch (Exception ex) {
                LOG.warn(
                    "Could not connect to Kafka server[{}:{}]", address, DEFAULT_BROKER_PORT, ex);
              }
              return false;
            });

    await()
        .atMost(30, TimeUnit.SECONDS)
        .until(
            () -> {
              try (Socket socket = new Socket()) {
                socket.connect(
                    new java.net.InetSocketAddress(getContainerIpAddress(), DEFAULT_BROKER_PORT),
                    3000);
              } catch (Exception ex) {
                LOG.warn(
                    "Could not connect to Kafka server[{}:{}]", address, DEFAULT_BROKER_PORT, ex);
                return false;
              }
              return true;
            });

    return true;
  }

  public static class Builder extends BaseContainer.Builder<Builder, KafkaContainer> {
    private Builder() {
      this.image = DEFAULT_KAFKA_IMAGE;
      this.hostName = DEFAULT_HOST_NAME;
      this.exposePorts = ImmutableSet.of(DEFAULT_BROKER_PORT);
      this.envVars =
          ImmutableMap.<String, String>builder()
              .put("KAFKA_PROCESS_ROLES", "broker,controller")
              .put("KAFKA_NODE_ID", "1")
              .put("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@" + hostName + ":9093")
              .put("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
              .put("KAFKA_LISTENERS", "PLAINTEXT://:" + DEFAULT_BROKER_PORT + ",CONTROLLER://:9093")
              .put("DEFAULT_BROKER_PORT", String.valueOf(DEFAULT_BROKER_PORT))
              .put("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
              .build();

      ClassLoader classLoader = getClass().getClassLoader();
      URL resource = classLoader.getResource("run");
      String filePath;
      try {
        filePath = Paths.get(resource.toURI()).toFile().getAbsolutePath();
      } catch (URISyntaxException e) {
        throw new RuntimeException("Could not find file", e);
      }
      this.filesToMount = ImmutableMap.of("/etc/kafka/docker/run", filePath);
    }

    @Override
    public KafkaContainer build() {
      return new KafkaContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
