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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.logging.log4j.util.Strings;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.DockerImageName;

/**
 * A single KRaft broker on the official {@code org.testcontainers.kafka.KafkaContainer} module,
 * which runs the {@code apache/kafka} image that {@link TestImages#KAFKA} names. The module owns
 * the listener layout; the mounted {@code run} script only advertises the container IP on {@link
 * #DEFAULT_BROKER_PORT}, which callers use as the bootstrap address.
 */
public class KafkaContainer extends BaseContainer {

  public static final int DEFAULT_BROKER_PORT = 9092;
  private static final String DEFAULT_KAFKA_IMAGE = TestImages.KAFKA;
  private static final String DEFAULT_HOST_NAME = "gravitino-ci-kafka";
  private static final String KAFKA_LOGS_DIR = "/opt/kafka/logs";
  private static final short API_VERSIONS_KEY = 18;
  private static final int PROBE_CORRELATION_ID = 0x6772;
  // The module's own readiness line: the broker has left recovery and is unfenced.
  private static final String RUNNING_LOG = ".*Transitioning from RECOVERY to RUNNING.*";

  protected KafkaContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(module(image), hostName, ports, extraHosts, filesToMount, envVars, network);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void close() {
    copyKafkaLogs();
    super.close();
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

  // The module waits for its log line, which the broker writes just before it serves requests; the
  // ApiVersions request checks that it does.
  @Override
  protected WaitStrategy waitStrategy() {
    return new WaitAllStrategy()
        .withStrategy(Wait.forLogMessage(RUNNING_LOG, 1))
        .withStrategy(
            new ProbeWaitStrategy(
                "Kafka to answer an ApiVersions request",
                target -> {
                  String address = ipAddress(target.getContainerInfo());
                  Preconditions.checkArgument(
                      Strings.isNotBlank(address), "Kafka container IP address is not available.");
                  return answersApiVersions(address, DEFAULT_BROKER_PORT);
                }));
  }

  // The log line's five minutes plus the minute the ApiVersions check had before.
  @Override
  protected Duration startupTimeout() {
    return Duration.ofMinutes(6);
  }

  /**
   * Sends a Kafka {@code ApiVersions} v0 request and returns whether the broker answers it. The
   * port accepts connections while the broker still starts; it answers requests only once it serves
   * clients.
   */
  static boolean answersApiVersions(String host, int port) {
    byte[] clientId = "gravitino-it".getBytes(StandardCharsets.UTF_8);
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, port), 3000);
      socket.setSoTimeout(3000);
      DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      // Request header v1: api key, api version, correlation id, client id.
      out.writeInt(2 + 2 + 4 + 2 + clientId.length);
      out.writeShort(API_VERSIONS_KEY);
      out.writeShort(0);
      out.writeInt(PROBE_CORRELATION_ID);
      out.writeShort(clientId.length);
      out.write(clientId);
      out.flush();
      DataInputStream in = new DataInputStream(socket.getInputStream());
      return in.readInt() >= 4 && in.readInt() == PROBE_CORRELATION_ID;
    } catch (IOException e) {
      LOG.info("Kafka server {}:{} is not ready yet: {}", host, port, e.getMessage());
      return false;
    }
  }

  private static org.testcontainers.kafka.KafkaContainer module(String image) {
    // The image may be a mirror or a custom tag; the module only needs an apache/kafka image.
    // Its environment holds the KRaft layout: PLAINTEXT on 9092, the inter-broker listener on
    // 9093 and the controller on 9094.
    return new org.testcontainers.kafka.KafkaContainer(
        DockerImageName.parse(image).asCompatibleSubstituteFor("apache/kafka"));
  }

  public static class Builder extends BaseContainer.Builder<Builder, KafkaContainer> {
    private Builder() {
      this.image = DEFAULT_KAFKA_IMAGE;
      this.hostName = DEFAULT_HOST_NAME;
      this.exposePorts = ImmutableSet.of(DEFAULT_BROKER_PORT);
      // The run script advertises PLAINTEXT on this port.
      this.envVars = ImmutableMap.of("DEFAULT_BROKER_PORT", String.valueOf(DEFAULT_BROKER_PORT));

      String rootDir = System.getenv("GRAVITINO_ROOT_DIR");
      Preconditions.checkArgument(rootDir != null, "GRAVITINO_ROOT_DIR is not set");
      String initScript =
          ITUtils.joinPath(rootDir, "integration-test-common", "docker-script/init/kafka/run");
      String filePath;
      filePath = Paths.get(initScript).toAbsolutePath().toString();
      this.filesToMount = ImmutableMap.of("/etc/kafka/docker/run", filePath);
    }

    @Override
    public KafkaContainer build() {
      return new KafkaContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
