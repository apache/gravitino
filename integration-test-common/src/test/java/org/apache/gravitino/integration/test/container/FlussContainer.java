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

import static java.lang.String.format;

import com.github.dockerjava.api.model.ContainerNetwork;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

/** Test container facade for a single-node Apache Fluss cluster */
public class FlussContainer extends BaseContainer {
  private static final Logger LOG = LoggerFactory.getLogger(FlussContainer.class);

  private static final String DEFAULT_FLUSS_IMAGE = "apache/fluss:0.9.1-incubating";
  private static final String DEFAULT_ZOOKEEPER_IMAGE = "zookeeper:3.8.3";
  private static final String FLUSS_IMAGE_ENV = "GRAVITINO_CI_FLUSS_DOCKER_IMAGE";
  private static final String ZOOKEEPER_IMAGE_ENV = "GRAVITINO_CI_FLUSS_ZOOKEEPER_DOCKER_IMAGE";
  private static final String HOST_NAME = "gravitino-ci-fluss";
  private static final String ZOOKEEPER_HOST_SUFFIX = "-zookeeper";
  private static final String COORDINATOR_HOST_SUFFIX = "-coordinator";
  private static final String TABLET_HOST_SUFFIX = "-tablet";
  private static final String FLUSS_LISTENER_NAME = "FLUSS";

  private static final String SASL_PROTOCOL = "sasl";
  private static final String SERVER_SASL_MECHANISM = "plain";
  private static final String CLIENT_SASL_MECHANISM = "PLAIN";
  public static final String AUTHENTICATED_FLUSS_HOST_NAME = "gravitino-ci-fluss-auth";
  /** Username used by the authenticated Fluss test cluster. */
  public static final String FLUSS_AUTH_USERNAME = "gravitino";
  /** Password used by the authenticated Fluss test cluster. */
  public static final String FLUSS_AUTH_PASSWORD = "gravitino-password";

  private static final int ZOOKEEPER_PORT = 2181;
  private static final int CLIENT_PORT = 9123;
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration READY_TIMEOUT = Duration.ofMinutes(2);
  private static final Duration SOCKET_CONNECT_TIMEOUT = Duration.ofSeconds(3);
  private static final long READY_CHECK_INTERVAL_MILLIS = 1000L;

  private final String flussImage;
  private final String zookeeperImage;
  private final Network clusterNetwork;
  private final boolean ownNetwork;
  private final Optional<SaslPlainAuth> saslPlainAuth;
  private final String zookeeperHost;
  private final String coordinatorHost;
  private final String tabletHost;

  private GenericContainer<?> zookeeper;
  private GenericContainer<?> coordinatorServer;
  private GenericContainer<?> tabletServer;
  private boolean networkClosed;

  /**
   * Creates a Fluss cluster facade.
   *
   * @param image Fluss Docker image.
   * @param hostName unused facade host name required by {@link BaseContainer}.
   * @param ports unused facade exposed ports required by {@link BaseContainer}.
   * @param extraHosts unused facade extra hosts required by {@link BaseContainer}.
   * @param filesToMount unused facade mounts required by {@link BaseContainer}.
   * @param envVars unused facade env vars required by {@link BaseContainer}.
   * @param network shared Docker network, or empty to create a private Fluss network.
   * @param zookeeperImage ZooKeeper Docker image.
   * @param saslPlainAuth optional SASL/PLAIN authentication settings.
   */
  protected FlussContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network,
      String zookeeperImage,
      Optional<SaslPlainAuth> saslPlainAuth) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network);
    this.flussImage = image;
    this.zookeeperImage = zookeeperImage;
    this.saslPlainAuth = saslPlainAuth;
    this.zookeeperHost = hostName + ZOOKEEPER_HOST_SUFFIX;
    this.coordinatorHost = hostName + COORDINATOR_HOST_SUFFIX;
    this.tabletHost = hostName + TABLET_HOST_SUFFIX;
    if (network.isPresent()) {
      this.clusterNetwork = network.get();
      this.ownNetwork = false;
    } else {
      this.clusterNetwork = Network.newNetwork();
      this.ownNetwork = true;
    }
  }

  /** Creates a Fluss cluster builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Starts the Fluss cluster. */
  @Override
  public void start() {
    try {
      zookeeper = createZooKeeper();
      coordinatorServer =
          createFlussServer(
              coordinatorHost, "coordinatorServer", coordinatorProperties(), "FlussCoordinator");
      tabletServer =
          createFlussServer(tabletHost, "tabletServer", tabletProperties(), "FlussTablet");

      zookeeper.start();
      coordinatorServer.start();
      tabletServer.start();
      Preconditions.checkArgument(checkContainerStatus(5), "Fluss container startup failed!");
    } catch (Exception e) {
      close();
      throw new RuntimeException("Failed to start Fluss container", e);
    }
  }

  /** Returns the bootstrap servers that clients can use to connect to Fluss. */
  public String bootstrapServers() {
    return format("%s:%d", getContainerIpAddress(), CLIENT_PORT);
  }

  /** Returns the host-reachable Fluss coordinator address. */
  @Override
  public String getContainerIpAddress() {
    Preconditions.checkArgument(
        coordinatorServer != null, "Fluss coordinator server is not started");
    return getContainerIpAddress(coordinatorServer);
  }

  /** Returns the host-mapped port of the Fluss coordinator. */
  @Override
  public Integer getMappedPort(int exposedPort) {
    Preconditions.checkArgument(
        coordinatorServer != null, "Fluss coordinator server is not started");
    return coordinatorServer.getMappedPort(exposedPort);
  }

  /** Stops the Fluss cluster. */
  @Override
  public void close() {
    if (tabletServer != null) {
      tabletServer.stop();
      tabletServer = null;
    }
    if (coordinatorServer != null) {
      coordinatorServer.stop();
      coordinatorServer = null;
    }
    if (zookeeper != null) {
      zookeeper.stop();
      zookeeper = null;
    }
    if (ownNetwork && !networkClosed) {
      clusterNetwork.close();
      networkClosed = true;
    }
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      if (isListening(coordinatorServer) && isListening(tabletServer)) {
        return true;
      }
      sleepQuietly();
    }

    LOG.warn("Timed out waiting for Fluss cluster to become ready");
    return false;
  }

  private GenericContainer<?> createZooKeeper() {
    return new GenericContainer<>(zookeeperImage)
        .withNetwork(clusterNetwork)
        .withNetworkAliases(zookeeperHost)
        .withCreateContainerCmdModifier(cmd -> cmd.withHostName(zookeeperHost))
        .withExposedPorts(ZOOKEEPER_PORT)
        .withLogConsumer(new PrintingContainerLog(format("%-20s| ", "FlussZooKeeper")))
        .waitingFor(Wait.forListeningPort())
        .withStartupTimeout(STARTUP_TIMEOUT);
  }

  private GenericContainer<?> createFlussServer(
      String hostName, String command, String properties, String logName) {
    return new GenericContainer<>(flussImage)
        .withNetwork(clusterNetwork)
        .withNetworkAliases(hostName)
        .withCreateContainerCmdModifier(cmd -> cmd.withHostName(hostName))
        .withExposedPorts(CLIENT_PORT)
        .withCommand(command)
        .withEnv("FLUSS_PROPERTIES", properties)
        .withLogConsumer(new PrintingContainerLog(format("%-20s| ", logName)))
        .waitingFor(Wait.forListeningPort())
        .withStartupTimeout(STARTUP_TIMEOUT);
  }

  private String coordinatorProperties() {
    List<String> properties = new ArrayList<>();
    properties.add("zookeeper.address: " + zookeeperHost + ":" + ZOOKEEPER_PORT);
    properties.add("bind.listeners: " + FLUSS_LISTENER_NAME + "://${HOSTNAME}:" + CLIENT_PORT);
    properties.add("remote.data.dir: /tmp/fluss/remote-data");
    addSaslPlainAuthProperties(properties);
    return String.join("\n", properties);
  }

  private String tabletProperties() {
    List<String> properties = new ArrayList<>();
    properties.add("zookeeper.address: " + zookeeperHost + ":" + ZOOKEEPER_PORT);
    properties.add("bind.listeners: " + FLUSS_LISTENER_NAME + "://${HOSTNAME}:" + CLIENT_PORT);
    properties.add("tablet-server.id: 0");
    properties.add("kv.snapshot.interval: 0s");
    properties.add("data.dir: /tmp/fluss/data");
    properties.add("remote.data.dir: /tmp/fluss/remote-data");
    addSaslPlainAuthProperties(properties);
    return String.join("\n", properties);
  }

  private void addSaslPlainAuthProperties(List<String> properties) {
    if (saslPlainAuth.isEmpty()) {
      return;
    }

    SaslPlainAuth auth = saslPlainAuth.get();
    properties.add("authorizer.enabled: true");
    properties.add("authorizer.type: default");
    properties.add("super.users: User:" + auth.username);
    properties.add("security.protocol.map: " + FLUSS_LISTENER_NAME + ":" + SASL_PROTOCOL);
    properties.add("security.sasl.enabled.mechanisms: " + SERVER_SASL_MECHANISM);
    properties.add("security.sasl.plain.jaas.config: " + auth.plainJaasConfig());
    properties.add("client.security.protocol: " + SASL_PROTOCOL);
    properties.add("client.security.sasl.mechanism: " + CLIENT_SASL_MECHANISM);
    properties.add("client.security.sasl.username: " + auth.username);
    properties.add("client.security.sasl.password: " + auth.password);
  }

  private static boolean isListening(GenericContainer<?> container) {
    String address = getContainerIpAddress(container);
    try (Socket socket = new Socket()) {
      socket.connect(
          new InetSocketAddress(address, CLIENT_PORT),
          Math.toIntExact(SOCKET_CONNECT_TIMEOUT.toMillis()));
      return true;
    } catch (Exception e) {
      LOG.info(
          "Fluss container is not reachable at {}:{} yet: {}",
          address,
          CLIENT_PORT,
          e.getMessage());
      return false;
    }
  }

  private static String getContainerIpAddress(GenericContainer<?> container) {
    Map<String, ContainerNetwork> containerNetworkMap =
        container.getContainerInfo().getNetworkSettings().getNetworks();
    Preconditions.checkArgument(
        containerNetworkMap.size() == 1, "Fluss container should use exactly one Docker network");
    return containerNetworkMap.values().iterator().next().getIpAddress();
  }

  private static void sleepQuietly() {
    try {
      Thread.sleep(READY_CHECK_INTERVAL_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for Fluss container", e);
    }
  }

  private static final class SaslPlainAuth {
    private final String username;
    private final String password;

    private SaslPlainAuth(String username, String password) {
      Preconditions.checkArgument(username != null && !username.isEmpty(), "username is empty");
      Preconditions.checkArgument(password != null && !password.isEmpty(), "password is empty");
      Preconditions.checkArgument(
          !username.contains("\"") && !username.contains("\n"),
          "username must not contain quotes or newlines");
      Preconditions.checkArgument(
          !password.contains("\"") && !password.contains("\n"),
          "password must not contain quotes or newlines");
      this.username = username;
      this.password = password;
    }

    private String plainJaasConfig() {
      return format(
          "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required user_%s=\"%s\";",
          username, password);
    }
  }

  /** Builder for {@link FlussContainer}. */
  public static class Builder extends BaseContainer.Builder<Builder, FlussContainer> {
    private String zookeeperImage =
        System.getenv().getOrDefault(ZOOKEEPER_IMAGE_ENV, DEFAULT_ZOOKEEPER_IMAGE);
    private Optional<SaslPlainAuth> saslPlainAuth = Optional.empty();

    private Builder() {
      this.image = System.getenv().getOrDefault(FLUSS_IMAGE_ENV, DEFAULT_FLUSS_IMAGE);
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(CLIENT_PORT);
    }

    /** Uses the given ZooKeeper Docker image. */
    public Builder withZooKeeperImage(String zookeeperImage) {
      this.zookeeperImage = zookeeperImage;
      return this;
    }

    /** Enables SASL/PLAIN authentication for the Fluss cluster. */
    public Builder withSaslPlainAuth(String username, String password) {
      this.saslPlainAuth = Optional.of(new SaslPlainAuth(username, password));
      return this;
    }

    /** Builds a Fluss cluster facade. */
    @Override
    public FlussContainer build() {
      return new FlussContainer(
          image,
          hostName,
          exposePorts,
          extraHosts,
          filesToMount,
          envVars,
          network,
          zookeeperImage,
          saslPlainAuth);
    }
  }
}
