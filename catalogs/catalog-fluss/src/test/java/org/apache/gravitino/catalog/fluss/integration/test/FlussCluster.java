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

package org.apache.gravitino.catalog.fluss.integration.test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.fluss.FlussCatalogOperations;
import org.apache.gravitino.rest.RESTUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

class FlussCluster implements AutoCloseable {

  static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

  private static final String FLUSS_IMAGE =
      System.getenv()
          .getOrDefault("GRAVITINO_CI_FLUSS_DOCKER_IMAGE", "apache/fluss:0.9.1-incubating");
  private static final String ZOOKEEPER_IMAGE =
      System.getenv().getOrDefault("GRAVITINO_CI_FLUSS_ZOOKEEPER_DOCKER_IMAGE", "zookeeper:3.9.2");
  private static final String ZOOKEEPER_HOST = "zookeeper";
  private static final String COORDINATOR_HOST = "coordinator-server";
  private static final String TABLET_HOST = "tablet-server";
  private static final int ZOOKEEPER_PORT = 2181;
  private static final int FLUSS_CLIENT_PORT = 9123;
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration READY_TIMEOUT = Duration.ofMinutes(2);

  private final Network network;
  private final GenericContainer<?> zookeeper;
  private final FixedPortContainer coordinatorServer;
  private final FixedPortContainer tabletServer;
  private final int coordinatorPort;

  private FlussCluster(
      Network network,
      GenericContainer<?> zookeeper,
      FixedPortContainer coordinatorServer,
      FixedPortContainer tabletServer,
      int coordinatorPort) {
    this.network = network;
    this.zookeeper = zookeeper;
    this.coordinatorServer = coordinatorServer;
    this.tabletServer = tabletServer;
    this.coordinatorPort = coordinatorPort;
  }

  static FlussCluster start() throws IOException {
    Network network = Network.newNetwork();
    int coordinatorPort = findAvailablePort();
    int tabletPort = findAvailablePort(coordinatorPort);

    GenericContainer<?> zookeeper =
        new GenericContainer<>(ZOOKEEPER_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(ZOOKEEPER_HOST)
            .withExposedPorts(ZOOKEEPER_PORT)
            .waitingFor(Wait.forListeningPort())
            .withStartupTimeout(STARTUP_TIMEOUT);

    FixedPortContainer coordinatorServer =
        new FixedPortContainer(FLUSS_IMAGE)
            .withFixedPort(coordinatorPort)
            .withNetwork(network)
            .withNetworkAliases(COORDINATOR_HOST)
            .withCommand("coordinatorServer")
            .withEnv("FLUSS_PROPERTIES", coordinatorProperties(coordinatorPort))
            .waitingFor(Wait.forListeningPort())
            .withStartupTimeout(STARTUP_TIMEOUT);

    FixedPortContainer tabletServer =
        new FixedPortContainer(FLUSS_IMAGE)
            .withFixedPort(tabletPort)
            .withNetwork(network)
            .withNetworkAliases(TABLET_HOST)
            .withCommand("tabletServer")
            .withEnv("FLUSS_PROPERTIES", tabletProperties(tabletPort))
            .waitingFor(Wait.forListeningPort())
            .withStartupTimeout(STARTUP_TIMEOUT);

    FlussCluster cluster =
        new FlussCluster(network, zookeeper, coordinatorServer, tabletServer, coordinatorPort);
    cluster.startContainers();
    cluster.waitUntilReady();
    return cluster;
  }

  String bootstrapServers() {
    return "localhost:" + coordinatorPort;
  }

  @Override
  public void close() {
    tabletServer.stop();
    coordinatorServer.stop();
    zookeeper.stop();
    network.close();
  }

  private void startContainers() {
    zookeeper.start();
    coordinatorServer.start();
    tabletServer.start();
  }

  private void waitUntilReady() {
    long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
    Exception last = null;
    while (System.nanoTime() < deadline) {
      try (FlussCatalogOperations ops = new FlussCatalogOperations()) {
        ops.initialize(Map.of(BOOTSTRAP_SERVERS, bootstrapServers()), null, null);
        ops.testConnection(
            NameIdentifier.of("metalake", "catalog"),
            Catalog.Type.RELATIONAL,
            "fluss",
            null,
            Map.of());
        return;
      } catch (Exception e) {
        last = e;
        sleepQuietly();
      }
    }
    throw new IllegalStateException("Timed out waiting for Fluss cluster to become ready", last);
  }

  private static String coordinatorProperties(int advertisedPort) {
    return String.join(
        "\n",
        List.of(
            "zookeeper.address: " + ZOOKEEPER_HOST + ":" + ZOOKEEPER_PORT,
            "bind.listeners: INTERNAL://"
                + COORDINATOR_HOST
                + ":0, CLIENT://"
                + COORDINATOR_HOST
                + ":"
                + FLUSS_CLIENT_PORT,
            "advertised.listeners: CLIENT://localhost:" + advertisedPort,
            "internal.listener.name: INTERNAL",
            "remote.data.dir: /tmp/fluss/remote-data"));
  }

  private static String tabletProperties(int advertisedPort) {
    return String.join(
        "\n",
        List.of(
            "zookeeper.address: " + ZOOKEEPER_HOST + ":" + ZOOKEEPER_PORT,
            "bind.listeners: INTERNAL://"
                + TABLET_HOST
                + ":0, CLIENT://"
                + TABLET_HOST
                + ":"
                + FLUSS_CLIENT_PORT,
            "advertised.listeners: CLIENT://localhost:" + advertisedPort,
            "internal.listener.name: INTERNAL",
            "tablet-server.id: 0",
            "kv.snapshot.interval: 0s",
            "data.dir: /tmp/fluss/data",
            "remote.data.dir: /tmp/fluss/remote-data"));
  }

  private static int findAvailablePort(int... excludedPorts) throws IOException {
    int port;
    do {
      port = RESTUtils.findAvailablePort(20000, 40000);
    } while (contains(excludedPorts, port));
    return port;
  }

  private static boolean contains(int[] values, int value) {
    for (int item : values) {
      if (item == value) {
        return true;
      }
    }
    return false;
  }

  private static void sleepQuietly() {
    try {
      Thread.sleep(1000L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for Fluss cluster", e);
    }
  }

  private static final class FixedPortContainer extends GenericContainer<FixedPortContainer> {

    private FixedPortContainer(String image) {
      super(image);
    }

    private FixedPortContainer withFixedPort(int hostPort) {
      addFixedExposedPort(hostPort, FLUSS_CLIENT_PORT);
      return this;
    }
  }
}
