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
package org.apache.gravitino.cache.integration.test;

import com.github.dockerjava.api.model.ContainerNetwork;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cache.RedisEntityCache;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Runs {@link RedisEntityCacheTestBase} against a Redis Cluster, where every multi-key script must
 * stay inside one hash slot and node-wide operations must tell primaries from replicas.
 *
 * <p>Uses the seed nodes named by the {@code GRAVITINO_REDIS_CLUSTER_ADDRESS} environment variable
 * when set. Otherwise it starts a three-primary, three-replica cluster container and connects to
 * the container's own address: the nodes announce that address to clients, so the cluster only
 * works from a host that can route to it, which a Linux Docker host (the CI runners) can and Docker
 * Desktop cannot. On a host that cannot reach the container the suite is skipped, with the reason.
 */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RedisClusterEntityCacheIT extends RedisEntityCacheTestBase {

  static final String ADDRESS_ENV = "GRAVITINO_REDIS_CLUSTER_ADDRESS";
  private static final String IMAGE = "grokzen/redis-cluster:7.0.10";
  private static final int FIRST_PORT = 7000;
  private static final int MASTERS = 3;
  private static final int REPLICAS_PER_MASTER = 1;
  private static final int NODES = MASTERS * (1 + REPLICAS_PER_MASTER);

  private GenericContainer<?> container;
  private String address;
  private JedisCluster rawClient;

  @BeforeAll
  void startCluster() {
    String external = System.getenv(ADDRESS_ENV);
    int expectedNodes = MASTERS;
    if (StringUtils.isNotBlank(external)) {
      address = external;
    } else {
      container =
          new GenericContainer<>(DockerImageName.parse(IMAGE))
              .withExposedPorts(FIRST_PORT)
              .withEnv("INITIAL_PORT", String.valueOf(FIRST_PORT))
              .withEnv("MASTERS", String.valueOf(MASTERS))
              .withEnv("SLAVES_PER_MASTER", String.valueOf(REPLICAS_PER_MASTER));
      container.start();
      String containerIp =
          container.getContainerInfo().getNetworkSettings().getNetworks().values().stream()
              .map(ContainerNetwork::getIpAddress)
              .filter(StringUtils::isNotBlank)
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("the cluster container has no IP"));
      Assumptions.assumeTrue(
          reachable(containerIp, FIRST_PORT, 30_000),
          "The Redis Cluster container at "
              + containerIp
              + " is not routable from this host (typical of Docker Desktop); set "
              + ADDRESS_ENV
              + " to a reachable cluster to run this suite here.");
      address =
          IntStream.range(FIRST_PORT, FIRST_PORT + MASTERS)
              .mapToObj(port -> containerIp + ":" + port)
              .collect(Collectors.joining(","));
      expectedNodes = NODES;
    }
    Set<HostAndPort> seeds =
        Arrays.stream(address.split(","))
            .map(String::trim)
            .map(HostAndPort::from)
            .collect(Collectors.toSet());
    int nodesWanted = expectedNodes;
    try {
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollInterval(1, TimeUnit.SECONDS)
          // Subclasses too: a cluster still forming throws JedisClusterOperationException.
          .ignoreExceptionsInstanceOf(JedisException.class)
          .until(
              () -> {
                try (JedisCluster probe = new JedisCluster(seeds)) {
                  // Replicas join the slot map a moment after the cluster reports itself ready.
                  return probe.getClusterNodes().size() >= nodesWanted
                      && !probe.exists("gravitino:{probe}:F:_");
                }
              });
    } catch (RuntimeException e) {
      String logs = container == null ? "(external cluster)" : container.getLogs();
      throw new IllegalStateException(
          "Redis Cluster at " + address + " did not become ready. Container logs:\n" + logs, e);
    }
    rawClient = new JedisCluster(seeds);
  }

  @AfterAll
  void stopCluster() {
    if (rawClient != null) {
      rawClient.close();
    }
    if (container != null) {
      container.stop();
    }
  }

  @Override
  protected String address() {
    return address;
  }

  @Override
  protected boolean cluster() {
    return true;
  }

  @Override
  protected UnifiedJedis rawClient() {
    return rawClient;
  }

  /** Whether a TCP connection to the address succeeds within the timeout, retrying meanwhile. */
  private static boolean reachable(String host, int port, long timeoutMs) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    do {
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(host, port), 2_000);
        return true;
      } catch (IOException e) {
        try {
          Thread.sleep(1_000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    } while (System.currentTimeMillis() < deadline);
    return false;
  }

  @Test
  void testClusterHasReplicasAndNodeWideOperationsSkipThem() {
    // With replicas present, a node-wide scan that treated them as primaries would either count
    // the same index twice or be redirected; size and clear must see each index exactly once.
    int replicas = 0;
    for (ConnectionPool pool : rawClient.getClusterNodes().values()) {
      try (Jedis node = new Jedis(pool.getResource())) {
        if (node.info("replication").contains("role:slave")) {
          replicas++;
        }
      }
    }
    if (container != null) {
      Assertions.assertEquals(
          MASTERS * REPLICAS_PER_MASTER, replicas, "the container cluster must have replicas");
    }
    Assertions.assertTrue(
        rawClient.getClusterNodes().size() >= MASTERS + replicas, "client must see every node");
    RedisEntityCache cache = newNode();
    for (int m = 0; m < 12; m++) {
      load(cache, metalake("metalake" + m));
      load(cache, catalog("metalake" + m, "c1"));
    }
    Assertions.assertEquals(24, cache.size());
    cache.clear();
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void testDropsAcrossManyMetalakesNeverCrossSlots() {
    RedisEntityCache cache = newNode();
    int metalakes = 40;
    for (int m = 0; m < metalakes; m++) {
      String metalake = "metalake" + m;
      load(cache, metalake(metalake));
      load(cache, catalog(metalake, "c1"));
      load(cache, schema(metalake, "c1", "s1"));
      load(cache, schema(metalake, "c1", "s1" + SEP + "nested"));
      load(cache, table(metalake, "c1", "s1", "t1", "v1"));
      load(cache, table(metalake, "c1", "s1" + SEP + "nested", "t2", "v1"));
    }
    Assertions.assertEquals(metalakes * 6, cache.size());

    // Metalakes spread over the cluster's slots; every drop is a multi-key script over one slot.
    for (int m = 0; m < metalakes; m++) {
      String metalake = "metalake" + m;
      cache.invalidate(NameIdentifier.of(metalake, "c1", "s1"), Entity.EntityType.SCHEMA);
      Assertions.assertFalse(
          cache.contains(
              NameIdentifier.of(metalake, "c1", "s1" + SEP + "nested", "t2"),
              Entity.EntityType.TABLE));
      cache.invalidate(NameIdentifier.of(metalake), Entity.EntityType.METALAKE);
    }
    Assertions.assertEquals(0, cache.size());
  }
}
