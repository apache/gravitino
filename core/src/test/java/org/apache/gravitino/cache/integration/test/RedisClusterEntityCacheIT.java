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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Runs {@link RedisEntityCacheTestBase} against a Redis Cluster, where every multi-key script must
 * stay inside one hash slot. Uses the seed nodes named by the {@code
 * GRAVITINO_REDIS_CLUSTER_ADDRESS} environment variable when set, otherwise starts a three-master
 * cluster container on the host network, which is why the container path needs Linux.
 */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RedisClusterEntityCacheIT extends RedisEntityCacheTestBase {

  static final String ADDRESS_ENV = "GRAVITINO_REDIS_CLUSTER_ADDRESS";
  private static final String IMAGE = "grokzen/redis-cluster:7.0.10";
  private static final int FIRST_PORT = 7000;
  private static final int MASTERS = 3;

  private GenericContainer<?> container;
  private String address;
  private JedisCluster rawClient;

  @BeforeAll
  void startCluster() {
    String external = System.getenv(ADDRESS_ENV);
    if (StringUtils.isNotBlank(external)) {
      address = external;
    } else {
      container =
          new GenericContainer<>(DockerImageName.parse(IMAGE))
              .withNetworkMode("host")
              .withEnv("IP", "127.0.0.1")
              .withEnv("INITIAL_PORT", String.valueOf(FIRST_PORT))
              .withEnv("MASTERS", String.valueOf(MASTERS))
              .withEnv("SLAVES_PER_MASTER", "0");
      container.start();
      address =
          IntStream.range(FIRST_PORT, FIRST_PORT + MASTERS)
              .mapToObj(port -> "127.0.0.1:" + port)
              .collect(Collectors.joining(","));
    }
    Set<HostAndPort> seeds =
        Arrays.stream(address.split(","))
            .map(String::trim)
            .map(HostAndPort::from)
            .collect(Collectors.toSet());
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(1, TimeUnit.SECONDS)
        .ignoreException(JedisException.class)
        .until(
            () -> {
              try (JedisCluster probe = new JedisCluster(seeds)) {
                return probe.getClusterNodes().size() >= MASTERS
                    && !probe.exists("gravitino:{probe}:F:_");
              }
            });
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

  @Test
  void testDropsAcrossManyMetalakesNeverCrossSlots() {
    RedisEntityCache cache = newNode();
    int metalakes = 40;
    for (int m = 0; m < metalakes; m++) {
      String metalake = "metalake" + m;
      cache.put(metalake(metalake));
      cache.put(catalog(metalake, "c1"));
      cache.put(schema(metalake, "c1", "s1"));
      cache.put(schema(metalake, "c1", "s1" + SEP + "nested"));
      cache.put(table(metalake, "c1", "s1", "t1", "v1"));
      cache.put(table(metalake, "c1", "s1" + SEP + "nested", "t2", "v1"));
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
