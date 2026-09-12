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

import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.cache.CacheFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;

/**
 * Runs {@link RedisEntityCacheTestBase} against a standalone Redis. Uses the server named by the
 * {@code GRAVITINO_REDIS_ADDRESS} environment variable when set, otherwise starts a Redis
 * container.
 */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RedisEntityCacheIT extends RedisEntityCacheTestBase {

  static final String ADDRESS_ENV = "GRAVITINO_REDIS_ADDRESS";
  private static final String IMAGE = "redis:7.2-alpine";
  private static final int PORT = 6379;

  private GenericContainer<?> container;
  private String address;
  private UnifiedJedis rawClient;

  @BeforeAll
  void startRedis() {
    String external = System.getenv(ADDRESS_ENV);
    if (StringUtils.isNotBlank(external)) {
      address = external;
    } else {
      container = new GenericContainer<>(DockerImageName.parse(IMAGE)).withExposedPorts(PORT);
      container.start();
      address = container.getHost() + ":" + container.getMappedPort(PORT);
    }
    rawClient = new JedisPooled(HostAndPort.from(address));
  }

  @AfterAll
  void stopRedis() {
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
    return false;
  }

  @Override
  protected UnifiedJedis rawClient() {
    return rawClient;
  }

  @Test
  void testStartupFailsFastWhenRedisIsUnreachable() {
    Config config = config(60_000L);
    config.set(Configs.CACHE_REDIS_ADDRESS, "127.0.0.1:1");
    config.set(Configs.CACHE_REDIS_TIMEOUT_MS, 500);
    RuntimeException e =
        Assertions.assertThrows(RuntimeException.class, () -> CacheFactory.getEntityCache(config));
    Assertions.assertInstanceOf(IllegalStateException.class, e.getCause().getCause());
  }
}
