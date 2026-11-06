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
package org.apache.gravitino.cache;

import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.SetParams;

/**
 * Exercises the Lua scripts of {@link RedisEntityCache} one at a time against a real Redis, at
 * interleavings the cache API cannot produce on demand: a fill racing an invalidation, a discard
 * arriving after a concurrent fill, an invalidation of a container with many cached descendants,
 * and memory pressure under an eviction policy.
 */
@Tag("gravitino-docker-test")
public class TestRedisEntityCacheScripts {

  private static final String IMAGE = "redis:7.2-alpine";
  private static final int PORT = 6379;

  private static GenericContainer<?> container;
  private static JedisPooled jedis;

  private String prefix;

  @BeforeAll
  static void startRedis() {
    container = new GenericContainer<>(DockerImageName.parse(IMAGE)).withExposedPorts(PORT);
    container.start();
    jedis = connect(container);
  }

  @AfterAll
  static void stopRedis() {
    if (jedis != null) {
      jedis.close();
    }
    if (container != null) {
      container.stop();
    }
  }

  @BeforeEach
  void newSlot() {
    prefix = "scripts-" + UUID.randomUUID().toString().substring(0, 8) + ":{m1}:";
  }

  @Test
  void testFillThenReadServesTheValueUnderAnUnchangedStamp() {
    List<?> miss = read(jedis, "m1.c1:CATALOG", "m1", "m1.c1");
    Assertions.assertEquals(0L, miss.get(0));
    Assertions.assertEquals("0,0", utf8((byte[]) miss.get(1)));

    Assertions.assertEquals(1L, put(jedis, "m1.c1:CATALOG", "v1", "0,0", "m1", "m1.c1"));

    List<?> hit = read(jedis, "m1.c1:CATALOG", "m1", "m1.c1");
    Assertions.assertEquals(1L, hit.get(0));
    Assertions.assertEquals("v1", utf8((byte[]) hit.get(1)));
    Assertions.assertTrue(jedis.pttl(prefix + "D:m1.c1:CATALOG") > 0);
  }

  @Test
  void testFillIsRejectedWhenAnInvalidationCommittedDuringTheLoad() {
    List<?> miss = read(jedis, "m1.c1.s1:SCHEMA", "m1", "m1.c1", "m1.c1.s1");
    // While the reader loads, another node drops the catalog above the schema.
    drop(jedis, "m1.c1:CATALOG", "m1.c1", false);

    Assertions.assertEquals(
        0L,
        put(
            jedis,
            "m1.c1.s1:SCHEMA",
            "old",
            utf8((byte[]) miss.get(1)),
            "m1",
            "m1.c1",
            "m1.c1.s1"));
    Assertions.assertFalse(jedis.exists(prefix + "D:m1.c1.s1:SCHEMA"));
  }

  @Test
  void testInvalidatingAContainerOutdatesEveryDescendantInConstantTime() {
    int descendants = 20_000;
    AbstractPipeline pipeline = jedis.pipelined();
    for (int i = 0; i < descendants; i++) {
      pipeline.hset(prefix + "D:m1.c1.s1.t" + i + ":TABLE", "v", "x");
      pipeline.hset(prefix + "D:m1.c1.s1.t" + i + ":TABLE", "s", "0,0,0,0");
    }
    pipeline.sync();
    pipeline.close();

    // The drop touches the counter, the fences and the catalog's own value, nothing else.
    Assertions.assertEquals(0L, drop(jedis, "m1.c1:CATALOG", "m1.c1", false));
    Assertions.assertTrue(jedis.exists(prefix + "D:m1.c1.s1.t0:TABLE"));

    // Each descendant stops being served on its next read, which also reclaims it.
    List<?> stale = read(jedis, "m1.c1.s1.t0:TABLE", "m1", "m1.c1", "m1.c1.s1", "m1.c1.s1.t0");
    Assertions.assertEquals(2L, stale.get(0));
    Assertions.assertFalse(jedis.exists(prefix + "D:m1.c1.s1.t0:TABLE"));
    // A sibling catalog keeps its stamp.
    Assertions.assertEquals(1L, put(jedis, "m1.c2:CATALOG", "v", "0,0", "m1", "m1.c2"));
    Assertions.assertEquals(1L, read(jedis, "m1.c2:CATALOG", "m1", "m1.c2").get(0));
  }

  @Test
  void testInvalidatingTheMetalakeDiscardsItsFencesAndOutdatesEverything() {
    drop(jedis, "m1.c1:CATALOG", "m1.c1", false);
    List<?> miss = read(jedis, "m1.c1:CATALOG", "m1", "m1.c1");
    String stamp = utf8((byte[]) miss.get(1));
    Assertions.assertEquals(1L, put(jedis, "m1.c1:CATALOG", "v", stamp, "m1", "m1.c1"));

    drop(jedis, "m1:METALAKE", "m1", true);

    Assertions.assertEquals(1L, jedis.hlen(prefix + "F"));
    Assertions.assertEquals("2", jedis.hget(prefix + "F", "m1"));
    Assertions.assertEquals(2L, read(jedis, "m1.c1:CATALOG", "m1", "m1.c1").get(0));
    // The counter survives, so the next generation never repeats one a reader observed.
    drop(jedis, "m1.c1:CATALOG", "m1.c1", false);
    Assertions.assertEquals("3", jedis.hget(prefix + "F", "m1.c1"));
  }

  @Test
  void testPeekServesOnlyCurrentValuesAndRecordsNothing() {
    put(jedis, "m1.c1:CATALOG", "cat", "0,0", "m1", "m1.c1");
    put(jedis, "m1:METALAKE", "ml", "0", "m1");
    put(jedis, "m1.c2:CATALOG", "old", "0,0", "m1", "m1.c2");
    drop(jedis, "m1.c2:CATALOG", "m1.c2", false);

    List<?> reply =
        (List<?>)
            jedis.eval(
                utf8(RedisEntityCache.PEEK_SCRIPT),
                ImmutableList.of(
                    utf8(prefix + "F"),
                    utf8(prefix + "D:m1.c1:CATALOG"),
                    utf8(prefix + "D:m1.c2:CATALOG"),
                    utf8(prefix + "D:m1.c3:CATALOG"),
                    utf8(prefix + "D:m1:METALAKE")),
                ImmutableList.of(
                    utf8("2"),
                    utf8("m1"),
                    utf8("m1.c1"),
                    utf8("2"),
                    utf8("m1"),
                    utf8("m1.c2"),
                    utf8("2"),
                    utf8("m1"),
                    utf8("m1.c3"),
                    utf8("1"),
                    utf8("m1")));

    Assertions.assertEquals(4, reply.size());
    Assertions.assertEquals("cat", utf8((byte[]) reply.get(0)));
    Assertions.assertNull(reply.get(1));
    Assertions.assertNull(reply.get(2));
    Assertions.assertEquals("ml", utf8((byte[]) reply.get(3)));
    Assertions.assertFalse(jedis.exists(prefix + "D:m1.c2:CATALOG"));
  }

  @Test
  void testDiscardLeavesAConcurrentFillIntact() {
    String value = prefix + "D:m1.c1:CATALOG";
    jedis.hset(value, "v", "garbage");
    // A reader observed "garbage"; before it discards, another node fills the key.
    jedis.hset(value, "v", "fresh");

    Assertions.assertEquals(0L, discard(value, "garbage"));
    Assertions.assertEquals("fresh", jedis.hget(value, "v"));
    Assertions.assertEquals(1L, discard(value, "fresh"));
    Assertions.assertFalse(jedis.exists(value));
  }

  /**
   * The round-3 reproduction: under {@code volatile-lru} memory pressure, the fence an invalidation
   * planted must survive, so a fill that loaded before it is still rejected.
   */
  @Test
  void testFencesSurviveVolatileEvictionSoAStaleFillIsStillRejected() {
    try (GenericContainer<?> small =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withExposedPorts(PORT)
            .withCommand(
                "redis-server", "--maxmemory", "4mb", "--maxmemory-policy", "volatile-lru")) {
      small.start();
      try (JedisPooled evicting = connect(small)) {
        // Node B misses on a key that was never invalidated and starts loading the old row.
        List<?> miss = read(evicting, "m1.c1:CATALOG", "m1", "m1.c1");
        // Node A commits an update and invalidates.
        drop(evicting, "m1.c1:CATALOG", "m1.c1", false);

        // Ordinary cache traffic (values carry a TTL) fills memory until Redis evicts.
        long evictedBefore = evictedKeys(evicting);
        byte[] filler = new byte[4096];
        int i = 0;
        while (evictedKeys(evicting) == evictedBefore && i < 100_000) {
          evicting.set(
              utf8(prefix + "D:filler" + i++ + ":TABLE"), filler, new SetParams().px(3_600_000));
        }
        Assertions.assertTrue(evictedKeys(evicting) > evictedBefore, "Redis never evicted");

        // The fence and the counter carry no TTL, so volatile-lru never picks them.
        Assertions.assertEquals("1", evicting.hget(prefix + "F", "m1.c1"));
        Assertions.assertEquals("1", evicting.get(prefix + "G"));
        Assertions.assertEquals(
            0L, put(evicting, "m1.c1:CATALOG", "old", utf8((byte[]) miss.get(1)), "m1", "m1.c1"));
      }
    }
  }

  private List<?> read(JedisPooled client, String member, String... paths) {
    return (List<?>)
        client.eval(
            utf8(RedisEntityCache.READ_SCRIPT),
            ImmutableList.of(utf8(prefix + "D:" + member), utf8(prefix + "F")),
            utf8All(paths));
  }

  private Object put(
      JedisPooled client, String member, String value, String stamp, String... paths) {
    List<byte[]> args = new java.util.ArrayList<>();
    args.add(utf8(value));
    args.add(utf8("3600000"));
    args.add(utf8(stamp));
    args.addAll(utf8All(paths));
    return client.eval(
        utf8(RedisEntityCache.PUT_SCRIPT),
        ImmutableList.of(utf8(prefix + "D:" + member), utf8(prefix + "F")),
        args);
  }

  private Object drop(JedisPooled client, String member, String path, boolean metalake) {
    return client.eval(
        utf8(RedisEntityCache.DROP_SCRIPT),
        ImmutableList.of(utf8(prefix + "G"), utf8(prefix + "F"), utf8(prefix + "D:" + member)),
        ImmutableList.of(utf8(path), utf8(metalake ? "1" : "0")));
  }

  private Object discard(String valueKey, String observed) {
    return jedis.eval(
        utf8(RedisEntityCache.DISCARD_SCRIPT),
        ImmutableList.of(utf8(valueKey)),
        ImmutableList.of(utf8(observed)));
  }

  private static long evictedKeys(JedisPooled client) {
    for (String line : client.info("stats").split("\r?\n")) {
      if (line.startsWith("evicted_keys:")) {
        return Long.parseLong(line.substring("evicted_keys:".length()).trim());
      }
    }
    throw new IllegalStateException("INFO stats reports no evicted_keys");
  }

  private static JedisPooled connect(GenericContainer<?> redis) {
    return new JedisPooled(HostAndPort.from(redis.getHost() + ":" + redis.getMappedPort(PORT)));
  }

  private static List<byte[]> utf8All(String... values) {
    List<byte[]> result = new java.util.ArrayList<>();
    for (String value : values) {
      result.add(utf8(value));
    }
    return result;
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String utf8(byte[] value) {
    return value == null ? "" : new String(value, StandardCharsets.UTF_8);
  }
}
