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
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;

/**
 * Exercises the Lua scripts of {@link RedisEntityCache} one at a time against a real Redis, at
 * interleavings the cache API cannot produce on demand: a discard arriving after a concurrent fill,
 * a reaper batch racing a refill, and fence generations across deletion of the fence key.
 */
@Tag("gravitino-docker-test")
public class TestRedisEntityCacheScripts {

  private static final String IMAGE = "redis:7.2-alpine";
  private static final int PORT = 6379;
  private static final byte[] NONE = new byte[0];

  private static GenericContainer<?> container;
  private static JedisPooled jedis;

  private String prefix;
  private String idx;

  @BeforeAll
  static void startRedis() {
    container = new GenericContainer<>(DockerImageName.parse(IMAGE)).withExposedPorts(PORT);
    container.start();
    jedis =
        new JedisPooled(
            HostAndPort.from(container.getHost() + ":" + container.getMappedPort(PORT)));
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
    idx = prefix + "IDX";
  }

  @Test
  void testDiscardLeavesAConcurrentFillIntact() {
    String value = prefix + "D:m1.c1:CATALOG";
    jedis.set(value, "garbage");
    jedis.zadd(idx, 0, "m1.c1:CATALOG");
    // A reader observed "garbage"; before it discards, another node fills the key.
    jedis.set(value, "fresh");

    Object discarded =
        jedis.eval(
            utf8(RedisEntityCache.DISCARD_SCRIPT),
            ImmutableList.of(utf8(idx), utf8(value)),
            ImmutableList.of(utf8("m1.c1:CATALOG"), utf8("garbage")));

    Assertions.assertEquals(0L, discarded);
    Assertions.assertEquals("fresh", jedis.get(value));
    Assertions.assertEquals(1, jedis.zcard(idx));

    // With the bytes unchanged, the discard removes the value and its member together.
    discarded =
        jedis.eval(
            utf8(RedisEntityCache.DISCARD_SCRIPT),
            ImmutableList.of(utf8(idx), utf8(value)),
            ImmutableList.of(utf8("m1.c1:CATALOG"), utf8("fresh")));
    Assertions.assertEquals(1L, discarded);
    Assertions.assertFalse(jedis.exists(value));
    Assertions.assertEquals(0, jedis.zcard(idx));
  }

  @Test
  void testReaperRemovesOnlyMembersWhoseValueIsGone() {
    jedis.set(prefix + "D:m1.c1:CATALOG", "v");
    jedis.zadd(idx, 0, "m1.c1:CATALOG");
    jedis.zadd(idx, 0, "m1.c2:CATALOG"); // value expired
    jedis.set(prefix + "D:m1.c3:CATALOG", "v");
    jedis.zadd(idx, 0, "m1.c3:CATALOG");
    jedis.zadd(idx, 0, "m1.c4:CATALOG"); // value expired

    List<?> first = reap("", 2);
    Assertions.assertEquals(1L, first.get(0));
    Assertions.assertEquals(2L, first.get(1));
    Assertions.assertEquals("m1.c2:CATALOG", utf8((byte[]) first.get(2)));

    List<?> second = reap(utf8((byte[]) first.get(2)), 2);
    Assertions.assertEquals(1L, second.get(0));
    Assertions.assertEquals(2L, second.get(1));
    Assertions.assertEquals("m1.c4:CATALOG", utf8((byte[]) second.get(2)));

    List<?> exhausted = reap(utf8((byte[]) second.get(2)), 2);
    Assertions.assertEquals(0L, exhausted.get(0));
    Assertions.assertEquals(0L, exhausted.get(1));
    Assertions.assertEquals("", utf8((byte[]) exhausted.get(2)));

    Assertions.assertEquals(
        ImmutableList.of("m1.c1:CATALOG", "m1.c3:CATALOG"), jedis.zrange(idx, 0, -1));
  }

  @Test
  void testReaperNeverUnindexesARefillThatLandedFirst() {
    // The member is stale, but a refill lands before the reaper looks: the script checks
    // existence and removes in one step, so it sees the refilled value and keeps the member.
    jedis.zadd(idx, 0, "m1.c1:CATALOG");
    jedis.set(prefix + "D:m1.c1:CATALOG", "refilled");

    List<?> reply = reap("", 10);

    Assertions.assertEquals(0L, reply.get(0));
    Assertions.assertEquals(1, jedis.zcard(idx));
  }

  @Test
  void testDropAssignsStrictlyIncreasingGenerationsEvenIfTheFenceKeyIsGone() {
    String fence = prefix + "F:m1.c1";
    drop("m1.c1");
    long first = Long.parseLong(jedis.get(fence));
    // The fence key expires or is otherwise lost; the generation counter is not.
    jedis.del(fence);
    drop("m1.c1");
    long second = Long.parseLong(jedis.get(fence));
    drop("m1.c1");
    long third = Long.parseLong(jedis.get(fence));

    Assertions.assertTrue(second > first);
    Assertions.assertTrue(third > second);
    Assertions.assertEquals(third, Long.parseLong(jedis.get(prefix + "G")));
  }

  @Test
  void testClearMovesTheMetalakeFenceAndDeletesValuesWithTheIndex() {
    jedis.set(prefix + "D:m1:METALAKE", "v");
    jedis.zadd(idx, 0, "m1:METALAKE");
    jedis.set(prefix + "D:m1.c1:CATALOG", "v");
    jedis.zadd(idx, 0, "m1.c1:CATALOG");
    String metalakeFence = prefix + "F:m1";
    // An earlier drop of a catalog that holds no value: it plants a fence and moves the generation.
    drop("m1.c9");
    long generationBefore = Long.parseLong(jedis.get(prefix + "G"));

    Object removed =
        jedis.eval(
            utf8(RedisEntityCache.CLEAR_SCRIPT),
            ImmutableList.of(utf8(idx)),
            ImmutableList.of(utf8(prefix), utf8(metalakeFence), utf8("0")));

    Assertions.assertEquals(2L, removed);
    Assertions.assertFalse(jedis.exists(prefix + "D:m1:METALAKE"));
    Assertions.assertFalse(jedis.exists(prefix + "D:m1.c1:CATALOG"));
    Assertions.assertFalse(jedis.exists(idx));
    Assertions.assertEquals(generationBefore + 1, Long.parseLong(jedis.get(metalakeFence)));
    // The catalog fence set by the earlier drop is left in place.
    Assertions.assertTrue(jedis.exists(prefix + "F:m1.c9"));
  }

  private List<?> reap(String cursor, int batch) {
    return (List<?>)
        jedis.eval(
            utf8(RedisEntityCache.REAP_SCRIPT),
            ImmutableList.of(utf8(idx)),
            ImmutableList.of(utf8(prefix), utf8(cursor), utf8(Integer.toString(batch))));
  }

  private void drop(String identifier) {
    jedis.eval(
        utf8(RedisEntityCache.DROP_SCRIPT),
        ImmutableList.of(utf8(idx)),
        ImmutableList.of(
            utf8(prefix),
            utf8(identifier + ":CATALOG"),
            utf8(identifier),
            utf8("0"),
            utf8(identifier + ".")));
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String utf8(byte[] value) {
    return value == null || value == NONE ? "" : new String(value, StandardCharsets.UTF_8);
  }
}
