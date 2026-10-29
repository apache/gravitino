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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisNoScriptException;
import redis.clients.jedis.util.JedisClusterCRC16;

/**
 * Unit tests of {@link RedisEntityCache} that need no Redis: configuration, the failure policy, and
 * the stamp bookkeeping around the scripts. Behavior against a real server is covered by {@link
 * TestRedisEntityCacheScripts} and the integration tests.
 */
public class TestRedisEntityCache {

  private static final NameIdentifier CATALOG_IDENT = NameIdentifier.of("m1", "c1");
  private static final CatalogEntity CATALOG =
      TestUtil.getTestCatalogEntity(1L, "c1", Namespace.of("m1"), "hive", "cmt");

  /** A miss reply: not found, and the fences of m1 and m1.c1 both absent. */
  private static final List<Object> MISS = ImmutableList.of(0L, utf8("0,0"));

  private static Config redisConfig() {
    Config config = new Config(false) {};
    config.set(Configs.CACHE_IMPLEMENTATION, "redis");
    config.set(Configs.CACHE_REDIS_ADDRESS, "127.0.0.1:6379");
    return config;
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  void testFactoryRegistersRedis() {
    Assertions.assertEquals(
        RedisEntityCache.class.getCanonicalName(), CacheFactory.ENTITY_CACHES.get("redis"));
  }

  @Test
  void testFactoryRequiresAnAddress() {
    Config config = new Config(false) {};
    config.set(Configs.CACHE_IMPLEMENTATION, "redis");
    RuntimeException e =
        Assertions.assertThrows(RuntimeException.class, () -> CacheFactory.getEntityCache(config));
    Assertions.assertTrue(
        e.getCause().getCause().getMessage().contains(Configs.CACHE_REDIS_ADDRESS.getKey()),
        e.getCause().getCause().getMessage());
  }

  @Test
  void testStandaloneTakesExactlyOneAddress() {
    Config config = redisConfig();
    config.set(Configs.CACHE_REDIS_ADDRESS, "127.0.0.1:6379,127.0.0.1:6380");
    Assertions.assertThrows(RuntimeException.class, () -> CacheFactory.getEntityCache(config));
  }

  @Test
  void testFactoryFailsFastWhenRedisIsUnreachable() {
    Config config = redisConfig();
    // Nothing listens on port 1, so the startup probe fails instead of degrading silently.
    config.set(Configs.CACHE_REDIS_ADDRESS, "127.0.0.1:1");
    config.set(Configs.CACHE_REDIS_TIMEOUT_MS, 500);
    RuntimeException e =
        Assertions.assertThrows(RuntimeException.class, () -> CacheFactory.getEntityCache(config));
    Assertions.assertInstanceOf(IllegalStateException.class, e.getCause().getCause());
  }

  @Test
  void testSerializerMustBeKryo() {
    Config config = redisConfig();
    Assertions.assertEquals("kryo", config.get(Configs.CACHE_REDIS_SERIALIZER));
    config.set(Configs.CACHE_REDIS_SERIALIZER, "json");
    Assertions.assertThrows(
        RuntimeException.class, () -> config.get(Configs.CACHE_REDIS_SERIALIZER));
  }

  @Test
  void testSslIsOffByDefault() {
    Assertions.assertFalse(redisConfig().get(Configs.CACHE_REDIS_SSL));
  }

  @Test
  void testNamespaceRejectsBracesAndGlobMetacharacters() {
    Config config = redisConfig();
    Assertions.assertEquals("gravitino", config.get(Configs.CACHE_REDIS_NAMESPACE));
    for (String bad : ImmutableList.of("a{b}", "a*b", "a?b", "a[b]", "a b", "")) {
      config.set(Configs.CACHE_REDIS_NAMESPACE, bad);
      Assertions.assertThrows(
          RuntimeException.class, () -> config.get(Configs.CACHE_REDIS_NAMESPACE), bad);
    }
    config.set(Configs.CACHE_REDIS_NAMESPACE, "prod.eu-1:cache_2");
    Assertions.assertEquals("prod.eu-1:cache_2", config.get(Configs.CACHE_REDIS_NAMESPACE));
  }

  @Test
  void testValueTtlMustBePositive() {
    // Outdated values are reclaimed by their TTL, so a value that never expires could leak.
    Config config = redisConfig();
    config.set(Configs.CACHE_EXPIRATION_TIME, 0L);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new RedisEntityCache(config, mock(UnifiedJedis.class)));
  }

  @Test
  void testCoherenceIsShared() {
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), mock(UnifiedJedis.class));
    Assertions.assertEquals(Coherence.SHARED, cache.coherence());
  }

  @Test
  void testEveryKeyOfAMetalakeHashesToOneClusterSlot() {
    RedisKeyspace keyspace = new RedisKeyspace("gravitino");
    NameIdentifier table = NameIdentifier.of("m1", "c1", "s1", "t1");
    int slot = JedisClusterCRC16.getSlot(keyspace.fencesKey(table));
    Assertions.assertEquals(
        slot,
        JedisClusterCRC16.getSlot(
            keyspace.valueKey(EntityCacheKey.of(table, Entity.EntityType.TABLE))));
    Assertions.assertEquals(
        slot,
        JedisClusterCRC16.getSlot(
            keyspace.valueKey(
                EntityCacheKey.of(NameIdentifier.of("m1"), Entity.EntityType.METALAKE))));
    Assertions.assertEquals(slot, JedisClusterCRC16.getSlot(keyspace.generationKey(table)));
    Assertions.assertNotEquals(
        slot, JedisClusterCRC16.getSlot(keyspace.fencesKey(NameIdentifier.of("m2"))));
  }

  @Test
  void testReadFailureIsAMiss() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.evalsha(any(byte[].class), anyList(), anyList()))
        .thenThrow(new JedisConnectionException("down"));
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    Assertions.assertEquals(
        Optional.empty(), cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG));
    Assertions.assertFalse(cache.contains(CATALOG_IDENT, Entity.EntityType.CATALOG));
    Assertions.assertEquals(2L, cache.metrics().getCounter("redis-failures-total").getCount());
  }

  @Test
  void testWriteFailureIsDiscarded() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.evalsha(any(byte[].class), anyList(), anyList()))
        .thenReturn(MISS)
        .thenThrow(new JedisConnectionException("down"));
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG);
    Assertions.assertDoesNotThrow(() -> cache.put(CATALOG));
    verify(jedis, times(2)).evalsha(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testInvalidateFailureIsRetriedThenReportedWithoutThrowing() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.evalsha(any(byte[].class), anyList(), anyList()))
        .thenThrow(new JedisConnectionException("down"));
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    // The store committed before invalidating, so a Redis failure must not turn a successful write
    // into a failed one. The change-log replay repeats the invalidation.
    Assertions.assertFalse(cache.invalidate(CATALOG_IDENT, Entity.EntityType.CATALOG));
    verify(jedis, times(2)).evalsha(any(byte[].class), anyList(), anyList());
    Assertions.assertEquals(
        1L, cache.metrics().getCounter("invalidation-failures-total").getCount());
  }

  @Test
  void testInvalidateSucceedsOnRetry() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.evalsha(any(byte[].class), anyList(), anyList()))
        .thenThrow(new JedisConnectionException("blip"))
        .thenReturn(1L);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    Assertions.assertTrue(cache.invalidate(CATALOG_IDENT, Entity.EntityType.CATALOG));
    Assertions.assertEquals(
        0L, cache.metrics().getCounter("invalidation-failures-total").getCount());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testInvalidateMovesOneFenceAndDeclaresEveryKey() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.evalsha(any(byte[].class), anyList(), anyList())).thenReturn(1L);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    cache.invalidate(CATALOG_IDENT, Entity.EntityType.CATALOG);
    cache.invalidate(NameIdentifier.of("m1"), Entity.EntityType.METALAKE);

    ArgumentCaptor<List<byte[]>> keys = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<byte[]>> args = ArgumentCaptor.forClass(List.class);
    verify(jedis, times(2)).evalsha(any(byte[].class), keys.capture(), args.capture());
    Assertions.assertEquals(
        ImmutableList.of("gravitino:{m1}:G", "gravitino:{m1}:F", "gravitino:{m1}:D:m1.c1:CATALOG"),
        decode(keys.getAllValues().get(0)));
    Assertions.assertEquals(ImmutableList.of("m1.c1", "0"), decode(args.getAllValues().get(0)));
    // Invalidating the metalake itself also discards its fences, which the new metalake fence
    // outdates anyway.
    Assertions.assertEquals(ImmutableList.of("m1", "1"), decode(args.getAllValues().get(1)));
  }

  @Test
  void testScriptBodyIsSentOnlyWhenRedisDoesNotHaveIt() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.evalsha(any(byte[].class), anyList(), anyList()))
        .thenThrow(new JedisNoScriptException("NOSCRIPT"));
    when(jedis.eval(any(byte[].class), anyList(), anyList())).thenReturn(MISS);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG);

    ArgumentCaptor<byte[]> body = ArgumentCaptor.forClass(byte[].class);
    verify(jedis).eval(body.capture(), anyList(), anyList());
    Assertions.assertEquals(RedisEntityCache.READ_SCRIPT, decode(body.getValue()));
    Assertions.assertEquals(0L, cache.metrics().getCounter("redis-failures-total").getCount());
  }

  @Test
  void testHitReturnsTheDeserializedEntity() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    byte[] value = new KryoEntitySerializer().serialize(CATALOG);
    when(jedis.evalsha(any(byte[].class), anyList(), anyList()))
        .thenReturn(ImmutableList.of(1L, value));
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    Optional<CatalogEntity> cached = cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG);
    Assertions.assertEquals(Optional.of(CATALOG), cached);
    Assertions.assertEquals(1L, cache.metrics().getCounter("hits-total").getCount());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testUndecodableEntryIsDiscardedOnlyIfUnchanged() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    byte[] garbage = utf8("not an entity");
    when(jedis.evalsha(any(byte[].class), anyList(), anyList()))
        .thenReturn(ImmutableList.of(1L, garbage))
        .thenReturn(1L);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    Assertions.assertEquals(
        Optional.empty(), cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG));

    ArgumentCaptor<List<byte[]>> keys = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<byte[]>> args = ArgumentCaptor.forClass(List.class);
    verify(jedis, times(2)).evalsha(any(byte[].class), keys.capture(), args.capture());
    // The discard is one script that checks the bytes are still the ones read, so a concurrent
    // fill that replaced the value is never removed.
    Assertions.assertEquals(
        ImmutableList.of("gravitino:{m1}:D:m1.c1:CATALOG"), decode(keys.getAllValues().get(1)));
    Assertions.assertArrayEquals(garbage, args.getAllValues().get(1).get(0));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testMissRecordsTheStampThatGuardsTheFollowingWrite() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    // The miss reports the stamp of m1 and m1.c1 as "0,3"; the write must echo it back.
    when(jedis.evalsha(any(byte[].class), anyList(), anyList()))
        .thenReturn(ImmutableList.of(0L, utf8("0,3")))
        .thenReturn(1L);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    Assertions.assertEquals(
        Optional.empty(), cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG));
    cache.put(CATALOG);
    // The record is consumed: a second write with no miss before it is not performed.
    cache.put(CATALOG);

    ArgumentCaptor<List<byte[]>> keys = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<byte[]>> args = ArgumentCaptor.forClass(List.class);
    verify(jedis, times(2)).evalsha(any(byte[].class), keys.capture(), args.capture());

    Assertions.assertEquals(
        ImmutableList.of("gravitino:{m1}:D:m1.c1:CATALOG", "gravitino:{m1}:F"),
        decode(keys.getAllValues().get(0)));
    Assertions.assertEquals(ImmutableList.of("m1", "m1.c1"), decode(args.getAllValues().get(0)));

    List<byte[]> guardedWrite = args.getAllValues().get(1);
    Assertions.assertEquals(
        ImmutableList.of("gravitino:{m1}:D:m1.c1:CATALOG", "gravitino:{m1}:F"),
        decode(keys.getAllValues().get(1)));
    Assertions.assertEquals(5, guardedWrite.size());
    Assertions.assertEquals("3600000", decode(guardedWrite.get(1)));
    Assertions.assertEquals("0,3", decode(guardedWrite.get(2)));
    Assertions.assertEquals("m1", decode(guardedWrite.get(3)));
    Assertions.assertEquals("m1.c1", decode(guardedWrite.get(4)));
    Assertions.assertEquals(1L, cache.metrics().getCounter("fills-written-total").getCount());
    Assertions.assertEquals(1L, cache.metrics().getCounter("fills-skipped-total").getCount());
  }

  @Test
  void testRejectedFillIsCounted() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.evalsha(any(byte[].class), anyList(), anyList())).thenReturn(MISS).thenReturn(0L);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG);
    cache.put(CATALOG);

    Assertions.assertEquals(1L, cache.metrics().getCounter("fills-rejected-total").getCount());
  }

  @Test
  void testWriteWithoutAPrecedingMissIsNotPerformed() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    // Caching a freshly inserted entity has no miss that bounded its load window, so it fails
    // closed rather than being written unguarded.
    cache.put(CATALOG);

    verify(jedis, never()).evalsha(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testReadFailureDropsAnEarlierRecordSoTheWriteIsNotPerformed() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.evalsha(any(byte[].class), anyList(), anyList()))
        .thenReturn(MISS)
        .thenThrow(new JedisConnectionException("down"));
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    // A miss records a stamp, then a second read of the same key fails: the failed read must not
    // let the earlier record vouch for a load it did not bound.
    cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG);
    Assertions.assertEquals(
        Optional.empty(), cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG));
    cache.put(CATALOG);

    verify(jedis, times(2)).evalsha(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testRecordsEvictedFromThePendingBoundFailClosed() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.evalsha(any(byte[].class), anyList(), anyList())).thenReturn(MISS);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);
    int misses = RedisEntityCache.MAX_PENDING_STAMPS + 1;
    CatalogEntity first = catalog(0);
    CatalogEntity last = catalog(misses - 1);

    // The batchGet shape: every miss is recorded before any load, one more than the bound.
    for (int i = 0; i < misses; i++) {
      cache.getIfPresent(catalog(i).nameIdentifier(), Entity.EntityType.CATALOG);
    }
    when(jedis.evalsha(any(byte[].class), anyList(), anyList())).thenReturn(1L);
    cache.put(first);
    verify(jedis, times(misses)).evalsha(any(byte[].class), anyList(), anyList());
    cache.put(last);
    verify(jedis, times(misses + 1)).evalsha(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testWithCacheLockDropsAnUnconsumedRecord() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.evalsha(any(byte[].class), anyList(), anyList())).thenReturn(MISS).thenReturn(1L);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);
    EntityCacheKey key = EntityCacheKey.of(CATALOG_IDENT, Entity.EntityType.CATALOG);

    // The load inside the lock finds nothing in the store, so no write consumes the record, and a
    // later write outside the lock must not reuse it.
    cache.withCacheLock(
        key, () -> cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG).isPresent());
    cache.put(CATALOG);

    verify(jedis, times(1)).evalsha(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testNonCacheableTypesNeverReachRedis() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);
    UserEntity user = TestUtil.getTestUserEntity();

    cache.put(user);
    Assertions.assertEquals(
        Optional.empty(), cache.getIfPresent(user.nameIdentifier(), Entity.EntityType.USER));

    verify(jedis, never()).evalsha(any(byte[].class), anyList(), anyList());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testPeekAllReadsAnAncestorChainInOneScriptAndRecordsNothing() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    BaseMetalake metalake = TestUtil.getTestMetalake(9L, "m1", "cmt");
    byte[] catalogBytes = new KryoEntitySerializer().serialize(CATALOG);
    byte[] metalakeBytes = new KryoEntitySerializer().serialize(metalake);
    when(jedis.evalsha(any(byte[].class), anyList(), anyList()))
        .thenReturn(Arrays.asList(catalogBytes, metalakeBytes))
        .thenReturn(1L);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    List<Optional<HasIdentifier>> found =
        cache.peekAll(
            ImmutableList.of(
                EntityCacheKey.of(CATALOG_IDENT, Entity.EntityType.CATALOG),
                EntityCacheKey.of(NameIdentifier.of("m1"), Entity.EntityType.METALAKE)));
    Assertions.assertEquals(1L, found.get(0).get().id());
    Assertions.assertEquals(9L, found.get(1).get().id());

    ArgumentCaptor<byte[]> sha = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<List<byte[]>> keys = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<byte[]>> args = ArgumentCaptor.forClass(List.class);
    verify(jedis).evalsha(sha.capture(), keys.capture(), args.capture());
    Assertions.assertEquals(
        ImmutableList.of(
            "gravitino:{m1}:F", "gravitino:{m1}:D:m1.c1:CATALOG", "gravitino:{m1}:D:m1:METALAKE"),
        decode(keys.getValue()));
    Assertions.assertEquals(
        ImmutableList.of("2", "m1", "m1.c1", "1", "m1"), decode(args.getValue()));

    // A peek prepares nothing for a later write.
    cache.put(CATALOG);
    verify(jedis, times(1)).evalsha(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testPeekAllFailureReportsEveryKeyAbsent() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.evalsha(any(byte[].class), anyList(), anyList()))
        .thenThrow(new JedisConnectionException("down"));
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    List<Optional<HasIdentifier>> found =
        cache.peekAll(
            ImmutableList.of(
                EntityCacheKey.of(CATALOG_IDENT, Entity.EntityType.CATALOG),
                EntityCacheKey.of(NameIdentifier.of("m1"), Entity.EntityType.METALAKE)));
    Assertions.assertEquals(ImmutableList.of(Optional.empty(), Optional.empty()), found);
  }

  @Test
  void testCloseReleasesTheClientAndLeavesSharedEntries() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    cache.close();

    verify(jedis).close();
    verify(jedis, never()).evalsha(any(byte[].class), anyList(), anyList());
    verify(jedis, never()).eval(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testOperationsAfterCloseFailInsteadOfReconnecting() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);
    cache.close();

    Assertions.assertThrows(
        IllegalStateException.class,
        () -> cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG));
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> cache.invalidate(CATALOG_IDENT, Entity.EntityType.CATALOG));
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> cache.contains(CATALOG_IDENT, Entity.EntityType.CATALOG));
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            cache.peekAll(
                ImmutableList.of(EntityCacheKey.of(CATALOG_IDENT, Entity.EntityType.CATALOG))));
    Assertions.assertThrows(IllegalStateException.class, () -> cache.put(CATALOG));
    Assertions.assertThrows(IllegalStateException.class, cache::size);
    Assertions.assertThrows(IllegalStateException.class, cache::clear);
    verify(jedis, never()).evalsha(any(byte[].class), anyList(), anyList());
  }

  private static CatalogEntity catalog(int i) {
    return TestUtil.getTestCatalogEntity(i + 1L, "c" + i, Namespace.of("m1"), "hive", "cmt");
  }

  private static List<String> decode(List<byte[]> values) {
    return values.stream()
        .map(TestRedisEntityCache::decode)
        .collect(ImmutableList.toImmutableList());
  }

  private static String decode(byte[] value) {
    return new String(value, StandardCharsets.UTF_8);
  }
}
