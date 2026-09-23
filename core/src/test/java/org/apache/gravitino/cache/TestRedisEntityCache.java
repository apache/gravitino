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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.util.JedisClusterCRC16;

/**
 * Unit tests of {@link RedisEntityCache} that need no Redis: configuration, the failure policy, and
 * the fence bookkeeping around the scripts. Behavior against a real server is covered by the
 * integration tests.
 */
public class TestRedisEntityCache {

  private static final NameIdentifier CATALOG_IDENT = NameIdentifier.of("m1", "c1");
  private static final CatalogEntity CATALOG =
      TestUtil.getTestCatalogEntity(1L, "c1", Namespace.of("m1"), "hive", "cmt");

  /** A miss reply: not found, and the fences of m1 and m1.c1 both absent. */
  private static final List<Object> MISS = ImmutableList.of(0L, utf8("0"), utf8("0"));

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
  void testFenceTtlMustOutliveValueTtl() {
    Config config = redisConfig();
    config.set(Configs.CACHE_EXPIRATION_TIME, 1_000L);
    config.set(Configs.CACHE_REDIS_FENCE_TTL_MS, 1_000L);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new RedisEntityCache(config, mock(UnifiedJedis.class)));

    config.set(Configs.CACHE_REDIS_FENCE_TTL_MS, 0L);
    Assertions.assertDoesNotThrow(() -> new RedisEntityCache(config, mock(UnifiedJedis.class)));

    config.set(Configs.CACHE_EXPIRATION_TIME, 0L);
    config.set(Configs.CACHE_REDIS_FENCE_TTL_MS, 5L);
    Assertions.assertDoesNotThrow(() -> new RedisEntityCache(config, mock(UnifiedJedis.class)));
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
    int slot = JedisClusterCRC16.getSlot(keyspace.indexKey(table));
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
    for (String path : RedisKeyspace.fencePaths(table, ":")) {
      Assertions.assertEquals(slot, JedisClusterCRC16.getSlot(keyspace.fenceKey(table, path)));
    }
    Assertions.assertNotEquals(
        slot, JedisClusterCRC16.getSlot(keyspace.indexKey(NameIdentifier.of("m2"))));
  }

  @Test
  void testReadFailureIsAMiss() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.eval(any(byte[].class), anyList(), anyList()))
        .thenThrow(new JedisConnectionException("down"));
    when(jedis.exists(any(byte[].class))).thenThrow(new JedisConnectionException("down"));
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    Assertions.assertEquals(
        Optional.empty(), cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG));
    Assertions.assertFalse(cache.contains(CATALOG_IDENT, Entity.EntityType.CATALOG));
  }

  @Test
  void testWriteFailureIsDiscarded() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.eval(any(byte[].class), anyList(), anyList()))
        .thenReturn(MISS)
        .thenThrow(new JedisConnectionException("down"));
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG);
    Assertions.assertDoesNotThrow(() -> cache.put(CATALOG));
    verify(jedis, times(2)).eval(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testInvalidateFailurePropagates() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.eval(any(byte[].class), anyList(), anyList()))
        .thenThrow(new JedisConnectionException("down"));
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    RuntimeException e =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> cache.invalidate(CATALOG_IDENT, Entity.EntityType.CATALOG));
    Assertions.assertInstanceOf(JedisConnectionException.class, e.getCause());
  }

  @Test
  void testHitReturnsTheDeserializedEntity() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    byte[] value = new KryoEntitySerializer().serialize(CATALOG);
    when(jedis.eval(any(byte[].class), anyList(), anyList()))
        .thenReturn(ImmutableList.of(1L, value));
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    Optional<CatalogEntity> cached = cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG);
    Assertions.assertEquals(Optional.of(CATALOG), cached);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testUndecodableEntryIsDiscardedOnlyIfUnchanged() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    byte[] garbage = utf8("not an entity");
    when(jedis.eval(any(byte[].class), anyList(), anyList()))
        .thenReturn(ImmutableList.of(1L, garbage))
        .thenReturn(1L);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    Assertions.assertEquals(
        Optional.empty(), cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG));

    ArgumentCaptor<byte[]> scripts = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<List<byte[]>> keys = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<byte[]>> args = ArgumentCaptor.forClass(List.class);
    verify(jedis, times(2)).eval(scripts.capture(), keys.capture(), args.capture());
    // The discard is one script that checks the bytes are still the ones read, so a concurrent
    // fill that replaced the value is never removed together with its index member.
    Assertions.assertEquals(RedisEntityCache.DISCARD_SCRIPT, decode(scripts.getAllValues().get(1)));
    Assertions.assertEquals(
        ImmutableList.of("gravitino:{m1}:IDX", "gravitino:{m1}:D:m1.c1:CATALOG"),
        decode(keys.getAllValues().get(1)));
    Assertions.assertEquals("m1.c1:CATALOG", decode(args.getAllValues().get(1).get(0)));
    Assertions.assertArrayEquals(garbage, args.getAllValues().get(1).get(1));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testMissRecordsFencesThatGuardTheFollowingWrite() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    // The miss reports the fences of m1 and m1.c1 as "0" and "3"; the write must echo them back.
    when(jedis.eval(any(byte[].class), anyList(), anyList()))
        .thenReturn(ImmutableList.of(0L, utf8("0"), utf8("3")))
        .thenReturn(1L);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    Assertions.assertEquals(
        Optional.empty(), cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG));
    cache.put(CATALOG);
    // The record is consumed: a second write with no miss before it is not performed.
    cache.put(CATALOG);

    ArgumentCaptor<List<byte[]>> keys = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<byte[]>> args = ArgumentCaptor.forClass(List.class);
    verify(jedis, times(2)).eval(any(byte[].class), keys.capture(), args.capture());

    List<byte[]> readArgs = args.getAllValues().get(0);
    Assertions.assertEquals(
        ImmutableList.of("gravitino:{m1}:D:m1.c1:CATALOG"), decode(keys.getAllValues().get(0)));
    Assertions.assertEquals(
        ImmutableList.of("gravitino:{m1}:F:m1", "gravitino:{m1}:F:m1.c1"), decode(readArgs));

    List<byte[]> guardedWrite = args.getAllValues().get(1);
    Assertions.assertEquals(
        ImmutableList.of("gravitino:{m1}:IDX", "gravitino:{m1}:D:m1.c1:CATALOG"),
        decode(keys.getAllValues().get(1)));
    Assertions.assertEquals(7, guardedWrite.size());
    Assertions.assertEquals("m1.c1:CATALOG", decode(guardedWrite.get(0)));
    Assertions.assertEquals("3600000", decode(guardedWrite.get(2)));
    Assertions.assertEquals("gravitino:{m1}:F:m1", decode(guardedWrite.get(3)));
    Assertions.assertEquals("0", decode(guardedWrite.get(4)));
    Assertions.assertEquals("gravitino:{m1}:F:m1.c1", decode(guardedWrite.get(5)));
    Assertions.assertEquals("3", decode(guardedWrite.get(6)));
  }

  @Test
  void testWriteWithoutAPrecedingMissIsNotPerformed() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    // Caching a freshly inserted entity has no miss that bounded its load window, so it fails
    // closed rather than being written unguarded.
    cache.put(CATALOG);

    verify(jedis, never()).eval(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testReadFailureDropsAnEarlierRecordSoTheWriteIsNotPerformed() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.eval(any(byte[].class), anyList(), anyList()))
        .thenReturn(MISS)
        .thenThrow(new JedisConnectionException("down"));
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    // A miss records fences, then a second read of the same key fails: the failed read must not
    // let the earlier record vouch for a load it did not bound.
    cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG);
    Assertions.assertEquals(
        Optional.empty(), cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG));
    cache.put(CATALOG);

    verify(jedis, times(2)).eval(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testRecordOlderThanTheFenceLifetimeIsNotWritten() throws InterruptedException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.eval(any(byte[].class), anyList(), anyList())).thenReturn(MISS).thenReturn(1L);
    Config config = redisConfig();
    config.set(Configs.CACHE_EXPIRATION_TIME, 1L);
    config.set(Configs.CACHE_REDIS_FENCE_TTL_MS, 2L);
    RedisEntityCache cache = new RedisEntityCache(config, jedis);

    cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG);
    Thread.sleep(10);
    // The fence this record observed may have expired by now, so the record cannot be trusted.
    cache.put(CATALOG);

    verify(jedis, times(1)).eval(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testRecordsEvictedFromThePendingBoundFailClosed() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.eval(any(byte[].class), anyList(), anyList())).thenReturn(MISS);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);
    int misses = RedisEntityCache.MAX_PENDING_FENCES + 1;
    CatalogEntity first = catalog(0);
    CatalogEntity last = catalog(misses - 1);

    // The batchGet shape: every miss is recorded before any load, one more than the bound.
    for (int i = 0; i < misses; i++) {
      cache.getIfPresent(catalog(i).nameIdentifier(), Entity.EntityType.CATALOG);
    }
    when(jedis.eval(any(byte[].class), anyList(), anyList())).thenReturn(1L);
    cache.put(first);
    verify(jedis, times(misses)).eval(any(byte[].class), anyList(), anyList());
    cache.put(last);
    verify(jedis, times(misses + 1)).eval(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testWithCacheLockDropsAnUnconsumedFenceRecord() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.eval(any(byte[].class), anyList(), anyList())).thenReturn(MISS).thenReturn(1L);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);
    EntityCacheKey key = EntityCacheKey.of(CATALOG_IDENT, Entity.EntityType.CATALOG);

    // The load inside the lock finds nothing in the store, so no write consumes the record, and a
    // later write outside the lock must not reuse it.
    cache.withCacheLock(
        key, () -> cache.getIfPresent(CATALOG_IDENT, Entity.EntityType.CATALOG).isPresent());
    cache.put(CATALOG);

    verify(jedis, times(1)).eval(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testNonCacheableTypesNeverReachRedis() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);
    UserEntity user = TestUtil.getTestUserEntity();

    cache.put(user);

    verify(jedis, never()).eval(any(byte[].class), anyList(), anyList());
  }

  @Test
  void testCloseReleasesTheClientAndLeavesSharedEntries() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    cache.close();

    verify(jedis).close();
    verify(jedis, never()).eval(any(byte[].class), anyList(), anyList());
    verify(jedis, never()).scan(anyString(), any(ScanParams.class));
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
    Assertions.assertThrows(IllegalStateException.class, () -> cache.put(CATALOG));
    Assertions.assertThrows(IllegalStateException.class, cache::size);
    Assertions.assertThrows(IllegalStateException.class, cache::clear);
    verify(jedis, never()).eval(any(byte[].class), anyList(), anyList());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testClearRunsTheAtomicScriptOnEveryOwnedIndex() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.scan(anyString(), any(ScanParams.class)))
        .thenReturn(
            new ScanResult<>(
                ScanParams.SCAN_POINTER_START,
                ImmutableList.of(
                    "gravitino:{m1}:IDX", "gravitino:other:{m1}:IDX", "gravitino:{m2}:IDX")));
    when(jedis.eval(any(byte[].class), anyList(), anyList())).thenReturn(0L);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    cache.clear();

    ArgumentCaptor<byte[]> scripts = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<List<byte[]>> keys = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<byte[]>> args = ArgumentCaptor.forClass(List.class);
    // The index of the other namespace shares this one as a prefix and is left alone.
    verify(jedis, times(2)).eval(scripts.capture(), keys.capture(), args.capture());
    Assertions.assertEquals(RedisEntityCache.CLEAR_SCRIPT, decode(scripts.getAllValues().get(0)));
    Assertions.assertEquals(
        ImmutableList.of("gravitino:{m1}:IDX"), decode(keys.getAllValues().get(0)));
    Assertions.assertEquals(
        ImmutableList.of("gravitino:{m1}:", "gravitino:{m1}:F:m1", "7200000"),
        decode(args.getAllValues().get(0)));
    Assertions.assertEquals(
        ImmutableList.of("gravitino:{m2}:IDX"), decode(keys.getAllValues().get(1)));
  }

  @Test
  void testSizeReapsOneBatchPerIndex() {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(jedis.scan(anyString(), any(ScanParams.class)))
        .thenReturn(
            new ScanResult<>(
                ScanParams.SCAN_POINTER_START, ImmutableList.of("gravitino:{m1}:IDX")));
    when(jedis.eval(any(byte[].class), anyList(), anyList()))
        .thenReturn(ImmutableList.of(2L, 2L, utf8("m1.c1:CATALOG")));
    when(jedis.zcard(eq("gravitino:{m1}:IDX"))).thenReturn(3L);
    RedisEntityCache cache = new RedisEntityCache(redisConfig(), jedis);

    Assertions.assertEquals(3L, cache.size());

    ArgumentCaptor<byte[]> scripts = ArgumentCaptor.forClass(byte[].class);
    verify(jedis, times(1)).eval(scripts.capture(), anyList(), anyList());
    Assertions.assertEquals(RedisEntityCache.REAP_SCRIPT, decode(scripts.getValue()));
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
