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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.CacheFactory;
import org.apache.gravitino.cache.Coherence;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.cache.EntityCacheKey;
import org.apache.gravitino.cache.RedisEntityCache;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.TestUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.UnifiedJedis;

/**
 * Behavior of {@link RedisEntityCache} against a real Redis, shared by the standalone and cluster
 * suites. Each test uses its own key namespace, and "nodes" are separate cache instances over the
 * same Redis, which is exactly what two Gravitino servers sharing one cache are.
 */
public abstract class RedisEntityCacheTestBase {

  protected static final String SEP = HierarchicalSchemaUtil.schemaSeparator();
  private static final long DEFAULT_TTL_MS = 60_000L;

  private final List<RedisEntityCache> caches = new ArrayList<>();
  protected String namespace;

  /** The {@code gravitino.cache.redis.address} value for the Redis under test. */
  protected abstract String address();

  /** Whether the Redis under test is a cluster. */
  protected abstract boolean cluster();

  /** A raw client for inspecting keys directly. */
  protected abstract UnifiedJedis rawClient();

  @BeforeEach
  void newNamespace() {
    namespace = "it-" + UUID.randomUUID().toString().substring(0, 8);
  }

  @AfterEach
  void closeNodes() {
    for (RedisEntityCache cache : caches) {
      try {
        cache.clear();
      } finally {
        cache.close();
      }
    }
    caches.clear();
  }

  protected Config config(long ttlMs) {
    Config config = new Config(false) {};
    config.set(Configs.CACHE_IMPLEMENTATION, "redis");
    config.set(Configs.CACHE_REDIS_ADDRESS, address());
    config.set(Configs.CACHE_REDIS_CLUSTER, cluster());
    config.set(Configs.CACHE_REDIS_NAMESPACE, namespace);
    config.set(Configs.CACHE_EXPIRATION_TIME, ttlMs);
    return config;
  }

  /** A cache instance standing in for one Gravitino node. */
  protected RedisEntityCache newNode() {
    return newNode(DEFAULT_TTL_MS);
  }

  protected RedisEntityCache newNode(long ttlMs) {
    EntityCache cache = CacheFactory.getEntityCache(config(ttlMs));
    Assertions.assertInstanceOf(RedisEntityCache.class, cache);
    caches.add((RedisEntityCache) cache);
    return (RedisEntityCache) cache;
  }

  protected String valueKey(NameIdentifier ident, Entity.EntityType type) {
    String metalake = ident.hasNamespace() ? ident.namespace().level(0) : ident.name();
    return namespace + ":{" + metalake + "}:D:" + ident + ":" + type;
  }

  protected String indexKey(String metalake) {
    return namespace + ":{" + metalake + "}:IDX";
  }

  protected static BaseMetalake metalake(String name) {
    return BaseMetalake.builder()
        .withId(1L)
        .withName(name)
        .withVersion(SchemaVersion.V_0_1)
        .withComment("c")
        .withProperties(ImmutableMap.of())
        .withAuditInfo(audit())
        .build();
  }

  protected static CatalogEntity catalog(String metalake, String name) {
    return TestUtil.getTestCatalogEntity(2L, name, Namespace.of(metalake), "hive", "c");
  }

  protected static SchemaEntity schema(String metalake, String catalog, String name) {
    return TestUtil.getTestSchemaEntity(3L, name, Namespace.of(metalake, catalog), "c");
  }

  protected static TableEntity table(
      String metalake, String catalog, String schema, String name, String comment) {
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(10L)
            .withName("id")
            .withPosition(0)
            .withDataType(Types.LongType.get())
            .withNullable(false)
            .withAutoIncrement(false)
            .withAuditInfo(audit())
            .build();
    return TableEntity.builder()
        .withId(4L)
        .withName(name)
        .withNamespace(Namespace.of(metalake, catalog, schema))
        .withComment(comment)
        .withColumns(ImmutableList.of(column))
        .withAuditInfo(audit())
        .build();
  }

  protected static AuditInfo audit() {
    return AuditInfo.builder()
        .withCreator("tester")
        .withCreateTime(Instant.ofEpochMilli(1_700_000_000_000L))
        .build();
  }

  private static <E extends Entity & HasIdentifier> Optional<E> get(
      EntityCache cache, NameIdentifier ident, Entity.EntityType type) {
    return cache.getIfPresent(ident, type);
  }

  @Test
  void testFactoryBuildsASharedCache() {
    RedisEntityCache cache = newNode();
    Assertions.assertEquals(Coherence.SHARED, cache.coherence());
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void testCrossNodeFreshness() {
    RedisEntityCache nodeA = newNode();
    RedisEntityCache nodeB = newNode();
    CatalogEntity catalog = catalog("m1", "c1");

    nodeA.put(catalog);
    Assertions.assertEquals(
        Optional.of(catalog), get(nodeB, catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertTrue(nodeB.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));

    nodeA.invalidate(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    Assertions.assertEquals(
        Optional.empty(), get(nodeB, catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertFalse(nodeB.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
  }

  @Test
  void testCatalogDropRemovesDescendantsAndKeepsSiblings() {
    RedisEntityCache cache = newNode();
    BaseMetalake metalake = metalake("m1");
    CatalogEntity catalog1 = catalog("m1", "catalog1");
    CatalogEntity catalog10 = catalog("m1", "catalog10");
    SchemaEntity schema = schema("m1", "catalog1", "s1");
    TableEntity table = table("m1", "catalog1", "s1", "t1", "v1");

    cache.put(metalake);
    cache.put(catalog1);
    cache.put(catalog10);
    cache.put(schema);
    cache.put(table);
    Assertions.assertEquals(5, cache.size());

    cache.invalidate(catalog1.nameIdentifier(), Entity.EntityType.CATALOG);

    Assertions.assertTrue(cache.contains(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
    Assertions.assertTrue(cache.contains(catalog10.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertFalse(cache.contains(catalog1.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertFalse(cache.contains(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(cache.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertEquals(2, cache.size());
  }

  @Test
  void testMetalakeDropRemovesEverythingUnderIt() {
    RedisEntityCache cache = newNode();
    cache.put(metalake("m1"));
    cache.put(catalog("m1", "c1"));
    cache.put(schema("m1", "c1", "s1"));
    cache.put(table("m1", "c1", "s1", "t1", "v1"));
    cache.put(metalake("m2"));
    cache.put(catalog("m2", "c1"));

    cache.invalidate(NameIdentifier.of("m1"), Entity.EntityType.METALAKE);

    Assertions.assertEquals(2, cache.size());
    Assertions.assertTrue(cache.contains(NameIdentifier.of("m2"), Entity.EntityType.METALAKE));
    Assertions.assertTrue(cache.contains(NameIdentifier.of("m2", "c1"), Entity.EntityType.CATALOG));
    Assertions.assertFalse(
        cache.contains(NameIdentifier.of("m1", "c1", "s1", "t1"), Entity.EntityType.TABLE));
  }

  @Test
  void testSchemaDropRemovesNestedSchemasAndKeepsPrefixSiblings() {
    RedisEntityCache cache = newNode();
    SchemaEntity raw = schema("m1", "c1", "raw");
    SchemaEntity rawEvents = schema("m1", "c1", "raw" + SEP + "events");
    TableEntity nestedTable = table("m1", "c1", "raw" + SEP + "events", "t1", "v1");
    SchemaEntity raw2 = schema("m1", "c1", "raw2");
    TableEntity raw2Table = table("m1", "c1", "raw2", "t1", "v1");

    cache.put(raw);
    cache.put(rawEvents);
    cache.put(nestedTable);
    cache.put(raw2);
    cache.put(raw2Table);

    cache.invalidate(raw.nameIdentifier(), Entity.EntityType.SCHEMA);

    Assertions.assertFalse(cache.contains(raw.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(cache.contains(rawEvents.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(cache.contains(nestedTable.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(raw2.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(raw2Table.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @Test
  void testStaleWriteIsRejectedAfterTheKeyWasInvalidated() {
    RedisEntityCache nodeA = newNode();
    RedisEntityCache nodeB = newNode();
    TableEntity oldTable = table("m1", "c1", "s1", "t1", "old");
    TableEntity newTable = table("m1", "c1", "s1", "t1", "new");
    NameIdentifier ident = oldTable.nameIdentifier();

    // Node B misses and starts loading the old row from the store...
    Assertions.assertEquals(Optional.empty(), get(nodeB, ident, Entity.EntityType.TABLE));
    // ...while node A commits an update and invalidates.
    nodeA.put(newTable);
    nodeA.invalidate(ident, Entity.EntityType.TABLE);
    // Node B's late fill must not resurrect the old row.
    nodeB.put(oldTable);

    Assertions.assertFalse(nodeA.contains(ident, Entity.EntityType.TABLE));
    Assertions.assertEquals(Optional.empty(), get(nodeA, ident, Entity.EntityType.TABLE));
  }

  @Test
  void testStaleWriteIsRejectedAfterAnAncestorWasDroppedEvenIfNeverIndexed() {
    RedisEntityCache nodeA = newNode();
    RedisEntityCache nodeB = newNode();
    TableEntity table = table("m1", "c1", "s1", "t1", "v1");

    // The table was never cached, so a drop of its catalog finds nothing in the index for it.
    Assertions.assertEquals(
        Optional.empty(), get(nodeB, table.nameIdentifier(), Entity.EntityType.TABLE));
    nodeA.invalidate(NameIdentifier.of("m1", "c1"), Entity.EntityType.CATALOG);
    nodeB.put(table);

    Assertions.assertFalse(nodeA.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @Test
  void testStaleWriteIsRejectedAfterANestedSchemaParentWasDropped() {
    RedisEntityCache nodeA = newNode();
    RedisEntityCache nodeB = newNode();
    TableEntity table = table("m1", "c1", "raw" + SEP + "events", "t1", "v1");

    Assertions.assertEquals(
        Optional.empty(), get(nodeB, table.nameIdentifier(), Entity.EntityType.TABLE));
    nodeA.invalidate(NameIdentifier.of("m1", "c1", "raw"), Entity.EntityType.SCHEMA);
    nodeB.put(table);

    Assertions.assertFalse(nodeA.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @Test
  void testReloadAfterInvalidationIsAccepted() {
    RedisEntityCache nodeA = newNode();
    RedisEntityCache nodeB = newNode();
    TableEntity table = table("m1", "c1", "s1", "t1", "v2");
    NameIdentifier ident = table.nameIdentifier();

    Assertions.assertEquals(Optional.empty(), get(nodeB, ident, Entity.EntityType.TABLE));
    nodeA.invalidate(ident, Entity.EntityType.TABLE);
    // A fresh miss observes the new fence, so the load that follows it is current.
    Assertions.assertEquals(Optional.empty(), get(nodeB, ident, Entity.EntityType.TABLE));
    nodeB.put(table);

    Assertions.assertEquals(Optional.of(table), get(nodeA, ident, Entity.EntityType.TABLE));
  }

  @Test
  void testWriteWithoutAPrecedingMissIsUnconditional() {
    RedisEntityCache cache = newNode();
    CatalogEntity catalog = catalog("m1", "c1");

    cache.invalidate(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    // Caching a freshly inserted entity has no load window to guard.
    cache.put(catalog);

    Assertions.assertEquals(
        Optional.of(catalog), get(cache, catalog.nameIdentifier(), Entity.EntityType.CATALOG));
  }

  @Test
  void testStoreReadPatternUnderCacheLock() {
    RedisEntityCache nodeA = newNode();
    RedisEntityCache nodeB = newNode();
    TableEntity table = table("m1", "c1", "s1", "t1", "v1");
    EntityCacheKey key = EntityCacheKey.of(table.nameIdentifier(), Entity.EntityType.TABLE);

    // The RelationalEntityStore#get shape: lock, miss, load, put.
    TableEntity loaded =
        nodeA.withCacheLock(
            key,
            () -> {
              Optional<TableEntity> cached =
                  get(nodeA, table.nameIdentifier(), Entity.EntityType.TABLE);
              if (cached.isPresent()) {
                return cached.get();
              }
              nodeA.put(table);
              return table;
            });
    Assertions.assertEquals(table, loaded);
    Assertions.assertEquals(
        Optional.of(table), get(nodeB, table.nameIdentifier(), Entity.EntityType.TABLE));

    // The same shape with another node invalidating between the miss and the put.
    nodeA.invalidate(table.nameIdentifier(), Entity.EntityType.TABLE);
    nodeA.withCacheLock(
        key,
        () -> {
          Assertions.assertEquals(
              Optional.empty(), get(nodeA, table.nameIdentifier(), Entity.EntityType.TABLE));
          nodeB.invalidate(table.nameIdentifier(), Entity.EntityType.TABLE);
          nodeA.put(table);
          return null;
        });
    Assertions.assertFalse(nodeB.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @Test
  void testConcurrentReadersAndWriters() throws Exception {
    RedisEntityCache nodeA = newNode();
    RedisEntityCache nodeB = newNode();
    TableEntity table = table("m1", "c1", "s1", "t1", "v1");
    NameIdentifier ident = table.nameIdentifier();
    EntityCacheKey key = EntityCacheKey.of(ident, Entity.EntityType.TABLE);
    int readers = 6;
    int iterations = 150;
    ExecutorService pool = Executors.newFixedThreadPool(readers + 1);
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger hits = new AtomicInteger();
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int r = 0; r < readers; r++) {
        RedisEntityCache node = r % 2 == 0 ? nodeA : nodeB;
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  for (int i = 0; i < iterations; i++) {
                    TableEntity seen =
                        node.withCacheLock(
                            key,
                            () -> {
                              Optional<TableEntity> cached =
                                  get(node, ident, Entity.EntityType.TABLE);
                              if (cached.isPresent()) {
                                hits.incrementAndGet();
                                return cached.get();
                              }
                              node.put(table);
                              return table;
                            });
                    Assertions.assertEquals(table, seen);
                  }
                  return null;
                }));
      }
      futures.add(
          pool.submit(
              () -> {
                start.await();
                for (int i = 0; i < iterations; i++) {
                  nodeB.invalidate(ident, Entity.EntityType.TABLE);
                }
                return null;
              }));
      start.countDown();
      for (Future<?> future : futures) {
        future.get(2, TimeUnit.MINUTES);
      }
    } finally {
      pool.shutdownNow();
    }

    Assertions.assertTrue(hits.get() > 0, "expected at least one cache hit");
    nodeA.invalidate(ident, Entity.EntityType.TABLE);
    Assertions.assertFalse(nodeB.contains(ident, Entity.EntityType.TABLE));
  }

  @Test
  void testValuesExpireByTtl() {
    RedisEntityCache cache = newNode(300L);
    CatalogEntity catalog = catalog("m1", "c1");

    cache.put(catalog);
    Assertions.assertTrue(cache.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS)
        .until(() -> !cache.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertEquals(
        Optional.empty(), get(cache, catalog.nameIdentifier(), Entity.EntityType.CATALOG));
  }

  @Test
  void testClearAndSizeSpanEveryMetalake() {
    RedisEntityCache cache = newNode();
    for (int m = 1; m <= 5; m++) {
      cache.put(metalake("m" + m));
      cache.put(catalog("m" + m, "c1"));
      cache.put(schema("m" + m, "c1", "s1"));
    }
    Assertions.assertEquals(15, cache.size());

    cache.clear();

    Assertions.assertEquals(0, cache.size());
    for (int m = 1; m <= 5; m++) {
      Assertions.assertFalse(
          cache.contains(NameIdentifier.of("m" + m, "c1"), Entity.EntityType.CATALOG));
    }
  }

  @Test
  void testUndecodableEntryIsDiscarded() {
    RedisEntityCache cache = newNode();
    NameIdentifier ident = NameIdentifier.of("m1", "c1");
    String key = valueKey(ident, Entity.EntityType.CATALOG);

    rawClient().set(key, "not a serialized entity");
    rawClient().zadd(indexKey("m1"), 0, "m1.c1:CATALOG");

    Assertions.assertEquals(Optional.empty(), get(cache, ident, Entity.EntityType.CATALOG));
    Assertions.assertFalse(rawClient().exists(key));
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void testNonCacheableTypesAreNotStored() {
    RedisEntityCache cache = newNode();
    cache.put(TestUtil.getTestUserEntity());
    cache.put(TestUtil.getTestRoleEntity());
    cache.put(TestUtil.getTestGroupEntity());
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void testNamespacesAreIsolated() {
    RedisEntityCache first = newNode();
    CatalogEntity catalog = catalog("m1", "c1");
    first.put(catalog);

    namespace = namespace + "-other";
    RedisEntityCache second = newNode();
    Assertions.assertFalse(second.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertTrue(first.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
  }
}
