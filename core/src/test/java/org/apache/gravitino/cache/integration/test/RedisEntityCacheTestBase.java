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
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * Behavior of {@link RedisEntityCache} against a real Redis, shared by the standalone and cluster
 * suites. Each test uses its own key namespace, and "nodes" are separate cache instances over the
 * same Redis, which is exactly what two Gravitino servers sharing one cache are.
 *
 * <p>A fill is only performed after a miss on the same thread, the shape of every store read, so
 * the tests populate the cache through {@link #load} rather than a bare {@code put}.
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
      } catch (RuntimeException e) {
        // A node closed by the test itself cannot clear; its namespace is unique anyway.
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
    return newNode(config(ttlMs));
  }

  protected RedisEntityCache newNode(long ttlMs, long fenceTtlMs) {
    Config config = config(ttlMs);
    config.set(Configs.CACHE_REDIS_FENCE_TTL_MS, fenceTtlMs);
    return newNode(config);
  }

  protected RedisEntityCache newNode(Config config) {
    EntityCache cache = CacheFactory.getEntityCache(config);
    Assertions.assertInstanceOf(RedisEntityCache.class, cache);
    caches.add((RedisEntityCache) cache);
    return (RedisEntityCache) cache;
  }

  protected String valueKey(NameIdentifier ident, Entity.EntityType type) {
    return slotPrefix(ident) + "D:" + ident + ":" + type;
  }

  protected String indexKey(String metalake) {
    return namespace + ":{" + metalake + "}:IDX";
  }

  protected String fenceKey(NameIdentifier ident) {
    return slotPrefix(ident) + "F:" + ident;
  }

  private String slotPrefix(NameIdentifier ident) {
    String metalake = ident.hasNamespace() ? ident.namespace().level(0) : ident.name();
    return namespace + ":{" + metalake + "}:";
  }

  /** Every value key of this namespace, scanned through the raw client. */
  protected List<String> valueKeys() {
    List<String> keys = new ArrayList<>();
    ScanParams params = new ScanParams().match(namespace + ":{*}:D:*").count(500);
    String cursor = ScanParams.SCAN_POINTER_START;
    do {
      ScanResult<String> result = rawClient().scan(cursor, params);
      keys.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
    return keys;
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

  protected static <E extends Entity & HasIdentifier> Optional<E> get(
      EntityCache cache, NameIdentifier ident, Entity.EntityType type) {
    return cache.getIfPresent(ident, type);
  }

  /** The store read shape: a miss that records the fences, then the fill it bounds. */
  protected static <E extends Entity & HasIdentifier> void load(RedisEntityCache cache, E entity) {
    Assertions.assertEquals(
        Optional.empty(), get(cache, entity.nameIdentifier(), entity.type()), "expected a miss");
    cache.put(entity);
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

    load(nodeA, catalog);
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

    load(cache, metalake);
    load(cache, catalog1);
    load(cache, catalog10);
    load(cache, schema);
    load(cache, table);
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
    load(cache, metalake("m1"));
    load(cache, catalog("m1", "c1"));
    load(cache, schema("m1", "c1", "s1"));
    load(cache, table("m1", "c1", "s1", "t1", "v1"));
    load(cache, metalake("m2"));
    load(cache, catalog("m2", "c1"));

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

    load(cache, raw);
    load(cache, rawEvents);
    load(cache, nestedTable);
    load(cache, raw2);
    load(cache, raw2Table);

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
    // ...while node A commits an update, invalidates, and serves the new row.
    nodeA.invalidate(ident, Entity.EntityType.TABLE);
    load(nodeA, newTable);
    // Node B's late fill must not overwrite the new row with the old one.
    nodeB.put(oldTable);

    Assertions.assertEquals(Optional.of(newTable), get(nodeA, ident, Entity.EntityType.TABLE));
    Assertions.assertEquals(Optional.of(newTable), get(nodeB, ident, Entity.EntityType.TABLE));
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
  void testWriteWithoutAPrecedingMissIsNotCached() {
    RedisEntityCache cache = newNode();
    CatalogEntity catalog = catalog("m1", "c1");

    // Caching a freshly inserted entity has no miss that bounded its load window, so it fails
    // closed; the next read loads it under a fresh record.
    cache.put(catalog);

    Assertions.assertFalse(cache.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertEquals(0, cache.size());
    load(cache, catalog);
    Assertions.assertEquals(
        Optional.of(catalog), get(cache, catalog.nameIdentifier(), Entity.EntityType.CATALOG));
  }

  @Test
  void testInsertTimeWarmingCannotResurrectAConcurrentDelete() {
    RedisEntityCache nodeA = newNode();
    RedisEntityCache nodeB = newNode();
    CatalogEntity catalog = catalog("m1", "c1");

    // The store insert shape on node A: commit, then cache.put with no miss before it. Node B
    // deletes the row and invalidates in between; the delayed warming must not bring it back.
    nodeB.invalidate(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    nodeA.put(catalog);

    Assertions.assertFalse(nodeB.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertEquals(
        Optional.empty(), get(nodeB, catalog.nameIdentifier(), Entity.EntityType.CATALOG));
  }

  @Test
  void testMissesBeyondThePendingBoundFailClosed() {
    RedisEntityCache nodeA = newNode();
    RedisEntityCache nodeB = newNode();
    int misses = 1025;
    List<CatalogEntity> catalogs = new ArrayList<>(misses);
    for (int i = 0; i < misses; i++) {
      catalogs.add(catalog("m1", "c" + i));
    }

    // The batchGet shape: every miss is recorded before any fill, one more than the bound.
    for (CatalogEntity catalog : catalogs) {
      Assertions.assertEquals(
          Optional.empty(), get(nodeA, catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    }
    // The first record was evicted; node B invalidates that key in the meantime.
    nodeB.invalidate(catalogs.get(0).nameIdentifier(), Entity.EntityType.CATALOG);
    nodeA.put(catalogs.get(0));
    nodeA.put(catalogs.get(misses - 1));

    Assertions.assertFalse(
        nodeB.contains(catalogs.get(0).nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertTrue(
        nodeB.contains(catalogs.get(misses - 1).nameIdentifier(), Entity.EntityType.CATALOG));
  }

  @Test
  void testDecodeFailureThenInvalidationCannotBeRefilled() {
    RedisEntityCache nodeA = newNode();
    RedisEntityCache nodeB = newNode();
    CatalogEntity catalog = catalog("m1", "c1");
    NameIdentifier ident = catalog.nameIdentifier();
    String key = valueKey(ident, Entity.EntityType.CATALOG);

    rawClient().set(key, "not a serialized entity");
    rawClient().zadd(indexKey("m1"), 0, "m1.c1:CATALOG");

    // The undecodable entry is discarded, which is a miss that recorded no fences...
    Assertions.assertEquals(Optional.empty(), get(nodeA, ident, Entity.EntityType.CATALOG));
    Assertions.assertFalse(rawClient().exists(key));
    // ...so the fill that follows it, racing an invalidation on node B, is not performed.
    nodeB.invalidate(ident, Entity.EntityType.CATALOG);
    nodeA.put(catalog);

    Assertions.assertFalse(nodeB.contains(ident, Entity.EntityType.CATALOG));
    Assertions.assertEquals(0, nodeA.size());
    // A fresh read then loads it normally.
    load(nodeA, catalog);
    Assertions.assertEquals(Optional.of(catalog), get(nodeB, ident, Entity.EntityType.CATALOG));
  }

  @Test
  void testRecreatedFenceNeverRepeatsTheGenerationAnOlderReadObserved() throws Exception {
    long valueTtlMs = 100L;
    long fenceTtlMs = 200L;
    RedisEntityCache nodeA = newNode(valueTtlMs, fenceTtlMs);
    RedisEntityCache nodeB = newNode(valueTtlMs, fenceTtlMs);
    TableEntity oldTable = table("m1", "c1", "s1", "t1", "old");
    NameIdentifier ident = oldTable.nameIdentifier();

    // Node A drops the table once: the fence exists with the first generation.
    nodeA.invalidate(ident, Entity.EntityType.TABLE);
    String firstFence = rawClient().get(fenceKey(ident));
    // Node B misses just before that fence expires and starts a slow load of the old row.
    Assertions.assertEquals(Optional.empty(), get(nodeB, ident, Entity.EntityType.TABLE));
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(20, TimeUnit.MILLISECONDS)
        .until(() -> !rawClient().exists(fenceKey(ident)));
    // Another node commits an update and invalidates again, recreating the fence.
    nodeA.invalidate(ident, Entity.EntityType.TABLE);
    String secondFence = rawClient().get(fenceKey(ident));
    Assertions.assertNotEquals(firstFence, secondFence, "generations must never be reused");
    Assertions.assertTrue(Long.parseLong(secondFence) > Long.parseLong(firstFence));

    // Node B's stale fill: rejected both because its record is older than the fence lifetime and
    // because the fence it would compare against carries a new generation.
    nodeB.put(oldTable);
    Assertions.assertFalse(nodeA.contains(ident, Entity.EntityType.TABLE));
  }

  @Test
  void testAbsentFenceThatWasSetAndExpiredCannotAdmitAnOlderRead() throws Exception {
    long valueTtlMs = 100L;
    long fenceTtlMs = 200L;
    RedisEntityCache nodeA = newNode(valueTtlMs, fenceTtlMs);
    RedisEntityCache nodeB = newNode(valueTtlMs, fenceTtlMs);
    TableEntity oldTable = table("m1", "c1", "s1", "t1", "old");
    NameIdentifier ident = oldTable.nameIdentifier();

    // Node B misses while no fence exists at all, and starts a slow load of the old row.
    Assertions.assertEquals(Optional.empty(), get(nodeB, ident, Entity.EntityType.TABLE));
    // Node A commits an update and invalidates; the fence is set, then expires again.
    nodeA.invalidate(ident, Entity.EntityType.TABLE);
    Assertions.assertTrue(rawClient().exists(fenceKey(ident)));
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(20, TimeUnit.MILLISECONDS)
        .until(() -> !rawClient().exists(fenceKey(ident)));

    // Absent before, absent now: only the record's age tells the two apart, and it is too old.
    nodeB.put(oldTable);
    Assertions.assertFalse(nodeA.contains(ident, Entity.EntityType.TABLE));
  }

  @Test
  void testClearRejectsAnInFlightFillAndLeavesNoOrphanValue() {
    RedisEntityCache nodeA = newNode();
    RedisEntityCache nodeB = newNode();
    TableEntity table = table("m1", "c1", "s1", "t1", "v1");
    load(nodeA, catalog("m1", "c1"));

    // Node B misses and starts loading; node A clears in between; node B's fill arrives late.
    Assertions.assertEquals(
        Optional.empty(), get(nodeB, table.nameIdentifier(), Entity.EntityType.TABLE));
    nodeA.clear();
    nodeB.put(table);

    Assertions.assertFalse(nodeA.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertEquals(0, nodeA.size());
    // Nothing survives that a later hierarchical invalidation could not find.
    load(nodeB, table);
    nodeA.invalidate(NameIdentifier.of("m1"), Entity.EntityType.METALAKE);
    Assertions.assertEquals(ImmutableList.of(), valueKeys());
  }

  @Test
  void testExpiredIndexMembersAreReclaimedAndRefillsSurvive() {
    RedisEntityCache cache = newNode(200L);
    CatalogEntity c1 = catalog("m1", "c1");
    CatalogEntity c2 = catalog("m1", "c2");
    CatalogEntity c3 = catalog("m1", "c3");
    load(cache, c1);
    load(cache, c2);
    load(cache, c3);
    Assertions.assertEquals(3, cache.size());

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS)
        .until(() -> !cache.contains(c3.nameIdentifier(), Entity.EntityType.CATALOG));
    // The values expired on their own; sizing reaps the members they left behind.
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS)
        .until(() -> cache.size() == 0);
    Assertions.assertEquals(0, rawClient().zcard(indexKey("m1")));

    // A refill after expiry is indexed again and is not reaped while its value is live.
    load(cache, c2);
    Assertions.assertEquals(1, cache.size());
    Assertions.assertTrue(cache.contains(c2.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertEquals(1, rawClient().zcard(indexKey("m1")));
  }

  @Test
  void testCloseReleasesThisNodeAndKeepsTheSharedEntries() {
    RedisEntityCache nodeA = newNode();
    RedisEntityCache nodeB = newNode();
    CatalogEntity catalog = catalog("m1", "c1");
    load(nodeA, catalog);

    nodeB.close();

    // Node B's client is released: it can no longer reach Redis at all.
    Assertions.assertThrows(
        RuntimeException.class,
        () -> nodeB.invalidate(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    // Node A's entry is untouched.
    Assertions.assertTrue(
        rawClient().exists(valueKey(catalog.nameIdentifier(), Entity.EntityType.CATALOG)));
    Assertions.assertEquals(
        Optional.of(catalog), get(nodeA, catalog.nameIdentifier(), Entity.EntityType.CATALOG));
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
  void testConcurrentReadersNeverObserveAVersionOlderThanACompletedInvalidation() throws Exception {
    RedisEntityCache nodeA = newNode();
    RedisEntityCache nodeB = newNode();
    NameIdentifier ident = NameIdentifier.of("m1", "c1", "s1", "t1");
    EntityCacheKey key = EntityCacheKey.of(ident, Entity.EntityType.TABLE);
    int readers = 6;
    int iterations = 150;
    ExecutorService pool = Executors.newFixedThreadPool(readers + 1);
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger hits = new AtomicInteger();
    // The "database": the version last committed, and the version whose invalidation completed.
    AtomicInteger committed = new AtomicInteger();
    AtomicInteger invalidated = new AtomicInteger();
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int r = 0; r < readers; r++) {
        RedisEntityCache node = r % 2 == 0 ? nodeA : nodeB;
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  for (int i = 0; i < iterations; i++) {
                    int floor = invalidated.get();
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
                              TableEntity loaded =
                                  table("m1", "c1", "s1", "t1", "v" + committed.get());
                              node.put(loaded);
                              return loaded;
                            });
                    int seenVersion = Integer.parseInt(seen.comment().substring(1));
                    // An invalidation that completed before this read began must not be undone:
                    // whatever is cached now was loaded after it, so it carries a newer version.
                    Assertions.assertTrue(
                        seenVersion >= floor,
                        "read version " + seenVersion + " older than invalidated " + floor);
                  }
                  return null;
                }));
      }
      futures.add(
          pool.submit(
              () -> {
                start.await();
                for (int i = 1; i <= iterations; i++) {
                  committed.set(i);
                  nodeB.invalidate(ident, Entity.EntityType.TABLE);
                  invalidated.set(i);
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

    load(cache, catalog);
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
      load(cache, metalake("m" + m));
      load(cache, catalog("m" + m, "c1"));
      load(cache, schema("m" + m, "c1", "s1"));
    }
    Assertions.assertEquals(15, cache.size());

    cache.clear();

    Assertions.assertEquals(0, cache.size());
    Assertions.assertEquals(ImmutableList.of(), valueKeys());
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
    load(first, catalog);

    namespace = namespace + "-other";
    RedisEntityCache second = newNode();
    Assertions.assertFalse(second.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertTrue(first.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
  }

  @Test
  void testClearingANamespaceLeavesALongerNamespaceItPrefixesAlone() {
    RedisEntityCache first = newNode();
    CatalogEntity catalog = catalog("m1", "c1");
    load(first, catalog);
    String firstNamespace = namespace;

    namespace = firstNamespace + ":other";
    RedisEntityCache second = newNode();
    load(second, catalog);
    Assertions.assertEquals(1, second.size());

    first.clear();

    Assertions.assertEquals(0, first.size());
    Assertions.assertFalse(first.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertEquals(1, second.size());
    Assertions.assertTrue(second.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
  }

  @Test
  void testNamespaceWithGlobMetacharactersIsRejected() {
    namespace = namespace + "*";
    Assertions.assertThrows(RuntimeException.class, this::newNode);
    namespace = "it-[a]";
    Assertions.assertThrows(RuntimeException.class, this::newNode);
  }
}
