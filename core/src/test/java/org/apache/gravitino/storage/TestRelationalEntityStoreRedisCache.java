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
package org.apache.gravitino.storage;

import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cache.RedisEntityCache;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;

/**
 * Drives two {@link RelationalEntityStore}s that share one Redis entity cache over one H2 database,
 * the shape of two Gravitino servers, through the real insert, get, batchGet, update and close
 * paths, with a latch holding one store between its backend load and its cache fill while the other
 * store commits and invalidates.
 */
@Tag("gravitino-docker-test")
public class TestRelationalEntityStoreRedisCache extends AbstractEntityStorageTest {

  private static final String IMAGE = "redis:7.2-alpine";
  private static final int PORT = 6379;
  private static final String CATALOG = "catalog_for_redis_cache_store_test";
  private static final String SCHEMA = "schema_for_redis_cache_store_test";

  private static GenericContainer<?> redis;
  private static String address;
  private static JedisPooled rawClient;

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
  private String namespace;
  // The H2 file outlives a test, so every test works in its own metalake.
  private String metalake;
  private EntityStore storeA;
  private EntityStore storeB;

  @BeforeAll
  static void startRedis() {
    redis = new GenericContainer<>(DockerImageName.parse(IMAGE)).withExposedPorts(PORT);
    redis.start();
    address = redis.getHost() + ":" + redis.getMappedPort(PORT);
    rawClient = new JedisPooled(HostAndPort.from(address));
  }

  @AfterAll
  static void stopRedis() {
    if (rawClient != null) {
      rawClient.close();
    }
    if (redis != null) {
      redis.stop();
    }
  }

  @BeforeEach
  void startStores() throws Exception {
    String suffix = UUID.randomUUID().toString().substring(0, 8);
    namespace = "store-" + suffix;
    metalake = "metalake_redis_cache_" + suffix;
    LatchedRedisEntityCache.reset();
    storeA = newStore();
    storeB = newStore();
  }

  @AfterEach
  void stopStores() throws Exception {
    LatchedRedisEntityCache.reset();
    try {
      // Drops the tables through the session factory the two stores share, so it must run before
      // the stores close that factory. A test that already closed a store closed the factory
      // with it; every test works in its own metalake, so leftover rows are harmless.
      destroy("h2");
    } catch (IllegalStateException alreadyClosed) {
      // The factory is gone; nothing left to drop.
    } finally {
      if (storeA != null) {
        storeA.close();
      }
      if (storeB != null) {
        storeB.close();
      }
    }
  }

  private EntityStore newStore() throws Exception {
    Config config = Mockito.mock(Config.class);
    init("h2", config);
    // The factory loads the class by its binary name, which for a nested class is Outer$Inner.
    Mockito.when(config.get(Configs.CACHE_IMPLEMENTATION))
        .thenReturn(LatchedRedisEntityCache.class.getName());
    Mockito.when(config.get(Configs.CACHE_REDIS_ADDRESS)).thenReturn(address);
    Mockito.when(config.get(Configs.CACHE_REDIS_CLUSTER)).thenReturn(false);
    Mockito.when(config.get(Configs.CACHE_REDIS_NAMESPACE)).thenReturn(namespace);
    Mockito.when(config.get(Configs.CACHE_REDIS_SERIALIZER)).thenReturn("kryo");
    Mockito.when(config.get(Configs.CACHE_REDIS_TIMEOUT_MS)).thenReturn(2_000);
    Mockito.when(config.get(Configs.CACHE_REDIS_FENCE_TTL_MS)).thenReturn(0L);
    EntityStore store = EntityStoreFactory.createEntityStore(config);
    store.initialize(config);
    return store;
  }

  private void createHierarchy(EntityStore store) throws Exception {
    BaseMetalake baseMetalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalake, auditInfo);
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalake),
            CATALOG,
            auditInfo);
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalake, CATALOG),
            SCHEMA,
            auditInfo);
    store.put(baseMetalake, false);
    store.put(catalog, false);
    store.put(schema, false);
  }

  private TableEntity newTable(String name, String comment) {
    return TableEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(NamespaceUtil.ofTable(metalake, CATALOG, SCHEMA))
        .withComment(comment)
        .withColumns(Collections.emptyList())
        .withAuditInfo(auditInfo)
        .build();
  }

  private static TableEntity withComment(TableEntity table, String comment) {
    return TableEntity.builder()
        .withId(table.id())
        .withName(table.name())
        .withNamespace(table.namespace())
        .withComment(comment)
        .withColumns(table.columns())
        .withAuditInfo(table.auditInfo())
        .build();
  }

  private String valueKey(NameIdentifier ident) {
    return namespace + ":{" + metalake + "}:D:" + ident + ":TABLE";
  }

  @Test
  void testAnOlderLoadCannotUndoASuccessfulInvalidationOnGet() throws Exception {
    createHierarchy(storeA);
    TableEntity v1 = newTable("t_get", "v1");
    storeA.put(v1, false);
    NameIdentifier ident = v1.nameIdentifier();
    Assertions.assertFalse(rawClient.exists(valueKey(ident)), "insert must not warm the cache");

    // Store A's get misses, loads v1 from H2, and pauses just before filling the cache with it.
    LatchedRedisEntityCache.pauseNextFillOf(ident);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<TableEntity> slowRead =
          pool.submit(() -> storeA.get(ident, Entity.EntityType.TABLE, TableEntity.class));
      Assertions.assertTrue(LatchedRedisEntityCache.awaitLoad(30, TimeUnit.SECONDS));

      // Store B commits v2 and invalidates the shared entry while A still holds v1.
      storeB.update(ident, TableEntity.class, Entity.EntityType.TABLE, t -> withComment(t, "v2"));
      Assertions.assertEquals(
          "v2", storeB.get(ident, Entity.EntityType.TABLE, TableEntity.class).comment());

      // A resumes: it returns what it loaded, but its fill of v1 must be rejected.
      LatchedRedisEntityCache.resume();
      Assertions.assertEquals("v1", slowRead.get(30, TimeUnit.SECONDS).comment());
    } finally {
      pool.shutdownNow();
    }

    Assertions.assertEquals(
        "v2", storeA.get(ident, Entity.EntityType.TABLE, TableEntity.class).comment());
    Assertions.assertEquals(
        "v2", storeB.get(ident, Entity.EntityType.TABLE, TableEntity.class).comment());
  }

  @Test
  void testAnOlderLoadCannotUndoASuccessfulInvalidationOnBatchGet() throws Exception {
    createHierarchy(storeA);
    TableEntity v1 = newTable("t_batch", "v1");
    TableEntity other = newTable("t_other", "o1");
    storeA.put(v1, false);
    storeA.put(other, false);
    NameIdentifier ident = v1.nameIdentifier();
    List<NameIdentifier> idents = ImmutableList.of(ident, other.nameIdentifier());

    LatchedRedisEntityCache.pauseNextFillOf(ident);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<List<TableEntity>> slowBatch =
          pool.submit(() -> storeA.batchGet(idents, Entity.EntityType.TABLE, TableEntity.class));
      Assertions.assertTrue(LatchedRedisEntityCache.awaitLoad(30, TimeUnit.SECONDS));

      storeB.update(ident, TableEntity.class, Entity.EntityType.TABLE, t -> withComment(t, "v2"));

      LatchedRedisEntityCache.resume();
      List<TableEntity> loaded = slowBatch.get(30, TimeUnit.SECONDS);
      Assertions.assertEquals(2, loaded.size());
      Assertions.assertEquals(
          "v1",
          loaded.stream()
              .filter(t -> t.nameIdentifier().equals(ident))
              .findFirst()
              .get()
              .comment());
    } finally {
      pool.shutdownNow();
    }

    List<TableEntity> fresh = storeB.batchGet(idents, Entity.EntityType.TABLE, TableEntity.class);
    Assertions.assertEquals(
        "v2",
        fresh.stream().filter(t -> t.nameIdentifier().equals(ident)).findFirst().get().comment());
    Assertions.assertEquals(
        "v2", storeA.get(ident, Entity.EntityType.TABLE, TableEntity.class).comment());
  }

  @Test
  void testInsertDoesNotResurrectAConcurrentDelete() throws Exception {
    createHierarchy(storeA);
    TableEntity table = newTable("t_insert", "v1");
    NameIdentifier ident = table.nameIdentifier();

    // The insert commits on A; B deletes and invalidates before A's post-insert cache.put would
    // have published the row. The put is not performed, so nothing can be resurrected.
    storeA.put(table, false);
    Assertions.assertTrue(storeB.delete(ident, Entity.EntityType.TABLE, false));

    Assertions.assertFalse(rawClient.exists(valueKey(ident)));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> storeA.get(ident, Entity.EntityType.TABLE, TableEntity.class));
    Assertions.assertFalse(storeA.exists(ident, Entity.EntityType.TABLE));
  }

  @Test
  void testClosingOneStoreLeavesTheOtherStoresEntries() throws Exception {
    createHierarchy(storeA);
    TableEntity table = newTable("t_close", "v1");
    storeA.put(table, false);
    NameIdentifier ident = table.nameIdentifier();
    // A read fills the shared cache.
    Assertions.assertEquals(
        "v1", storeB.get(ident, Entity.EntityType.TABLE, TableEntity.class).comment());
    Assertions.assertTrue(rawClient.exists(valueKey(ident)));

    storeB.close();
    storeB = null;

    Assertions.assertTrue(rawClient.exists(valueKey(ident)), "close must not clear shared data");
    Assertions.assertEquals(
        "v1", storeA.get(ident, Entity.EntityType.TABLE, TableEntity.class).comment());
  }

  /**
   * A {@link RedisEntityCache} that can pause one thread between a chosen key's backend load and
   * the cache fill that follows it, so a test can order another store's commit and invalidation in
   * between.
   */
  public static class LatchedRedisEntityCache extends RedisEntityCache {

    private static final AtomicReference<NameIdentifier> pauseOn = new AtomicReference<>();
    private static volatile CountDownLatch loaded = new CountDownLatch(1);
    private static volatile CountDownLatch resume = new CountDownLatch(1);

    public LatchedRedisEntityCache(Config cacheConfig) {
      super(cacheConfig);
    }

    static void reset() {
      pauseOn.set(null);
      loaded = new CountDownLatch(1);
      resume = new CountDownLatch(1);
    }

    static void pauseNextFillOf(NameIdentifier ident) {
      loaded = new CountDownLatch(1);
      resume = new CountDownLatch(1);
      pauseOn.set(ident);
    }

    static boolean awaitLoad(long timeout, TimeUnit unit) throws InterruptedException {
      return loaded.await(timeout, unit);
    }

    static void resume() {
      resume.countDown();
    }

    @Override
    protected <E extends Entity & HasIdentifier> void doPut(E entity) {
      if (entity.nameIdentifier().equals(pauseOn.getAndSet(null))) {
        loaded.countDown();
        try {
          if (!resume.await(60, TimeUnit.SECONDS)) {
            throw new IllegalStateException(
                "test never resumed the paused fill of " + entity.nameIdentifier());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      super.doPut(entity);
    }
  }
}
