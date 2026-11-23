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
package org.apache.gravitino.storage.relational;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationQuery;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.cache.BaseEntityCache;
import org.apache.gravitino.cache.CacheReadGate;
import org.apache.gravitino.cache.CachedEntityIdResolver;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.cache.Coherence;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.cache.EntityCacheKey;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.EntityIdResolver;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class TestRelationalEntityStore {

  private static final AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

  private RelationalEntityStore store;
  private RelationalBackend backend;

  @BeforeEach
  void setUp() throws IllegalAccessException {
    store = new RelationalEntityStore();
    backend = Mockito.mock(RelationalBackend.class);

    Config config = new Config(false) {};
    config.set(Configs.CACHE_ENABLED, false);

    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", Mockito.spy(new NoOpsCache(config)), true);
  }

  @Test
  void testUncachedInsertStillInvokesRelatedInvalidationAfterEpochChanges() throws Exception {
    BaseEntityCache cache = Mockito.mock(BaseEntityCache.class);
    FieldUtils.writeField(store, "cache", cache, true);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "model");
    ModelEntity entity = Mockito.mock(ModelEntity.class);
    Mockito.when(entity.type()).thenReturn(Entity.EntityType.MODEL);
    Mockito.when(entity.nameIdentifier()).thenReturn(ident);
    Mockito.doAnswer(
            invocation -> {
              store.clearCache();
              return null;
            })
        .when(backend)
        .insert(entity, false);
    store.put(entity, false);
    // Epoch advanced during insert, so publication is fenced; related invalidation still ran via
    // beginPendingWrite / the final invalidateCache path.
    Mockito.verify(cache, Mockito.atLeastOnce()).invalidate(ident, Entity.EntityType.MODEL);
  }

  @Test
  void testUpdateDropsCacheBeforeBackendUpdate()
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException,
          IllegalAccessException {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog");
    EntityCacheKey key = EntityCacheKey.of(ident, Entity.EntityType.CATALOG);
    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);
    @SuppressWarnings("unchecked")
    Map<EntityCacheKey, AtomicInteger> pendingWrites =
        (Map<EntityCacheKey, AtomicInteger>) FieldUtils.readField(store, "pendingWrites", true);
    AtomicBoolean invalidatedBeforeBackend = new AtomicBoolean(false);
    AtomicBoolean pendingDuringBackend = new AtomicBoolean(false);

    CatalogEntity updated = Mockito.mock(CatalogEntity.class);
    Mockito.when(updated.nameIdentifier()).thenReturn(ident);

    Mockito.doAnswer(
            invocation -> {
              Mockito.verify(cache).invalidate(ident, Entity.EntityType.CATALOG);
              invalidatedBeforeBackend.set(true);
              AtomicInteger count = pendingWrites.get(key);
              pendingDuringBackend.set(count != null && count.get() > 0);
              @SuppressWarnings("unchecked")
              Function<CatalogEntity, CatalogEntity> updater = invocation.getArgument(2);
              return updater.apply(updated);
            })
        .when(backend)
        .update(eq(ident), eq(Entity.EntityType.CATALOG), any(Function.class));

    store.update(ident, null, Entity.EntityType.CATALOG, entity -> entity);

    Assertions.assertTrue(invalidatedBeforeBackend.get());
    Assertions.assertTrue(pendingDuringBackend.get());
    Assertions.assertFalse(pendingWrites.containsKey(key));
  }

  @Test
  void testRenamePendingBlocksNewIdentBeforeCommit()
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException,
          IllegalAccessException {
    NameIdentifier oldIdent = NameIdentifier.of("metalake", "catalog", "schema", "old");
    NameIdentifier newIdent = NameIdentifier.of("metalake", "catalog", "schema", "new");
    EntityCacheKey newKey = EntityCacheKey.of(newIdent, Entity.EntityType.TOPIC);
    TopicEntity renamed = Mockito.mock(TopicEntity.class);
    Mockito.when(renamed.nameIdentifier()).thenReturn(newIdent);
    Mockito.when(renamed.type()).thenReturn(Entity.EntityType.TOPIC);

    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);
    @SuppressWarnings("unchecked")
    Map<EntityCacheKey, AtomicInteger> pendingWrites =
        (Map<EntityCacheKey, AtomicInteger>) FieldUtils.readField(store, "pendingWrites", true);
    AtomicBoolean newPendingBeforeReturn = new AtomicBoolean(false);
    AtomicBoolean newInvalidatedBeforeReturn = new AtomicBoolean(false);

    Mockito.doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<TopicEntity, TopicEntity> updater = invocation.getArgument(2);
              TopicEntity result = updater.apply(renamed);
              AtomicInteger count = pendingWrites.get(newKey);
              newPendingBeforeReturn.set(count != null && count.get() > 0);
              Mockito.verify(cache).invalidate(newIdent, Entity.EntityType.TOPIC);
              newInvalidatedBeforeReturn.set(true);
              return result;
            })
        .when(backend)
        .update(eq(oldIdent), eq(Entity.EntityType.TOPIC), any(Function.class));

    Assertions.assertSame(
        renamed, store.update(oldIdent, null, Entity.EntityType.TOPIC, entity -> renamed));

    Assertions.assertTrue(newPendingBeforeReturn.get());
    Assertions.assertTrue(newInvalidatedBeforeReturn.get());
    Assertions.assertFalse(pendingWrites.containsKey(newKey));
    Assertions.assertFalse(
        pendingWrites.containsKey(EntityCacheKey.of(oldIdent, Entity.EntityType.TOPIC)));
  }

  @Test
  void testRenameBeginPendingRollsBackWhenInvalidateFails()
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException,
          IllegalAccessException {
    NameIdentifier oldIdent = NameIdentifier.of("metalake", "catalog", "schema", "old");
    NameIdentifier newIdent = NameIdentifier.of("metalake", "catalog", "schema", "new");
    EntityCacheKey newKey = EntityCacheKey.of(newIdent, Entity.EntityType.TOPIC);
    TopicEntity renamed = Mockito.mock(TopicEntity.class);
    Mockito.when(renamed.nameIdentifier()).thenReturn(newIdent);

    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);
    @SuppressWarnings("unchecked")
    Map<EntityCacheKey, AtomicInteger> pendingWrites =
        (Map<EntityCacheKey, AtomicInteger>) FieldUtils.readField(store, "pendingWrites", true);
    @SuppressWarnings("unchecked")
    Map<String, AtomicInteger> pendingHierarchyPrefixes =
        (Map<String, AtomicInteger>) FieldUtils.readField(store, "pendingHierarchyPrefixes", true);

    Mockito.doReturn(true).when(cache).invalidate(eq(oldIdent), eq(Entity.EntityType.TOPIC));
    Mockito.doThrow(new RuntimeException("invalidate failed"))
        .when(cache)
        .invalidate(eq(newIdent), eq(Entity.EntityType.TOPIC));

    Mockito.doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<TopicEntity, TopicEntity> updater = invocation.getArgument(2);
              return updater.apply(renamed);
            })
        .when(backend)
        .update(eq(oldIdent), eq(Entity.EntityType.TOPIC), any(Function.class));

    Assertions.assertThrows(
        RuntimeException.class,
        () -> store.update(oldIdent, null, Entity.EntityType.TOPIC, entity -> renamed));

    Assertions.assertFalse(pendingWrites.containsKey(newKey));
    Assertions.assertFalse(
        pendingWrites.containsKey(EntityCacheKey.of(oldIdent, Entity.EntityType.TOPIC)));
    Assertions.assertTrue(pendingHierarchyPrefixes.isEmpty());
  }

  @Test
  void testClearCacheKeepsPendingRegistrations() throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog");
    EntityCacheKey key = EntityCacheKey.of(ident, Entity.EntityType.CATALOG);
    @SuppressWarnings("unchecked")
    Map<EntityCacheKey, AtomicInteger> pendingWrites =
        (Map<EntityCacheKey, AtomicInteger>) FieldUtils.readField(store, "pendingWrites", true);
    @SuppressWarnings("unchecked")
    Map<String, AtomicInteger> pendingHierarchyPrefixes =
        (Map<String, AtomicInteger>) FieldUtils.readField(store, "pendingHierarchyPrefixes", true);
    pendingWrites.put(key, new AtomicInteger(1));
    for (String prefix : CaffeineEntityCache.descendantPrefixes(ident, Entity.EntityType.CATALOG)) {
      pendingHierarchyPrefixes.put(prefix, new AtomicInteger(1));
    }

    store.clearCache();

    Assertions.assertEquals(1, pendingWrites.get(key).get());
    Assertions.assertFalse(pendingHierarchyPrefixes.isEmpty());
  }

  @Test
  void testExistsAndBatchGetSkipCacheWhilePublishing() throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "topic");
    EntityCacheKey key = EntityCacheKey.of(ident, Entity.EntityType.TOPIC);
    TopicEntity cached = Mockito.mock(TopicEntity.class);
    Mockito.when(cached.nameIdentifier()).thenReturn(ident);
    Mockito.when(cached.type()).thenReturn(Entity.EntityType.TOPIC);

    EntityCache cache = Mockito.mock(EntityCache.class);
    FieldUtils.writeField(store, "cache", cache, true);
    Mockito.when(cache.contains(ident, Entity.EntityType.TOPIC)).thenReturn(true);
    Mockito.when(cache.getIfPresent(ident, Entity.EntityType.TOPIC))
        .thenReturn(Optional.of(cached));
    Mockito.when(
            cache.withCacheLock(any(EntityCacheKey.class), any(EntityCache.ThrowingSupplier.class)))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              EntityCache.ThrowingSupplier<?, Exception> action = invocation.getArgument(1);
              return action.get();
            });
    Mockito.when(backend.exists(ident, Entity.EntityType.TOPIC)).thenReturn(false);
    Mockito.when(backend.batchGet(List.of(ident), Entity.EntityType.TOPIC)).thenReturn(List.of());

    @SuppressWarnings("unchecked")
    Map<EntityCacheKey, AtomicInteger> publishingWrites =
        (Map<EntityCacheKey, AtomicInteger>) FieldUtils.readField(store, "publishingWrites", true);
    publishingWrites.put(key, new AtomicInteger(1));

    Assertions.assertFalse(store.exists(ident, Entity.EntityType.TOPIC));
    Assertions.assertTrue(
        store.batchGet(List.of(ident), Entity.EntityType.TOPIC, TopicEntity.class).isEmpty());
    Mockito.verify(backend).exists(ident, Entity.EntityType.TOPIC);
    Mockito.verify(backend).batchGet(List.of(ident), Entity.EntityType.TOPIC);
  }

  @Test
  void testCachedEntityIdResolverSkipsCacheWhenGateDisallows() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "topic");
    TopicEntity cachedEntity = Mockito.mock(TopicEntity.class);
    Mockito.when(cachedEntity.id()).thenReturn(1L);
    EntityCache cache = Mockito.mock(EntityCache.class);
    Mockito.when(cache.getIfPresent(ident, Entity.EntityType.TOPIC))
        .thenReturn(Optional.of(cachedEntity));
    EntityIdResolver underlying = Mockito.mock(EntityIdResolver.class);
    Mockito.when(underlying.getEntityId(ident, Entity.EntityType.TOPIC)).thenReturn(99L);

    AtomicBoolean allowCache = new AtomicBoolean(false);
    CacheReadGate gate = (id, type) -> allowCache.get();
    EntityIdResolver resolver = new CachedEntityIdResolver(cache, underlying, gate);

    Assertions.assertEquals(99L, resolver.getEntityId(ident, Entity.EntityType.TOPIC));
    Mockito.verify(underlying).getEntityId(ident, Entity.EntityType.TOPIC);
    Mockito.verify(cache, Mockito.never()).getIfPresent(ident, Entity.EntityType.TOPIC);

    allowCache.set(true);
    Assertions.assertEquals(1L, resolver.getEntityId(ident, Entity.EntityType.TOPIC));
    Mockito.verify(cache).getIfPresent(ident, Entity.EntityType.TOPIC);
  }

  @Test
  void testEndPendingWriteAdvancesEpochBeforeReleasingPrefixes() throws Exception {
    NameIdentifier schema = NameIdentifier.of("metalake", "catalog", "schema");
    NameIdentifier table = NameIdentifier.of("metalake", "catalog", "schema", "table");
    EntityCacheKey schemaKey = EntityCacheKey.of(schema, Entity.EntityType.SCHEMA);
    @SuppressWarnings("unchecked")
    Map<EntityCacheKey, AtomicInteger> pendingWrites =
        (Map<EntityCacheKey, AtomicInteger>) FieldUtils.readField(store, "pendingWrites", true);
    @SuppressWarnings("unchecked")
    Map<String, AtomicInteger> pendingHierarchyPrefixes =
        (Map<String, AtomicInteger>) FieldUtils.readField(store, "pendingHierarchyPrefixes", true);
    AtomicLong epoch = (AtomicLong) FieldUtils.readField(store, "cacheInvalidationEpoch", true);

    pendingWrites.put(schemaKey, new AtomicInteger(1));
    for (String prefix : CaffeineEntityCache.descendantPrefixes(schema, Entity.EntityType.SCHEMA)) {
      pendingHierarchyPrefixes.put(prefix, new AtomicInteger(1));
    }
    long epochBefore = epoch.get();
    Assertions.assertTrue(
        (boolean)
            org.apache.commons.lang3.reflect.MethodUtils.invokeMethod(
                store, true, "isPendingWrite", table, Entity.EntityType.TABLE));

    org.apache.commons.lang3.reflect.MethodUtils.invokeMethod(
        store, true, "endPendingWrite", schema, Entity.EntityType.SCHEMA);

    Assertions.assertTrue(epoch.get() > epochBefore);
    Assertions.assertFalse(pendingWrites.containsKey(schemaKey));
    Assertions.assertTrue(pendingHierarchyPrefixes.isEmpty());
    Assertions.assertFalse(
        (boolean)
            org.apache.commons.lang3.reflect.MethodUtils.invokeMethod(
                store, true, "isPendingWrite", table, Entity.EntityType.TABLE));
  }

  @Test
  void testGetDoesNotPublishWhileWritePending() throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "topic");
    EntityCacheKey key = EntityCacheKey.of(ident, Entity.EntityType.TOPIC);
    TopicEntity entity = Mockito.mock(TopicEntity.class);
    Mockito.when(entity.nameIdentifier()).thenReturn(ident);
    Mockito.when(entity.type()).thenReturn(Entity.EntityType.TOPIC);
    Mockito.when(backend.get(ident, Entity.EntityType.TOPIC)).thenReturn(entity);

    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);
    @SuppressWarnings("unchecked")
    Map<EntityCacheKey, AtomicInteger> pendingWrites =
        (Map<EntityCacheKey, AtomicInteger>) FieldUtils.readField(store, "pendingWrites", true);
    pendingWrites.put(key, new AtomicInteger(1));

    Assertions.assertSame(entity, store.get(ident, Entity.EntityType.TOPIC, TopicEntity.class));
    Mockito.verify(cache, Mockito.never()).put(entity);
  }

  @Test
  void testGetDoesNotPublishWhileAncestorWritePending() throws Exception {
    NameIdentifier catalog = NameIdentifier.of("metalake", "catalog");
    NameIdentifier topic = NameIdentifier.of("metalake", "catalog", "schema", "topic");
    TopicEntity entity = Mockito.mock(TopicEntity.class);
    Mockito.when(entity.nameIdentifier()).thenReturn(topic);
    Mockito.when(entity.type()).thenReturn(Entity.EntityType.TOPIC);
    Mockito.when(backend.get(topic, Entity.EntityType.TOPIC)).thenReturn(entity);

    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);
    @SuppressWarnings("unchecked")
    Map<String, AtomicInteger> pendingHierarchyPrefixes =
        (Map<String, AtomicInteger>) FieldUtils.readField(store, "pendingHierarchyPrefixes", true);
    for (String prefix :
        CaffeineEntityCache.descendantPrefixes(catalog, Entity.EntityType.CATALOG)) {
      pendingHierarchyPrefixes.put(prefix, new AtomicInteger(1));
    }

    Assertions.assertSame(entity, store.get(topic, Entity.EntityType.TOPIC, TopicEntity.class));
    Mockito.verify(cache, Mockito.never()).put(entity);
  }

  @Test
  void testOverwriteDropsCacheBeforeBackendInsert()
      throws IOException, EntityAlreadyExistsException, IllegalAccessException {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "topic");
    EntityCacheKey key = EntityCacheKey.of(ident, Entity.EntityType.TOPIC);
    TopicEntity topic = Mockito.mock(TopicEntity.class);
    Mockito.when(topic.nameIdentifier()).thenReturn(ident);
    Mockito.when(topic.type()).thenReturn(Entity.EntityType.TOPIC);
    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);
    @SuppressWarnings("unchecked")
    Map<EntityCacheKey, AtomicInteger> pendingWrites =
        (Map<EntityCacheKey, AtomicInteger>) FieldUtils.readField(store, "pendingWrites", true);
    AtomicBoolean invalidatedBeforeBackend = new AtomicBoolean(false);
    AtomicBoolean pendingDuringBackend = new AtomicBoolean(false);

    Mockito.doAnswer(
            invocation -> {
              Mockito.verify(cache).invalidate(ident, Entity.EntityType.TOPIC);
              invalidatedBeforeBackend.set(true);
              AtomicInteger count = pendingWrites.get(key);
              pendingDuringBackend.set(count != null && count.get() > 0);
              return null;
            })
        .when(backend)
        .insert(topic, true);

    store.put(topic, true);

    Assertions.assertTrue(invalidatedBeforeBackend.get());
    Assertions.assertTrue(pendingDuringBackend.get());
    Assertions.assertFalse(pendingWrites.containsKey(key));
    Mockito.verify(cache, Mockito.never()).put(topic);
  }

  @Test
  void testNonOverwritePutDropsStaleCacheBeforeInsertAndPublishesAfter()
      throws IOException, EntityAlreadyExistsException, IllegalAccessException {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "topic");
    EntityCacheKey key = EntityCacheKey.of(ident, Entity.EntityType.TOPIC);
    TopicEntity created = Mockito.mock(TopicEntity.class);
    Mockito.when(created.nameIdentifier()).thenReturn(ident);
    Mockito.when(created.type()).thenReturn(Entity.EntityType.TOPIC);
    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);
    @SuppressWarnings("unchecked")
    Map<EntityCacheKey, AtomicInteger> pendingWrites =
        (Map<EntityCacheKey, AtomicInteger>) FieldUtils.readField(store, "pendingWrites", true);
    AtomicBoolean invalidatedBeforeBackend = new AtomicBoolean(false);
    AtomicBoolean pendingDuringBackend = new AtomicBoolean(false);

    Mockito.doAnswer(
            invocation -> {
              Mockito.verify(cache).invalidate(ident, Entity.EntityType.TOPIC);
              invalidatedBeforeBackend.set(true);
              AtomicInteger count = pendingWrites.get(key);
              pendingDuringBackend.set(count != null && count.get() > 0);
              return null;
            })
        .when(backend)
        .insert(created, false);

    store.put(created, false);

    Assertions.assertTrue(invalidatedBeforeBackend.get());
    Assertions.assertTrue(pendingDuringBackend.get());
    Assertions.assertFalse(pendingWrites.containsKey(key));
    Mockito.verify(cache).put(created);
  }

  @Test
  void testIsPendingWriteLooksUpAncestorPrefixes() throws Exception {
    NameIdentifier catalog = NameIdentifier.of("metalake", "catalog");
    NameIdentifier table = NameIdentifier.of("metalake", "catalog", "schema", "table");
    NameIdentifier other = NameIdentifier.of("metalake", "other", "schema", "table");
    @SuppressWarnings("unchecked")
    Map<String, AtomicInteger> pendingHierarchyPrefixes =
        (Map<String, AtomicInteger>) FieldUtils.readField(store, "pendingHierarchyPrefixes", true);
    for (String prefix :
        CaffeineEntityCache.descendantPrefixes(catalog, Entity.EntityType.CATALOG)) {
      pendingHierarchyPrefixes.put(prefix, new AtomicInteger(1));
    }

    Assertions.assertTrue(
        (boolean)
            org.apache.commons.lang3.reflect.MethodUtils.invokeMethod(
                store, true, "isPendingWrite", table, Entity.EntityType.TABLE));
    Assertions.assertFalse(
        (boolean)
            org.apache.commons.lang3.reflect.MethodUtils.invokeMethod(
                store, true, "isPendingWrite", other, Entity.EntityType.TABLE));
  }

  @Test
  void testDeleteDropsCacheBeforeBackendDelete()
      throws IOException, NoSuchEntityException, IllegalAccessException {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog");
    EntityCacheKey key = EntityCacheKey.of(ident, Entity.EntityType.CATALOG);
    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);
    @SuppressWarnings("unchecked")
    Map<EntityCacheKey, AtomicInteger> pendingWrites =
        (Map<EntityCacheKey, AtomicInteger>) FieldUtils.readField(store, "pendingWrites", true);
    AtomicBoolean invalidatedBeforeBackend = new AtomicBoolean(false);
    AtomicBoolean pendingDuringBackend = new AtomicBoolean(false);

    Mockito.doAnswer(
            invocation -> {
              Mockito.verify(cache).invalidate(ident, Entity.EntityType.CATALOG);
              invalidatedBeforeBackend.set(true);
              AtomicInteger count = pendingWrites.get(key);
              pendingDuringBackend.set(count != null && count.get() > 0);
              return true;
            })
        .when(backend)
        .delete(ident, Entity.EntityType.CATALOG, true);

    Assertions.assertTrue(store.delete(ident, Entity.EntityType.CATALOG, true));

    Assertions.assertTrue(invalidatedBeforeBackend.get());
    Assertions.assertTrue(pendingDuringBackend.get());
    Assertions.assertFalse(pendingWrites.containsKey(key));
  }

  @Test
  void testInsertDomainWithNewBackingTagInvalidatesTagAfterBackendInsert()
      throws IOException, IllegalAccessException {
    DomainEntity domain =
        DomainEntity.builder()
            .withId(1L)
            .withName("Finance")
            .withNamespace(NamespaceUtil.ofDomain("metalake"))
            .withStatus(Domain.Status.DRAFT)
            .withAuditInfo(AUDIT_INFO)
            .build();
    TagEntity tag =
        TagEntity.builder()
            .withId(2L)
            .withName("finance_tag")
            .withNamespace(NamespaceUtil.ofTag("metalake"))
            .withAllowedValues(new String[0])
            .withAuditInfo(AUDIT_INFO)
            .build();
    NameIdentifier tagIdent = tag.nameIdentifier();
    Consumer<LockedDomainState> check = state -> {};
    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);

    Mockito.doAnswer(
            invocation -> {
              Mockito.verify(cache, Mockito.never()).invalidate(tagIdent, Entity.EntityType.TAG);
              return null;
            })
        .when(backend)
        .insertDomainWithNewBackingTag(domain, tag, check);

    store.insertDomainWithNewBackingTag(domain, tag, check);

    InOrder inOrder = Mockito.inOrder(backend, cache);
    inOrder.verify(backend).insertDomainWithNewBackingTag(domain, tag, check);
    inOrder.verify(cache).invalidate(tagIdent, Entity.EntityType.TAG);

    // A failed insert leaves no new tag, so nothing is invalidated.
    Mockito.clearInvocations(cache);
    Mockito.doThrow(new TagAlreadyExistsException("Tag finance_tag already exists"))
        .when(backend)
        .insertDomainWithNewBackingTag(domain, tag, check);
    Assertions.assertThrows(
        TagAlreadyExistsException.class,
        () -> store.insertDomainWithNewBackingTag(domain, tag, check));
    Mockito.verify(cache, Mockito.never()).invalidate(any(), any());
  }

  @Test
  void testInsertRelationInvalidatesCacheAfterBackendInsert()
      throws IOException, IllegalAccessException {
    NameIdentifier src = NameIdentifier.of("metalake", "catalog", "schema", "table1");
    NameIdentifier dst = NameIdentifier.of("metalake", "tag1");
    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);

    Mockito.doAnswer(
            invocation -> {
              Mockito.verify(cache, Mockito.never()).invalidate(src, Entity.EntityType.TABLE);
              Mockito.verify(cache, Mockito.never()).invalidate(dst, Entity.EntityType.TAG);
              return null;
            })
        .when(backend)
        .insertRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            src,
            Entity.EntityType.TABLE,
            dst,
            Entity.EntityType.TAG,
            true);

    store.insertRelation(
        SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
        src,
        Entity.EntityType.TABLE,
        dst,
        Entity.EntityType.TAG,
        true);

    InOrder inOrder = Mockito.inOrder(backend, cache);
    inOrder
        .verify(backend)
        .insertRelation(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            src,
            Entity.EntityType.TABLE,
            dst,
            Entity.EntityType.TAG,
            true);
    inOrder.verify(cache).invalidate(src, Entity.EntityType.TABLE);
    inOrder.verify(cache).invalidate(dst, Entity.EntityType.TAG);
  }

  @Test
  void testUpdateEntityRelationsInvalidatesDestinationTypeAfterBackendUpdate()
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException,
          IllegalAccessException {
    NameIdentifier src = NameIdentifier.of("metalake", "catalog", "schema", "table1");
    NameIdentifier destToAdd = NameIdentifier.of("metalake", "dest1");
    NameIdentifier destToRemove = NameIdentifier.of("metalake", "dest2");
    NameIdentifier[] destEntitiesToAdd = new NameIdentifier[] {destToAdd};
    NameIdentifier[] destEntitiesToRemove = new NameIdentifier[] {destToRemove};
    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);

    Mockito.doAnswer(
            invocation -> {
              Mockito.verify(cache, Mockito.never()).invalidate(src, Entity.EntityType.TABLE);
              Mockito.verify(cache, Mockito.never()).invalidate(destToAdd, Entity.EntityType.TAG);
              Mockito.verify(cache, Mockito.never())
                  .invalidate(destToRemove, Entity.EntityType.TAG);
              return List.of();
            })
        .when(backend)
        .updateEntityRelations(
            eq(SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
            eq(src),
            eq(Entity.EntityType.TABLE),
            any(NameIdentifier[].class),
            any(NameIdentifier[].class));
    store.updateEntityRelations(
        SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
        src,
        Entity.EntityType.TABLE,
        destEntitiesToAdd,
        destEntitiesToRemove);

    InOrder inOrder = Mockito.inOrder(backend, cache);
    inOrder
        .verify(backend)
        .updateEntityRelations(
            eq(SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
            eq(src),
            eq(Entity.EntityType.TABLE),
            any(NameIdentifier[].class),
            any(NameIdentifier[].class));
    inOrder.verify(cache).invalidate(src, Entity.EntityType.TABLE);
    inOrder.verify(cache).invalidate(destToAdd, Entity.EntityType.TAG);
    inOrder.verify(cache).invalidate(destToRemove, Entity.EntityType.TAG);
  }

  @Test
  void testUpdateEntityRelationsRejectsUnsupportedRelationType() {
    NameIdentifier src = NameIdentifier.of("metalake", "catalog", "schema", "table1");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            store.updateEntityRelations(
                SupportsRelationOperations.Type.OWNER_REL,
                src,
                Entity.EntityType.TABLE,
                new NameIdentifier[0],
                new NameIdentifier[0]));
    Mockito.verifyNoInteractions(backend);
  }

  @Test
  void testListEntitiesByRelationWithRelationValueDelegatesToBackend() throws IOException {
    NameIdentifier tag = NameIdentifier.of("metalake", "tag1");
    RelationQuery query =
        RelationQuery.of(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            tag,
            Entity.EntityType.TAG,
            true,
            "dev");

    store.listEntitiesByRelation(query);

    Mockito.verify(backend).listEntitiesByRelation(query);
  }

  @Test
  void testUpdateRelationWithValuesInvalidatesCacheAfterBackendUpdate()
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException,
          IllegalAccessException {
    NameIdentifier src = NameIdentifier.of("metalake", "catalog", "schema", "table1");
    NameIdentifier destToAdd = NameIdentifier.of("metalake", "tag1");
    NameIdentifier destToRemove = NameIdentifier.of("metalake", "tag2");
    RelationEdgeTarget[] destEntitiesToAdd =
        new RelationEdgeTarget[] {RelationEdgeTarget.of(destToAdd, Entity.EntityType.TAG, "dev")};
    RelationEdgeTarget[] destEntitiesToRemove =
        new RelationEdgeTarget[] {
          RelationEdgeTarget.of(destToRemove, Entity.EntityType.TAG, "prod")
        };
    RelationUpdate update =
        RelationUpdate.of(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            src,
            Entity.EntityType.TABLE,
            destEntitiesToAdd,
            destEntitiesToRemove);
    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);

    Mockito.doAnswer(
            invocation -> {
              Mockito.verify(cache, Mockito.never()).invalidate(src, Entity.EntityType.TABLE);
              Mockito.verify(cache, Mockito.never()).invalidate(destToAdd, Entity.EntityType.TAG);
              Mockito.verify(cache, Mockito.never())
                  .invalidate(destToRemove, Entity.EntityType.TAG);
              return List.of();
            })
        .when(backend)
        .updateEntityRelations(update);

    store.updateEntityRelations(update);

    InOrder inOrder = Mockito.inOrder(backend, cache);
    inOrder.verify(backend).updateEntityRelations(update);
    inOrder.verify(cache).invalidate(src, Entity.EntityType.TABLE);
    inOrder.verify(cache).invalidate(destToAdd, Entity.EntityType.TAG);
    inOrder.verify(cache).invalidate(destToRemove, Entity.EntityType.TAG);
  }

  @Test
  void testUpdateRelationRejectsMismatchedTargetTypeBeforeBackendUpdate() {
    NameIdentifier src = NameIdentifier.of("metalake", "catalog", "schema", "table1");
    NameIdentifier tag = NameIdentifier.of("metalake", "tag1");
    RelationUpdate update =
        RelationUpdate.of(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            src,
            Entity.EntityType.TABLE,
            new RelationEdgeTarget[] {RelationEdgeTarget.of(tag, Entity.EntityType.TABLE, "dev")},
            new RelationEdgeTarget[0]);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> store.updateEntityRelations(update));
    Mockito.verifyNoInteractions(backend);
  }

  @Test
  void testLocalPerNodeCacheRegistersChangeLogListener() throws IllegalAccessException {
    EntityChangeLogPoller poller = givenCacheWithCoherence(Coherence.LOCAL_PER_NODE);

    store.registerCacheChangeLogListener();

    // A per-node cache holds its own copy, so it must replay changes made on the other nodes.
    EntityCacheChangeLogListener listener =
        (EntityCacheChangeLogListener)
            FieldUtils.readField(store, "entityCacheChangeLogListener", true);
    Assertions.assertNotNull(listener);
    Mockito.verify(poller).registerListener(listener);
  }

  @Test
  void testSharedCacheDoesNotRegisterChangeLogListener() throws IllegalAccessException {
    EntityChangeLogPoller poller = givenCacheWithCoherence(Coherence.SHARED);

    store.registerCacheChangeLogListener();

    // A shared cache has a single cluster-wide copy, so there is nothing per-node to invalidate.
    Assertions.assertNull(FieldUtils.readField(store, "entityCacheChangeLogListener", true));
    Mockito.verify(poller, Mockito.never()).registerListener(Mockito.any());
  }

  @Test
  void testCacheDisabledDoesNotRegisterChangeLogListener() throws IllegalAccessException {
    EntityChangeLogPoller poller = givenCacheWithCoherence(Coherence.NONE);

    store.registerCacheChangeLogListener();

    // Nothing is cached, so there is nothing to invalidate either.
    Assertions.assertNull(FieldUtils.readField(store, "entityCacheChangeLogListener", true));
    Mockito.verify(poller, Mockito.never()).registerListener(Mockito.any());
  }

  /**
   * Installs a cache reporting the given coherence mode plus a mock poller, and returns the poller
   * so the caller can assert on listener registration.
   */
  private EntityChangeLogPoller givenCacheWithCoherence(Coherence coherence)
      throws IllegalAccessException {
    EntityCache cache = Mockito.mock(EntityCache.class);
    Mockito.when(cache.coherence()).thenReturn(coherence);
    EntityChangeLogPoller poller = Mockito.mock(EntityChangeLogPoller.class);

    FieldUtils.writeField(store, "cache", cache, true);
    FieldUtils.writeField(store, "entityChangeLogPoller", poller, true);
    return poller;
  }
}
