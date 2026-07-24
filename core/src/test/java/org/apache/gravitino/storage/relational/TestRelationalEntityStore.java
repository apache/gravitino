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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class TestRelationalEntityStore {

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
  void testUpdateInvalidatesCacheAfterBackendUpdate()
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException,
          IllegalAccessException {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog");
    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);

    Mockito.doAnswer(
            invocation -> {
              Mockito.verify(cache, Mockito.never()).invalidate(ident, Entity.EntityType.CATALOG);
              return null;
            })
        .when(backend)
        .update(eq(ident), eq(Entity.EntityType.CATALOG), any(Function.class));

    store.update(ident, null, Entity.EntityType.CATALOG, entity -> entity);

    InOrder inOrder = Mockito.inOrder(backend, cache);
    inOrder.verify(backend).update(eq(ident), eq(Entity.EntityType.CATALOG), any(Function.class));
    inOrder.verify(cache).invalidate(ident, Entity.EntityType.CATALOG);
  }

  @Test
  void testDeleteInvalidatesCacheAfterBackendDelete()
      throws IOException, NoSuchEntityException, IllegalAccessException {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog");
    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);

    Mockito.doAnswer(
            invocation -> {
              Mockito.verify(cache, Mockito.never()).invalidate(ident, Entity.EntityType.CATALOG);
              return true;
            })
        .when(backend)
        .delete(ident, Entity.EntityType.CATALOG, true);

    Assertions.assertTrue(store.delete(ident, Entity.EntityType.CATALOG, true));

    InOrder inOrder = Mockito.inOrder(backend, cache);
    inOrder.verify(backend).delete(ident, Entity.EntityType.CATALOG, true);
    inOrder.verify(cache).invalidate(ident, Entity.EntityType.CATALOG);
  }

  @Test
  void testInsertRelationInvalidatesCacheAfterBackendInsert()
      throws IOException, IllegalAccessException {
    NameIdentifier src = NameIdentifier.of("metalake", "catalog", "schema", "table1");
    NameIdentifier dst = NameIdentifier.of("metalake", "tag1");
    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);

    Mockito.doAnswer(
            invocation -> {
              Mockito.verify(cache, Mockito.never())
                  .invalidate(
                      src,
                      Entity.EntityType.TABLE,
                      SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL);
              Mockito.verify(cache, Mockito.never())
                  .invalidate(
                      dst,
                      Entity.EntityType.TAG,
                      SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL);
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
    inOrder
        .verify(cache)
        .invalidate(
            src, Entity.EntityType.TABLE, SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL);
    inOrder
        .verify(cache)
        .invalidate(
            dst, Entity.EntityType.TAG, SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL);
  }

  @Test
  void testUpdateEntityRelationsInvalidatesCacheAfterBackendUpdate()
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException,
          IllegalAccessException {
    NameIdentifier src = NameIdentifier.of("metalake", "catalog", "schema", "table1");
    NameIdentifier destToAdd = NameIdentifier.of("metalake", "tag1");
    NameIdentifier destToRemove = NameIdentifier.of("metalake", "tag2");
    NameIdentifier[] destEntitiesToAdd = new NameIdentifier[] {destToAdd};
    NameIdentifier[] destEntitiesToRemove = new NameIdentifier[] {destToRemove};
    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);

    Mockito.doAnswer(
            invocation -> {
              Mockito.verify(cache, Mockito.never())
                  .invalidate(
                      src,
                      Entity.EntityType.TABLE,
                      SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL);
              Mockito.verify(cache, Mockito.never())
                  .invalidate(
                      destToAdd,
                      Entity.EntityType.TABLE,
                      SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL);
              Mockito.verify(cache, Mockito.never())
                  .invalidate(
                      destToRemove,
                      Entity.EntityType.TABLE,
                      SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL);
              return List.of();
            })
        .when(backend)
        .updateEntityRelations(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            src,
            Entity.EntityType.TABLE,
            destEntitiesToAdd,
            destEntitiesToRemove);

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
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            src,
            Entity.EntityType.TABLE,
            destEntitiesToAdd,
            destEntitiesToRemove);
    inOrder
        .verify(cache)
        .invalidate(
            src, Entity.EntityType.TABLE, SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL);
    inOrder
        .verify(cache)
        .invalidate(
            destToAdd,
            Entity.EntityType.TABLE,
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL);
    inOrder
        .verify(cache)
        .invalidate(
            destToRemove,
            Entity.EntityType.TABLE,
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL);
  }

  /**
   * Verifies that {@link RelationalEntityStore#batchListEntitiesByRelation} does not hold a cache
   * lock across the backend call: with the backend blocked mid-call, a concurrent {@code invalidate}
   * on the same key must still complete.
   */
  @Test
  void testBatchListDoesNotHoldCacheLockAcrossBackendCall()
      throws IOException, IllegalAccessException, InterruptedException {
    SupportsRelationOperations.Type relType =
        SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL;
    Entity.EntityType identType = Entity.EntityType.TABLE;
    NameIdentifier src = NameIdentifier.of("metalake", "catalog", "schema", "table1");

    Config config = new Config(false) {};
    CaffeineEntityCache realCache = new CaffeineEntityCache(config);
    FieldUtils.writeField(store, "cache", realCache, true);

    CountDownLatch backendEntered = new CountDownLatch(1);
    CountDownLatch releaseBackend = new CountDownLatch(1);
    Mockito.when(backend.batchListEntitiesByRelation(eq(relType), any(List.class), eq(identType)))
        .thenAnswer(
            invocation -> {
              backendEntered.countDown();
              // Hold the backend "DB" call open to simulate a slow round-trip.
              releaseBackend.await(10, TimeUnit.SECONDS);
              return new ArrayList<RelationalEntity<?>>();
            });

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<List<RelationalEntity<?>>> listFuture =
          executor.submit(
              () -> store.batchListEntitiesByRelation(relType, List.of(src), identType));

      // Wait until the list is blocked inside the backend call.
      Assertions.assertTrue(
          backendEntered.await(5, TimeUnit.SECONDS), "backend call should have been entered");

      // A concurrent cache mutation on the same key must not be blocked by the in-flight list.
      Future<Boolean> invalidateFuture =
          executor.submit(
              () -> {
                realCache.invalidate(src, identType, relType);
                return Boolean.TRUE;
              });
      Assertions.assertDoesNotThrow(
          () -> invalidateFuture.get(5, TimeUnit.SECONDS),
          "concurrent cache invalidate must not block on the in-flight batch list");

      releaseBackend.countDown();
      Assertions.assertDoesNotThrow(() -> listFuture.get(5, TimeUnit.SECONDS));
    } finally {
      releaseBackend.countDown();
      executor.shutdownNow();
    }
  }
}
