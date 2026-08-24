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
import java.util.List;
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
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.cache.Coherence;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

  @ParameterizedTest
  @CsvSource({"TAG_METADATA_OBJECT_REL, TAG", "POLICY_METADATA_OBJECT_REL, POLICY"})
  void testUpdateEntityRelationsInvalidatesDestinationTypeAfterBackendUpdate(
      SupportsRelationOperations.Type relationType, Entity.EntityType destinationType)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException,
          IllegalAccessException {
    NameIdentifier src = NameIdentifier.of("metalake", "catalog", "schema", "table1");
    NameIdentifier destToAdd = NameIdentifier.of("metalake", "dest1");
    NameIdentifier destToRemove = NameIdentifier.of("metalake", "dest2");
    NameIdentifier[] destEntitiesToAdd = new NameIdentifier[] {destToAdd};
    NameIdentifier[] destEntitiesToRemove = new NameIdentifier[] {destToRemove};
    NoOpsCache cache = (NoOpsCache) FieldUtils.readField(store, "cache", true);

    if (relationType == SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL) {
      Mockito.doAnswer(
              invocation -> {
                Mockito.verify(cache, Mockito.never()).invalidate(src, Entity.EntityType.TABLE);
                Mockito.verify(cache, Mockito.never()).invalidate(destToAdd, destinationType);
                Mockito.verify(cache, Mockito.never()).invalidate(destToRemove, destinationType);
                return List.of();
              })
          .when(backend)
          .updateEntityRelations(
              eq(relationType),
              eq(src),
              eq(Entity.EntityType.TABLE),
              any(NameIdentifier[].class),
              any(NameIdentifier[].class));
    } else {
      Mockito.doAnswer(
              invocation -> {
                Mockito.verify(cache, Mockito.never()).invalidate(src, Entity.EntityType.TABLE);
                Mockito.verify(cache, Mockito.never()).invalidate(destToAdd, destinationType);
                Mockito.verify(cache, Mockito.never()).invalidate(destToRemove, destinationType);
                return List.of();
              })
          .when(backend)
          .updateEntityRelations(any(RelationUpdate.class));
    }

    store.updateEntityRelations(
        relationType, src, Entity.EntityType.TABLE, destEntitiesToAdd, destEntitiesToRemove);

    InOrder inOrder = Mockito.inOrder(backend, cache);
    if (relationType == SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL) {
      inOrder
          .verify(backend)
          .updateEntityRelations(
              eq(relationType),
              eq(src),
              eq(Entity.EntityType.TABLE),
              any(NameIdentifier[].class),
              any(NameIdentifier[].class));
    } else {
      inOrder.verify(backend).updateEntityRelations(any(RelationUpdate.class));
    }
    inOrder.verify(cache).invalidate(src, Entity.EntityType.TABLE);
    inOrder.verify(cache).invalidate(destToAdd, destinationType);
    inOrder.verify(cache).invalidate(destToRemove, destinationType);
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
