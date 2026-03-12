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

import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.cache.CacheFactory;
import org.apache.gravitino.cache.CachedEntityIdResolver;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.cache.EntityCacheRelationKey;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.cache.invalidation.CacheDomain;
import org.apache.gravitino.cache.invalidation.CacheInvalidationEvent;
import org.apache.gravitino.cache.invalidation.CacheInvalidationOperation;
import org.apache.gravitino.cache.invalidation.CacheInvalidationService;
import org.apache.gravitino.cache.invalidation.CacheInvalidationServiceFactory;
import org.apache.gravitino.cache.invalidation.EntityCacheInvalidationKey;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.utils.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relation store to store entities. This means we can store entities in a relational store. I.e.,
 * MySQL, PostgreSQL, etc. If you want to use a different backend, you can implement the {@link
 * RelationalBackend} interface
 */
public class RelationalEntityStore implements EntityStore, SupportsRelationOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelationalEntityStore.class);
  public static final ImmutableMap<String, String> RELATIONAL_BACKENDS =
      ImmutableMap.of(
          Configs.DEFAULT_ENTITY_RELATIONAL_STORE, JDBCBackend.class.getCanonicalName());
  private RelationalBackend backend;
  private RelationalGarbageCollector garbageCollector;
  private EntityCache cache;
  private CacheInvalidationService cacheInvalidationService;
  private String cacheInvalidationHandlerId;

  @VisibleForTesting
  public EntityCache getCache() {
    return cache;
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    if (config.get(Configs.CACHE_ENABLED)) {
      this.cache = CacheFactory.getEntityCache(config);
      EntityIdService.initialize(
          new CachedEntityIdResolver(cache, new RelationalEntityStoreIdResolver()));
    } else {
      this.cache = new NoOpsCache(config);
      EntityIdService.initialize(new RelationalEntityStoreIdResolver());
    }

    this.backend = createRelationalEntityBackend(config);
    this.garbageCollector = new RelationalGarbageCollector(backend, config);
    this.garbageCollector.start();

    this.cacheInvalidationService = CacheInvalidationServiceFactory.getOrCreate(config);
    this.cacheInvalidationHandlerId = "relational-entity-store-" + System.identityHashCode(this);
    cacheInvalidationService.registerHandler(
        CacheDomain.ENTITY, cacheInvalidationHandlerId, this::handleEntityInvalidationEvent);
  }

  private RelationalBackend createRelationalEntityBackend(Config config) {
    String backendName = config.get(ENTITY_RELATIONAL_STORE);
    String className =
        RELATIONAL_BACKENDS.getOrDefault(backendName, Configs.DEFAULT_ENTITY_RELATIONAL_STORE);

    try {
      RelationalBackend relationalBackend =
          (RelationalBackend) Class.forName(className).getDeclaredConstructor().newInstance();
      relationalBackend.initialize(config);

      return relationalBackend;
    } catch (Exception e) {
      LOGGER.error(
          "Failed to create and initialize RelationalBackend by name '{}'.", backendName, e);
      throw new RuntimeException(
          "Failed to create and initialize RelationalBackend by name: " + backendName, e);
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> type, Entity.EntityType entityType) throws IOException {
    return backend.list(namespace, entityType, false);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> type, Entity.EntityType entityType, boolean allFields)
      throws IOException {
    return backend.list(namespace, entityType, allFields);
  }

  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) throws IOException {
    boolean existsInCache = cache.contains(ident, entityType);
    return existsInCache || backend.exists(ident, entityType);
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    backend.insert(e, overwritten);
    cache.put(e);
    publishOrApplyEntityUpsert(e.nameIdentifier(), e.type());
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    E entity = backend.update(ident, entityType, updater);
    publishOrApplyEntityInvalidation(ident, entityType);
    return entity;
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException {
    return cache.withCacheLock(
        EntityCacheRelationKey.of(ident, entityType),
        () -> {
          Optional<E> entityFromCache = cache.getIfPresent(ident, entityType);
          if (entityFromCache.isPresent()) {
            return entityFromCache.get();
          }

          E entity = backend.get(ident, entityType);
          cache.put(entity);
          return entity;
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> batchGet(
      List<NameIdentifier> idents, Entity.EntityType entityType, Class<E> clazz) {
    List<E> allEntities = new ArrayList<>();
    List<NameIdentifier> noCacheIdents =
        idents.stream()
            .filter(
                ident -> {
                  Optional<E> entity = cache.getIfPresent(ident, entityType);
                  entity.ifPresent(allEntities::add);
                  return entity.isEmpty();
                })
            .toList();
    List<E> fetchEntities = backend.batchGet(noCacheIdents, entityType);
    for (E entity : fetchEntities) {
      cache.put(entity);
      allEntities.add(entity);
    }
    return allEntities;
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException {
    try {
      boolean deleted = backend.delete(ident, entityType, cascade);
      if (deleted) {
        publishOrApplyEntityInvalidation(ident, entityType);
      }
      return deleted;
    } catch (NoSuchEntityException e) {
      return false;
    }
  }

  @Override
  public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable) {
    throw new UnsupportedOperationException("Unsupported operation in relational entity store.");
  }

  @Override
  public void close() throws IOException {
    cache.clear();
    if (cacheInvalidationService != null && cacheInvalidationHandlerId != null) {
      cacheInvalidationService.unregisterHandler(CacheDomain.ENTITY, cacheInvalidationHandlerId);
    }
    garbageCollector.close();
    backend.close();
  }

  @Override
  public SupportsRelationOperations relationOperations() {
    return this;
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> listEntitiesByRelation(
      Type relType, NameIdentifier nameIdentifier, Entity.EntityType identType, boolean allFields)
      throws IOException {
    return cache.withCacheLock(
        EntityCacheRelationKey.of(nameIdentifier, identType, relType),
        () -> {
          Optional<List<E>> entities = cache.getIfPresent(relType, nameIdentifier, identType);
          if (entities.isPresent()) {
            return entities.get();
          }

          // Use allFields=true to cache complete entities
          List<E> backendEntities =
              backend.listEntitiesByRelation(relType, nameIdentifier, identType, true);

          cache.put(nameIdentifier, identType, relType, backendEntities);

          return backendEntities;
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> E getEntityByRelation(
      Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier destEntityIdent)
      throws IOException, NoSuchEntityException {
    return cache.withCacheLock(
        EntityCacheRelationKey.of(srcIdentifier, srcType, relType),
        () -> {
          Optional<List<E>> entities = cache.getIfPresent(relType, srcIdentifier, srcType);
          if (entities.isPresent()) {
            return entities.get().stream()
                .filter(e -> e.nameIdentifier().equals(destEntityIdent))
                .findFirst()
                .orElseThrow(
                    () ->
                        new NoSuchEntityException(
                            "No such entity with ident: %s", destEntityIdent));
          }

          // Use allFields=true to cache complete entities
          List<E> backendEntities =
              backend.listEntitiesByRelation(relType, srcIdentifier, srcType, true);

          E r =
              backendEntities.stream()
                  .filter(e -> e.nameIdentifier().equals(destEntityIdent))
                  .findFirst()
                  .orElseThrow(
                      () ->
                          new NoSuchEntityException(
                              "No such entity with ident: %s", destEntityIdent));

          cache.put(srcIdentifier, srcType, relType, backendEntities);

          return r;
        });
  }

  @Override
  public void insertRelation(
      SupportsRelationOperations.Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier dstIdentifier,
      Entity.EntityType dstType,
      boolean override)
      throws IOException {
    backend.insertRelation(relType, srcIdentifier, srcType, dstIdentifier, dstType, override);
    publishOrApplyRelationInvalidation(srcIdentifier, srcType, relType);
    publishOrApplyRelationInvalidation(dstIdentifier, dstType, relType);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> updateEntityRelations(
      Type relType,
      NameIdentifier srcEntityIdent,
      Entity.EntityType srcEntityType,
      NameIdentifier[] destEntitiesToAdd,
      NameIdentifier[] destEntitiesToRemove)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {

    // We need to clear the cache of the source entity and all destination entities being added or
    // removed. This ensures that any subsequent reads will fetch the updated relations from the
    // backend. For example, if we are adding a tag to table, we need to invalidate the cache for
    // that table and the tag being added or removed. Otherwise, we might return stale data if we
    // list all tags for that table or all tables for that tag.
    List<E> updated =
        backend.updateEntityRelations(
            relType, srcEntityIdent, srcEntityType, destEntitiesToAdd, destEntitiesToRemove);
    publishOrApplyRelationInvalidation(srcEntityIdent, srcEntityType, relType);
    for (NameIdentifier destToAdd : destEntitiesToAdd) {
      publishOrApplyRelationInvalidation(destToAdd, srcEntityType, relType);
    }

    for (NameIdentifier destToRemove : destEntitiesToRemove) {
      publishOrApplyRelationInvalidation(destToRemove, srcEntityType, relType);
    }
    return updated;
  }

  @Override
  public int batchDelete(
      List<Pair<NameIdentifier, Entity.EntityType>> entitiesToDelete, boolean cascade)
      throws IOException {
    int result = backend.batchDelete(entitiesToDelete, cascade);
    for (Pair<NameIdentifier, Entity.EntityType> entity : entitiesToDelete) {
      publishOrApplyEntityInvalidation(entity.getLeft(), entity.getRight());
    }
    return result;
  }

  @Override
  public <E extends Entity & HasIdentifier> void batchPut(List<E> entities, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    backend.batchPut(entities, overwritten);
    entities.forEach(entity -> publishOrApplyEntityUpsert(entity.nameIdentifier(), entity.type()));
  }

  private void publishOrApplyEntityInvalidation(
      NameIdentifier ident, Entity.EntityType entityType) {
    if (!cacheInvalidationService.isEnabled()) {
      cache.invalidate(ident, entityType);
      return;
    }

    cacheInvalidationService.publish(
        CacheInvalidationEvent.of(
            CacheDomain.ENTITY,
            CacheInvalidationOperation.INVALIDATE_KEY,
            EntityCacheInvalidationKey.of(ident, entityType, null),
            cacheInvalidationService.localNodeId()));
  }

  private void publishOrApplyRelationInvalidation(
      NameIdentifier ident, Entity.EntityType entityType, SupportsRelationOperations.Type relType) {
    if (!cacheInvalidationService.isEnabled()) {
      cache.invalidate(ident, entityType, relType);
      return;
    }

    cacheInvalidationService.publish(
        CacheInvalidationEvent.of(
            CacheDomain.ENTITY,
            CacheInvalidationOperation.INVALIDATE_KEY,
            EntityCacheInvalidationKey.of(ident, entityType, relType),
            cacheInvalidationService.localNodeId()));
  }

  private void publishOrApplyEntityUpsert(NameIdentifier ident, Entity.EntityType entityType) {
    if (!cacheInvalidationService.isEnabled()) {
      return;
    }

    cacheInvalidationService.publish(
        CacheInvalidationEvent.of(
            CacheDomain.ENTITY,
            CacheInvalidationOperation.UPSERT_KEY,
            EntityCacheInvalidationKey.of(ident, entityType, null),
            cacheInvalidationService.localNodeId()));
  }

  private void handleEntityInvalidationEvent(CacheInvalidationEvent event) {
    if (event.operation() == CacheInvalidationOperation.INVALIDATE_ALL) {
      cache.clear();
      return;
    }

    if (event.operation() != CacheInvalidationOperation.INVALIDATE_KEY
        || !(event.key() instanceof EntityCacheInvalidationKey)) {
      return;
    }

    EntityCacheInvalidationKey key = (EntityCacheInvalidationKey) event.key();
    if (key.relationType() == null) {
      cache.invalidate(key.identifier(), key.entityType());
      return;
    }

    cache.invalidate(key.identifier(), key.entityType(), key.relationType());
  }
}
