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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsExternalIdOperations;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.cache.CacheFactory;
import org.apache.gravitino.cache.CachedEntityIdResolver;
import org.apache.gravitino.cache.Coherence;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.cache.EntityCacheKey;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.utils.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relation store to store entities. This means we can store entities in a relational store. I.e.,
 * MySQL, PostgreSQL, etc. If you want to use a different backend, you can implement the {@link
 * RelationalBackend} interface. The default JDBC backend is {@link JDBCBackend}.
 */
public class RelationalEntityStore
    implements EntityStore,
        SupportsRelationOperations,
        SupportsExternalIdOperations,
        SupportsEntityChangeLog {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelationalEntityStore.class);
  public static final ImmutableMap<String, String> RELATIONAL_BACKENDS =
      ImmutableMap.of(
          Configs.DEFAULT_ENTITY_RELATIONAL_STORE, JDBCBackend.class.getCanonicalName());
  private RelationalBackend backend;
  private RelationalGarbageCollector garbageCollector;
  private EntityChangeLogPoller entityChangeLogPoller;
  private EntityCache cache;

  // Non-null only for a LOCAL_PER_NODE cache, which needs cross-node invalidation. A SHARED cache
  // has a single cluster-wide copy, so there is nothing per-node to invalidate and no listener is
  // registered.
  @Nullable private EntityCacheChangeLogListener entityCacheChangeLogListener;

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

    // The change-log poller is a side module of the entity store: it polls the entity_change_log
    // table this store writes to, dispatches batches to registered listeners (e.g. for cross-node
    // cache invalidation), and prunes expired rows. Like the garbage collector, it is owned and
    // lifecycle-managed by the store itself.
    this.entityChangeLogPoller =
        new EntityChangeLogPoller(
            config.get(Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS),
            TimeUnit.SECONDS.toMillis(config.get(Configs.ENTITY_CHANGE_LOG_RETENTION_SECS)),
            TimeUnit.SECONDS.toMillis(config.get(Configs.ENTITY_CHANGE_LOG_CLEANUP_INTERVAL_SECS)));

    // The coherence gate: a LOCAL_PER_NODE cache keeps its own copy per node, so changes made on
    // other nodes must be replayed here through the change log. A SHARED cache (or a disabled
    // NoOpsCache) has nothing per-node to invalidate, so no listener is registered.
    if (cache.coherence() == Coherence.LOCAL_PER_NODE && !(cache instanceof NoOpsCache)) {
      this.entityCacheChangeLogListener = new EntityCacheChangeLogListener(cache);
      this.entityChangeLogPoller.registerListener(entityCacheChangeLogListener);
    }
    this.entityChangeLogPoller.start();
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
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    E updatedEntity = backend.update(ident, entityType, updater);
    cache.invalidate(ident, entityType);
    return updatedEntity;
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException {
    return cache.withCacheLock(
        EntityCacheKey.of(ident, entityType),
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
  public SupportsExternalIdOperations externalIdOperations() {
    return this;
  }

  @Override
  public <E extends Entity & HasIdentifier> E getByExternalId(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> type)
      throws NoSuchEntityException, IOException {
    return backend.getByExternalId(ident, entityType);
  }

  @Override
  public <E extends Entity & HasIdentifier> E updateByExternalId(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> type, Function<E, E> updater)
      throws NoSuchEntityException, IOException {
    E updatedEntity = backend.updateByExternalId(ident, entityType, updater);
    cache.invalidate(updatedEntity.nameIdentifier(), entityType);
    return updatedEntity;
  }

  @Override
  public boolean deleteByExternalId(NameIdentifier ident, Entity.EntityType entityType)
      throws IOException {
    NameIdentifier nameIdent = null;
    try {
      HasIdentifier entity = backend.getByExternalId(ident, entityType);
      nameIdent = entity.nameIdentifier();
      return backend.delete(nameIdent, entityType, false);
    } catch (NoSuchEntityException e) {
      LOGGER.warn(
          "The entity to be deleted by external id does not exist in the store: {}", ident, e);
      return false;
    } finally {
      if (nameIdent != null) {
        cache.invalidate(nameIdent, entityType);
      }
    }
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
      return deleted;
    } catch (NoSuchEntityException e) {
      return false;
    } finally {
      cache.invalidate(ident, entityType);
    }
  }

  @Override
  public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable) {
    throw new UnsupportedOperationException("Unsupported operation in relational entity store.");
  }

  @Override
  public void registerEntityChangeLogListener(EntityChangeLogListener listener) {
    entityChangeLogPoller.registerListener(listener);
  }

  @Override
  public void unregisterEntityChangeLogListener(EntityChangeLogListener listener) {
    entityChangeLogPoller.unregisterListener(listener);
  }

  @Override
  public void close() throws IOException {
    cache.clear();
    entityChangeLogPoller.close();
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
    return backend.listEntitiesByRelation(relType, nameIdentifier, identType, allFields);
  }

  @Override
  public List<RelationalEntity<?>> batchListEntitiesByRelation(
      Type relType, List<NameIdentifier> nameIdentifiers, Entity.EntityType identType)
      throws IOException {
    if (nameIdentifiers == null || nameIdentifiers.isEmpty()) {
      return new ArrayList<>();
    }
    return backend.batchListEntitiesByRelation(relType, nameIdentifiers, identType);
  }

  @Override
  public <E extends Entity & HasIdentifier> E getEntityByRelation(
      Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier destEntityIdent)
      throws IOException, NoSuchEntityException {
    List<E> backendEntities = backend.listEntitiesByRelation(relType, srcIdentifier, srcType, true);
    return backendEntities.stream()
        .filter(e -> e.nameIdentifier().equals(destEntityIdent))
        .findFirst()
        .orElseThrow(
            () -> new NoSuchEntityException("No such entity with ident: %s", destEntityIdent));
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
    // Relation results are no longer cached, but the entities on both sides may embed
    // relation-derived data (e.g. a user's role names), so drop their single-entity entries.
    cache.invalidate(srcIdentifier, srcType);
    cache.invalidate(dstIdentifier, dstType);
  }

  @Override
  public void batchInsertRelations(
      Type relType,
      List<NameIdentifier> srcIdentifiers,
      Entity.EntityType srcType,
      NameIdentifier dstIdentifier,
      Entity.EntityType dstType,
      boolean override)
      throws IOException {
    if (srcIdentifiers == null || srcIdentifiers.isEmpty()) {
      return;
    }
    backend.batchInsertRelations(
        relType, srcIdentifiers, srcType, dstIdentifier, dstType, override);
    for (NameIdentifier ident : srcIdentifiers) {
      cache.invalidate(ident, srcType);
    }
    cache.invalidate(dstIdentifier, dstType);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> updateEntityRelations(
      Type relType,
      NameIdentifier srcEntityIdent,
      Entity.EntityType srcEntityType,
      NameIdentifier[] destEntitiesToAdd,
      NameIdentifier[] destEntitiesToRemove)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    List<E> result =
        backend.updateEntityRelations(
            relType, srcEntityIdent, srcEntityType, destEntitiesToAdd, destEntitiesToRemove);

    // Invalidate after the backend write, not before: invalidating first opens a window where a
    // concurrent read could repopulate the cache with stale pre-commit data.
    cache.invalidate(srcEntityIdent, srcEntityType);
    for (NameIdentifier destToAdd : destEntitiesToAdd) {
      cache.invalidate(destToAdd, srcEntityType);
    }

    for (NameIdentifier destToRemove : destEntitiesToRemove) {
      cache.invalidate(destToRemove, srcEntityType);
    }

    return result;
  }

  @Override
  public int batchDelete(
      List<Pair<NameIdentifier, Entity.EntityType>> entitiesToDelete, boolean cascade)
      throws IOException {
    return backend.batchDelete(entitiesToDelete, cascade);
  }

  @Override
  public <E extends Entity & HasIdentifier> void batchPut(List<E> entities, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    backend.batchPut(entities, overwritten);
  }
}
