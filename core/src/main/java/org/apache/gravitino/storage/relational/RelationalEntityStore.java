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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationQuery;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsExternalIdOperations;
import org.apache.gravitino.SupportsIdOperations;
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
        SupportsIdOperations,
        SupportsEntityChangeLog {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelationalEntityStore.class);
  public static final ImmutableMap<String, String> RELATIONAL_BACKENDS =
      ImmutableMap.of(
          Configs.DEFAULT_ENTITY_RELATIONAL_STORE, JDBCBackend.class.getCanonicalName());
  private RelationalBackend backend;
  private RelationalGarbageCollector garbageCollector;
  private EntityChangeLogPoller entityChangeLogPoller;
  private EntityChangeLogCleaner entityChangeLogCleaner;
  private EntityCache cache;

  // Non-null only for a LOCAL_PER_NODE cache, which needs cross-node invalidation. SHARED and NONE
  // caches have no per-node copy to invalidate, so no listener is registered.
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

    // Polling and cleanup use separate single-threaded schedulers. Polling only dispatches changes
    // to local listeners, while cleanup independently removes records beyond the retention period.
    this.entityChangeLogPoller =
        new EntityChangeLogPoller(config.get(Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS));
    this.entityChangeLogCleaner =
        new EntityChangeLogCleaner(
            TimeUnit.SECONDS.toMillis(config.get(Configs.ENTITY_CHANGE_LOG_RETENTION_SECS)),
            TimeUnit.SECONDS.toMillis(config.get(Configs.ENTITY_CHANGE_LOG_CLEANUP_INTERVAL_SECS)),
            TimeUnit.SECONDS.toMillis(config.get(Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS)));

    registerCacheChangeLogListener();

    this.entityChangeLogPoller.start();
    this.entityChangeLogCleaner.start();
  }

  /**
   * The coherence gate: a {@link Coherence#LOCAL_PER_NODE} cache keeps its own copy per node, so
   * changes made on other nodes must be replayed here through the change log. {@link
   * Coherence#SHARED} and {@link Coherence#NONE} caches have nothing per-node to invalidate, so no
   * listener is registered.
   */
  @VisibleForTesting
  void registerCacheChangeLogListener() {
    if (cache.coherence() != Coherence.LOCAL_PER_NODE) {
      return;
    }

    this.entityCacheChangeLogListener = new EntityCacheChangeLogListener(cache);
    this.entityChangeLogPoller.registerListener(entityCacheChangeLogListener);
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
    if (overwritten) {
      // An overwrite is resolved by the database, which may keep the identity and version of the
      // row it already had. Caching the copy handed in here would publish values the stored row
      // does not carry, so the next read is served from the backend instead.
      cache.invalidate(e.nameIdentifier(), e.type());
    } else {
      cache.put(e);
    }
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
  public SupportsIdOperations idOperations() {
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
  public <E extends Entity & HasIdentifier> E getById(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> type)
      throws NoSuchEntityException, IOException {
    return backend.getById(ident, entityType);
  }

  @Override
  public <E extends Entity & HasIdentifier> E updateById(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> type, Function<E, E> updater)
      throws NoSuchEntityException, IOException {
    E updatedEntity = backend.updateById(ident, entityType, updater);
    cache.invalidate(updatedEntity.nameIdentifier(), entityType);
    return updatedEntity;
  }

  @Override
  public boolean deleteById(NameIdentifier ident, Entity.EntityType entityType) throws IOException {
    NameIdentifier nameIdent = null;
    try {
      HasIdentifier entity = backend.getById(ident, entityType);
      nameIdent = entity.nameIdentifier();
      return backend.delete(nameIdent, entityType, false);
    } catch (NoSuchEntityException e) {
      LOGGER.warn("The entity to be deleted by id does not exist in the store: {}", ident, e);
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
  public <E extends Entity & HasIdentifier> Optional<E> deleteAndGet(
      NameIdentifier ident,
      Entity.EntityType entityType,
      Class<E> clazz,
      Consumer<E> postDeleteAction)
      throws IOException {
    try {
      return backend.deleteAndGet(ident, entityType, clazz, postDeleteAction);
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
    // Keep shutting the remaining components down even if one of them fails, and tolerate a
    // half-finished initialize() that left some of them null.
    IOException failure = null;
    failure = closeComponent(failure, "entity cache", cache == null ? null : cache::clear);
    failure = closeComponent(failure, "entity change log poller", entityChangeLogPoller);
    failure = closeComponent(failure, "entity change log cleaner", entityChangeLogCleaner);
    failure = closeComponent(failure, "relational garbage collector", garbageCollector);
    failure = closeComponent(failure, "relational backend", backend);

    if (failure != null) {
      throw failure;
    }
  }

  private static IOException closeComponent(
      @Nullable IOException failure, String name, @Nullable AutoCloseable component) {
    if (component == null) {
      return failure;
    }

    try {
      component.close();
      return failure;
    } catch (Exception e) {
      LOGGER.warn("Failed to close {}", name, e);
      if (failure != null) {
        failure.addSuppressed(e);
        return failure;
      }
      return e instanceof IOException
          ? (IOException) e
          : new IOException("Failed to close " + name, e);
    }
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
    return backend.getEntityByRelation(relType, srcIdentifier, srcType, destEntityIdent);
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
    // Relation query results themselves are not cached, but both endpoints may be cached entities
    // (OWNER_REL, TAG_/POLICY_METADATA_OBJECT_REL and METADATA_OBJECT_ROLE_REL are keyed by
    // catalog/schema/table/... on the source side), so drop their entries conservatively: a
    // relation write can change data materialized into the endpoint entity. Note this is not free —
    // EntityCache#invalidate cascades over the identifier hierarchy, so invalidating a catalog also
    // drops every cached schema and table beneath it.
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
    // Invalidate both endpoints for the same reason as insertRelation, including the hierarchy
    // cascade noted there.
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
    RelationUpdate update =
        RelationUpdate.of(
            relType,
            srcEntityIdent,
            srcEntityType,
            toRelationEdgeTargets(relType, destEntitiesToAdd),
            toRelationEdgeTargets(relType, destEntitiesToRemove));
    if (relType != SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL) {
      return updateEntityRelations(update);
    }

    List<E> result =
        backend.updateEntityRelations(
            relType, srcEntityIdent, srcEntityType, destEntitiesToAdd, destEntitiesToRemove);
    Entity.EntityType targetEntityType = relationUpdateTargetType(relType);
    cache.invalidate(srcEntityIdent, srcEntityType);
    invalidateRelationTargetCache(targetEntityType, update.targetsToAdd());
    invalidateRelationTargetCache(targetEntityType, update.targetsToRemove());

    return result;
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> listEntitiesByRelation(RelationQuery query)
      throws IOException {
    if (query.relationValue().isPresent()) {
      return backend.listEntitiesByRelation(query);
    }

    return listEntitiesByRelation(
        query.relationType(),
        query.anchorIdentifier(),
        query.anchorEntityType(),
        query.allFields());
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> updateEntityRelations(RelationUpdate update)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    validateRelationTargetTypes(update);

    RelationEdgeTarget[] targetsToAdd = update.targetsToAdd();
    RelationEdgeTarget[] targetsToRemove = update.targetsToRemove();
    List<E> result = backend.updateEntityRelations(update);

    // Invalidate after the backend write, not before: invalidating first opens a window where a
    // concurrent read could repopulate the cache with stale pre-commit data.
    Entity.EntityType targetEntityType = relationUpdateTargetType(update.relationType());
    cache.invalidate(update.sourceIdentifier(), update.sourceEntityType());
    invalidateRelationTargetCache(targetEntityType, targetsToAdd);
    invalidateRelationTargetCache(targetEntityType, targetsToRemove);

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

  private void invalidateRelationTargetCache(
      Entity.EntityType targetEntityType, RelationEdgeTarget[] relationTargets) {
    for (RelationEdgeTarget relationTarget : relationTargets) {
      cache.invalidate(relationTarget.nameIdentifier(), targetEntityType);
    }
  }

  private static void validateRelationTargetTypes(RelationUpdate update) {
    Entity.EntityType targetEntityType = relationUpdateTargetType(update.relationType());
    validateRelationTargetTypes(update.relationType(), targetEntityType, update.targetsToAdd());
    validateRelationTargetTypes(update.relationType(), targetEntityType, update.targetsToRemove());
  }

  private static void validateRelationTargetTypes(
      Type relType, Entity.EntityType targetEntityType, RelationEdgeTarget[] relationTargets) {
    for (RelationEdgeTarget relationTarget : relationTargets) {
      Preconditions.checkArgument(
          relationTarget.entityType() == targetEntityType,
          "Relation target type %s does not match expected destination type %s for relation type %s",
          relationTarget.entityType(),
          targetEntityType,
          relType);
    }
  }

  private static RelationEdgeTarget[] toRelationEdgeTargets(
      Type relType, NameIdentifier[] nameIdentifiers) {
    if (nameIdentifiers == null) {
      return new RelationEdgeTarget[0];
    }

    Entity.EntityType targetEntityType = relationUpdateTargetType(relType);
    return Arrays.stream(nameIdentifiers)
        .map(nameIdentifier -> RelationEdgeTarget.of(nameIdentifier, targetEntityType, null))
        .toArray(RelationEdgeTarget[]::new);
  }

  private static Entity.EntityType relationUpdateTargetType(Type relType) {
    switch (relType) {
      case POLICY_METADATA_OBJECT_REL:
        return Entity.EntityType.POLICY;
      case TAG_METADATA_OBJECT_REL:
        return Entity.EntityType.TAG;
      default:
        throw new IllegalArgumentException(
            String.format("Doesn't support the relation type %s", relType));
    }
  }
}
