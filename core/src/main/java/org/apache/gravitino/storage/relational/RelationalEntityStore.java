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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationQuery;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsDomainOperations;
import org.apache.gravitino.SupportsOntologyIdLookup;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.SupportsTableIdLookup;
import org.apache.gravitino.cache.BaseEntityCache;
import org.apache.gravitino.cache.CacheFactory;
import org.apache.gravitino.cache.CachedEntityIdResolver;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.cache.Coherence;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.cache.EntityCacheKey;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.EntityChangeLogMetricsSource;
import org.apache.gravitino.semantic.SemanticModelMetric;
import org.apache.gravitino.semantic.SemanticModelRevision;
import org.apache.gravitino.semantic.SemanticModelRevisionSelector;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relation store to store entities. This means we can store entities in a relational store. I.e.,
 * MySQL, PostgreSQL, etc. If you want to use a different backend, you can implement the {@link
 * RelationalBackend} interface. The default JDBC backend is {@link JDBCBackend}.
 *
 * <p>A write that fails because a statement waited on a row lock longer than the backend's lock
 * wait timeout is reported as an {@link OptimisticLockException}, so callers see a conflict rather
 * than an internal error. The store write is rolled back; retrying the whole operation also
 * requires accounting for any preceding external side effects. Writers queue on the same rows
 * inside the database, for example a table drop behind a statistic update of that table, and the
 * backend aborts the waiting statement once the timeout is reached. See {@link
 * ExceptionUtils#isLockWaitTimeout}.
 */
public class RelationalEntityStore
    implements EntityStore,
        SupportsTableIdLookup,
        SupportsOntologyIdLookup,
        SupportsRelationOperations,
        SupportsDomainOperations,
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
  // Created with the store rather than in initialize(), so that a listener built before or without
  // initialization still has somewhere to record. initialize() only registers it for export.
  private final EntityChangeLogMetricsSource changeLogMetrics = new EntityChangeLogMetricsSource();

  // Advanced before every invalidation observed by this store, whether local or replayed from the
  // change log. A shared cache without a local change-log listener needs its own distributed
  // version check: this counter cannot detect changes made on another node.
  private final AtomicLong cacheInvalidationEpoch = new AtomicLong();

  // One refcount map for every reason a cache hit must be ignored:
  // {@code w:} exact key and hierarchy prefixes while a write is in flight, {@code p:} exact key
  // while a value is being published and may still be rolled back.
  private final ConcurrentHashMap<String, AtomicInteger> cacheBlocks = new ConcurrentHashMap<>();

  // Non-null only for a LOCAL_PER_NODE cache, which needs cross-node invalidation. SHARED and NONE
  // caches have no per-node copy to invalidate, so no listener is registered.
  @Nullable private EntityCacheChangeLogListener entityCacheChangeLogListener;

  @VisibleForTesting
  public EntityCache getCache() {
    return cache;
  }

  /**
   * Wraps a relational backend for offline migration without caches or background workers.
   *
   * <p>The caller initializes the SQL session factory and owns transaction boundaries. Closing the
   * returned store also closes the backend. All servers sharing the database must be stopped.
   *
   * @param backend The backend using the caller's SQL session factory.
   * @param config The configuration for the uncached store.
   * @return A store that participates in the current thread's SQL transaction.
   */
  public static RelationalEntityStore forOfflineMigration(
      RelationalBackend backend, Config config) {
    RelationalEntityStore store = new RelationalEntityStore();
    store.backend = backend;
    store.cache = new NoOpsCache(config);
    EntityIdService.initialize(new RelationalEntityStoreIdResolver());
    return store;
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    if (config.get(Configs.CACHE_ENABLED)) {
      this.cache = CacheFactory.getEntityCache(config);
      EntityIdService.initialize(
          new CachedEntityIdResolver(
              cache,
              new RelationalEntityStoreIdResolver(),
              (ident, type) -> !isCacheObservationBlocked(ident, type)));
    } else {
      this.cache = new NoOpsCache(config);
      EntityIdService.initialize(new RelationalEntityStoreIdResolver());
    }

    this.backend = createRelationalEntityBackend(config);
    this.garbageCollector = new RelationalGarbageCollector(backend, config);
    this.garbageCollector.start();

    // Polling and cleanup use separate single-threaded schedulers. Polling only dispatches changes
    // to local listeners, while cleanup independently removes records beyond the retention period.
    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    if (metricsSystem != null) {
      metricsSystem.register(changeLogMetrics);
    }
    this.entityChangeLogPoller =
        new EntityChangeLogPoller(
            config.get(Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS),
            config.get(Configs.ENTITY_CHANGE_LOG_POLL_BATCH_SIZE),
            config.get(Configs.ENTITY_CHANGE_LOG_REPLAY_WINDOW_SECS),
            config.get(Configs.ENTITY_CHANGE_LOG_REPLAY_MAX_ENTRIES),
            changeLogMetrics);
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

    this.entityCacheChangeLogListener = newCacheChangeLogListener();
    this.entityChangeLogPoller.registerListener(entityCacheChangeLogListener);
  }

  /**
   * Creates the change-log listener that keeps this store's cache coherent with changes made on
   * other nodes.
   *
   * @return a listener whose invalidations go through this store, see {@link #batchGet}
   */
  @VisibleForTesting
  EntityCacheChangeLogListener newCacheChangeLogListener() {
    return new EntityCacheChangeLogListener(
        new EntityCacheChangeLogListener.Target() {
          @Override
          public void invalidate(NameIdentifier ident, Entity.EntityType type) {
            invalidateOrClear(ident, type);
          }

          @Override
          public void clear() {
            clearCache();
          }
        },
        changeLogMetrics);
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
  public List<SchemaEntity> listDescendantSchemas(NameIdentifier identifier) throws IOException {
    return backend.listDescendantSchemas(identifier);
  }

  /** {@inheritDoc} */
  @Override
  public long getSemanticModelId(NameIdentifier ident) throws IOException {
    // Semantic Models are not in the entity cache allowlist, so the identity row is always read.
    return backend.getSemanticModelId(ident);
  }

  @Override
  public SemanticModelEntity loadSemanticModelRevision(
      NameIdentifier ident, SemanticModelRevisionSelector selector) throws IOException {
    return backend.loadSemanticModelRevision(ident, selector);
  }

  @Override
  public SemanticModelRevision[] listSemanticModelRevisions(NameIdentifier ident)
      throws IOException {
    return backend.listSemanticModelRevisions(ident);
  }

  @Override
  public SemanticModelEntity publishSemanticModelRevision(
      NameIdentifier ident, int revision, String principal) throws IOException {
    try {
      return backend.publishSemanticModelRevision(ident, revision, principal);
    } catch (RuntimeException e) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(e, Entity.EntityType.SEMANTIC_MODEL, ident);
    }
  }

  @Override
  public List<NameIdentifier> listSemanticModelIdentifiers(Namespace namespace) throws IOException {
    return backend.listSemanticModelIdentifiers(namespace);
  }

  @Override
  public SemanticModelEntity updateSemanticModel(
      NameIdentifier ident,
      @Nullable Integer expectedRevision,
      boolean rename,
      Function<SemanticModelEntity, SemanticModelEntity> updater)
      throws IOException {
    try {
      return backend.updateSemanticModel(ident, expectedRevision, rename, updater);
    } catch (RuntimeException e) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(e, Entity.EntityType.SEMANTIC_MODEL, ident);
    }
  }

  @Override
  public List<NameIdentifier> listSemanticModelMetrics(Namespace namespace) throws IOException {
    return backend.listSemanticModelMetrics(namespace);
  }

  @Override
  public List<SemanticModelMetric> listSemanticModelMetricInfos(Namespace namespace)
      throws IOException {
    return backend.listSemanticModelMetricInfos(namespace);
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
    // Same-key lock as get/publish so a mid-publication put cannot land between the blocked check
    // and contains.
    boolean cached =
        cache.withCacheLock(
            EntityCacheKey.of(ident, entityType),
            () ->
                !isCacheObservationBlocked(ident, entityType) && cache.contains(ident, entityType));
    return cached || backend.exists(ident, entityType);
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    NameIdentifier ident = e.nameIdentifier();
    Entity.EntityType type = e.type();
    try {
      if (overwritten) {
        // Drop the cache and block refill before the overwrite commits. Do not publish {@code e}:
        // the database may keep the identity and version of the row it already had.
        beginPendingWrite(ident, type);
        try {
          backend.insert(e, true);
        } finally {
          endPendingWrite(ident, type);
        }
      } else {
        // Clear any same-key stale row (for example a peer-dropped identity still cached here)
        // before insert, but keep the pre-insert epoch so a concurrent ancestor delete during
        // insert still fences publication.
        beginPendingWrite(ident, type);
        long epochBeforeInsert = cacheInvalidationEpoch.get();
        try {
          backend.insert(e, false);
        } finally {
          // Release without endPendingWrite's post-commit invalidate/epoch bumps so the insert
          // epoch fence below stays meaningful. Exact-key pending is enough to drop the stale row.
          releasePendingWrite(ident, type);
        }
        cacheIfEpochUnchanged(e, epochBeforeInsert);
        if (cacheInvalidationEpoch.get() != epochBeforeInsert) {
          // Publication was fenced; make sure the key stays empty for SHARED peer fills too.
          invalidateOrClear(ident, type);
        }
      }
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, type, ident);
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    // Populated inside the wrapped updater when it renames; ended in finally. The updater runs
    // inside backend.updateEntity before a backend-owned commit, so the new name is pending
    // before the renamed row is visible to other connections.
    NameIdentifier[] renamedTo = new NameIdentifier[1];
    beginPendingWrite(ident, entityType);
    try {
      try {
        return backend.update(
            ident,
            entityType,
            current -> {
              E updated = updater.apply(current);
              NameIdentifier newIdent = updated.nameIdentifier();
              if (!ident.equals(newIdent)) {
                // Record renamedTo only after begin succeeds so finally does not end a pending
                // write that never started.
                beginPendingWrite(newIdent, entityType);
                renamedTo[0] = newIdent;
              }
              return updated;
            });
      } catch (RuntimeException re) {
        throw ExceptionUtils.lockWaitTimeoutAsConflict(re, entityType, ident);
      }
    } finally {
      // Release rename target even if ending the old name fails (or the reverse).
      try {
        if (renamedTo[0] != null) {
          endPendingWrite(renamedTo[0], entityType);
        }
      } finally {
        endPendingWrite(ident, entityType);
      }
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException {
    // Ontology mutations use the versioned service directly. Governance reads resolve its
    // identity from the same store without retaining a pre-rename or pre-drop snapshot.
    if (entityType == Entity.EntityType.ONTOLOGY) {
      return backend.get(ident, entityType);
    }
    Optional<E> cached =
        cache.withCacheLock(
            EntityCacheKey.of(ident, entityType),
            () ->
                isCacheObservationBlocked(ident, entityType)
                    ? Optional.empty()
                    : cache.getIfPresent(ident, entityType));
    if (cached.isPresent()) {
      return cached.get();
    }

    // DB I/O stays outside the cache lock so this thread never holds both.
    long epochBeforeRead = cacheInvalidationEpoch.get();
    E entity = backend.get(ident, entityType);
    cacheIfEpochUnchanged(entity, epochBeforeRead);
    return entity;
  }

  @Override
  public long getTableId(NameIdentifier ident) throws IOException {
    Entity.EntityType entityType = Entity.EntityType.TABLE;
    Optional<Long> cachedId =
        cache.withCacheLock(
            EntityCacheKey.of(ident, entityType),
            () -> {
              if (isCacheObservationBlocked(ident, entityType)) {
                return Optional.empty();
              }
              Optional<TableEntity> cached = cache.getIfPresent(ident, entityType);
              return cached.map(TableEntity::id);
            });
    // Do not publish a partial TableEntity or introduce a separate name-to-ID cache. JDBC stays
    // outside the segment lock, matching get().
    if (cachedId.isPresent()) {
      return cachedId.get();
    }
    return backend.getTableId(ident);
  }

  /** Ontologies are never cached, so their IDs always come from the backend's ID-only query. */
  @Override
  public long getOntologyId(NameIdentifier ident) throws IOException {
    return backend.getOntologyId(ident);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> batchGet(
      List<NameIdentifier> idents, Entity.EntityType entityType, Class<E> clazz) {
    List<E> allEntities = new ArrayList<>();
    List<NameIdentifier> noCacheIdents =
        idents.stream()
            .filter(
                ident -> {
                  // Per-key lock matches get(): blocked check and getIfPresent are not separable.
                  Optional<E> entity =
                      cache.withCacheLock(
                          EntityCacheKey.of(ident, entityType),
                          () ->
                              isCacheObservationBlocked(ident, entityType)
                                  ? Optional.empty()
                                  : cache.getIfPresent(ident, entityType));
                  entity.ifPresent(allEntities::add);
                  return entity.isEmpty();
                })
            .toList();
    // Batch reads avoid holding multiple key locks across database I/O. Both point and batch
    // reads use an epoch guard: ancestor invalidation can race a point read too, because ancestor
    // and child keys need not share a segment lock.
    long epochBeforeRead = cacheInvalidationEpoch.get();
    List<E> fetchEntities = backend.batchGet(noCacheIdents, entityType);
    for (E entity : fetchEntities) {
      cacheIfEpochUnchanged(entity, epochBeforeRead);
      allEntities.add(entity);
    }
    return allEntities;
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException {
    beginPendingWrite(ident, entityType);
    try {
      return backend.delete(ident, entityType, cascade);
    } catch (NoSuchEntityException e) {
      return false;
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, entityType, ident);
    } finally {
      endPendingWrite(ident, entityType);
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> Optional<E> deleteAndGet(
      NameIdentifier ident,
      Entity.EntityType entityType,
      Class<E> clazz,
      Consumer<E> postDeleteAction)
      throws IOException {
    beginPendingWrite(ident, entityType);
    try {
      return backend.deleteAndGet(ident, entityType, clazz, postDeleteAction);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, entityType, ident);
    } finally {
      endPendingWrite(ident, entityType);
    }
  }

  @Override
  public Optional<SchemaEntity> deleteSchemaAndGet(
      NameIdentifier ident, boolean cascadeFunctions, Consumer<SchemaEntity> postDeleteAction)
      throws IOException {
    beginPendingWrite(ident, Entity.EntityType.SCHEMA);
    try {
      return backend.deleteSchemaAndGet(ident, cascadeFunctions, postDeleteAction);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, Entity.EntityType.SCHEMA, ident);
    } finally {
      endPendingWrite(ident, Entity.EntityType.SCHEMA);
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
    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    if (metricsSystem != null) {
      metricsSystem.unregister(changeLogMetrics);
    }

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
  public SupportsDomainOperations domainOperations() {
    return this;
  }

  @Override
  public void insertDomainWithBackingTag(
      DomainEntity domain, TagValue backingTag, Consumer<LockedDomainState> check)
      throws IOException {
    try {
      backend.insertDomainWithBackingTag(domain, backingTag, check);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re, Entity.EntityType.DOMAIN, domain.nameIdentifier());
    }
  }

  @Override
  public void insertDomainWithNewBackingTag(
      DomainEntity domain, TagEntity backingTag, Consumer<LockedDomainState> check)
      throws IOException {
    try {
      backend.insertDomainWithNewBackingTag(domain, backingTag, check);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re, Entity.EntityType.DOMAIN, domain.nameIdentifier());
    }
    // Tags are cached; the new tag must not be shadowed by an entry cached for its name.
    invalidateCache(backingTag.nameIdentifier(), Entity.EntityType.TAG);
  }

  @Override
  public boolean deleteDomain(NameIdentifier ident, Consumer<DomainEntity> check)
      throws IOException {
    try {
      return backend.deleteDomain(ident, check);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, Entity.EntityType.DOMAIN, ident);
    } finally {
      invalidateCache(ident, Entity.EntityType.DOMAIN);
    }
  }

  @Override
  public DomainEntity updateDomainBackingTagAndGet(
      NameIdentifier ident,
      @Nullable TagValue backingTag,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check)
      throws IOException {
    try {
      return backend.updateDomainBackingTagAndGet(ident, backingTag, auditInfo, check);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, Entity.EntityType.DOMAIN, ident);
    }
  }

  @Override
  public DomainEntity setDomainParentAndGet(
      NameIdentifier ident,
      NameIdentifier parentIdent,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check)
      throws IOException {
    try {
      return backend.setDomainParentAndGet(ident, parentIdent, auditInfo, check);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, Entity.EntityType.DOMAIN, ident);
    }
  }

  @Override
  public DomainEntity removeDomainParentAndGet(
      NameIdentifier ident, Consumer<LockedDomainState> check) throws IOException {
    try {
      return backend.removeDomainParentAndGet(ident, check);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, Entity.EntityType.DOMAIN, ident);
    }
  }

  @Override
  public DomainEntity updateDomainStatusAndGet(
      NameIdentifier ident,
      Domain.Status status,
      AuditInfo auditInfo,
      Consumer<LockedDomainState> check)
      throws IOException {
    try {
      return backend.updateDomainStatusAndGet(ident, status, auditInfo, check);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, Entity.EntityType.DOMAIN, ident);
    }
  }

  @Override
  public Map<Long, List<GenericEntity>> listDomainAssignments(
      String metalake,
      List<DomainEntity> domains,
      Set<MetadataObject.Type> types,
      Map<Long, List<GenericEntity>> selectedAssignments)
      throws IOException {
    return backend.listDomainAssignments(metalake, domains, types, selectedAssignments);
  }

  @Override
  public Map<Long, List<TagEntity>> listDirectTagsByObjectIds(
      String metalake, MetadataObject.Type type, List<Long> objectIds) throws IOException {
    return backend.listDirectTagsByObjectIds(metalake, type, objectIds);
  }

  @Override
  public List<DomainEntity> listDomainsByBackingTagIds(String metalake, List<Long> backingTagIds)
      throws IOException {
    return backend.listDomainsByBackingTagIds(metalake, backingTagIds);
  }

  @Override
  public List<DomainEntity> listSubDomains(NameIdentifier parentIdent) throws IOException {
    return backend.listSubDomains(parentIdent);
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
    try {
      backend.insertRelation(relType, srcIdentifier, srcType, dstIdentifier, dstType, override);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re,
          String.format(
              "The %s relation of %s %s",
              relType.name().toLowerCase(Locale.ROOT),
              srcType.name().toLowerCase(Locale.ROOT),
              srcIdentifier));
    }
    // Relation query results themselves are not cached, but both endpoints may be cached entities
    // (OWNER_REL, TAG_METADATA_OBJECT_REL and METADATA_OBJECT_ROLE_REL are keyed by
    // catalog/schema/table/... on the source side), so drop their entries conservatively: a
    // relation write can change data materialized into the endpoint entity. Note this is not free —
    // EntityCache#invalidate cascades over the identifier hierarchy, so invalidating a catalog also
    // drops every cached schema and table beneath it.
    invalidateCache(srcIdentifier, srcType);
    invalidateCache(dstIdentifier, dstType);
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
    try {
      backend.batchInsertRelations(
          relType, srcIdentifiers, srcType, dstIdentifier, dstType, override);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re,
          String.format(
              "The %s relations of %d %s entities starting with %s",
              relType.name().toLowerCase(Locale.ROOT),
              srcIdentifiers.size(),
              srcType.name().toLowerCase(Locale.ROOT),
              srcIdentifiers.get(0)));
    }
    // Invalidate both endpoints for the same reason as insertRelation, including the hierarchy
    // cascade noted there.
    for (NameIdentifier ident : srcIdentifiers) {
      invalidateCache(ident, srcType);
    }
    invalidateCache(dstIdentifier, dstType);
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

    List<E> result;
    try {
      result =
          backend.updateEntityRelations(
              relType, srcEntityIdent, srcEntityType, destEntitiesToAdd, destEntitiesToRemove);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(re, srcEntityType, srcEntityIdent);
    }
    Entity.EntityType targetEntityType = relationUpdateTargetType(relType);
    invalidateCache(srcEntityIdent, srcEntityType);
    invalidateRelationTargetCache(targetEntityType, update.targetsToAdd());
    invalidateRelationTargetCache(targetEntityType, update.targetsToRemove());

    return result;
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> listEntitiesByRelation(RelationQuery query)
      throws IOException {
    if (query.anchorId().isPresent() || query.relationValue().isPresent()) {
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
    List<E> result;
    try {
      result = backend.updateEntityRelations(update);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re, update.sourceEntityType(), update.sourceIdentifier());
    }

    // Invalidate after the backend write, not before: invalidating first opens a window where a
    // concurrent read could repopulate the cache with stale pre-commit data.
    Entity.EntityType targetEntityType = relationUpdateTargetType(update.relationType());
    invalidateCache(update.sourceIdentifier(), update.sourceEntityType());
    invalidateRelationTargetCache(targetEntityType, targetsToAdd);
    invalidateRelationTargetCache(targetEntityType, targetsToRemove);

    return result;
  }

  @Override
  public int batchDelete(
      List<Pair<NameIdentifier, Entity.EntityType>> entitiesToDelete, boolean cascade)
      throws IOException {
    try {
      return backend.batchDelete(entitiesToDelete, cascade);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re,
          describeBatch(
              entitiesToDelete.size(),
              entitiesToDelete.isEmpty() ? null : entitiesToDelete.get(0).getRight(),
              entitiesToDelete.isEmpty() ? null : entitiesToDelete.get(0).getLeft()));
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> void batchPut(List<E> entities, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    try {
      backend.batchPut(entities, overwritten);
    } catch (RuntimeException re) {
      throw ExceptionUtils.lockWaitTimeoutAsConflict(
          re,
          describeBatch(
              entities.size(),
              entities.isEmpty() ? null : entities.get(0).type(),
              entities.isEmpty() ? null : entities.get(0).nameIdentifier()));
    }
  }

  /** Describes a batch write by its size and its first entity, for a conflict message. */
  private static String describeBatch(
      int size, @Nullable Entity.EntityType firstType, @Nullable NameIdentifier first) {
    if (firstType == null) {
      return String.format("The batch of %d entities", size);
    }
    return String.format(
        "The batch of %d entities starting with %s %s",
        size, firstType.name().toLowerCase(Locale.ROOT), first);
  }

  private void invalidateRelationTargetCache(
      Entity.EntityType targetEntityType, RelationEdgeTarget[] relationTargets) {
    for (RelationEdgeTarget relationTarget : relationTargets) {
      invalidateCache(relationTarget.nameIdentifier(), targetEntityType);
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
      case TAG_METADATA_OBJECT_REL:
        return Entity.EntityType.TAG;
      case POLICY_TAG_REL:
        return Entity.EntityType.POLICY;
      default:
        throw new IllegalArgumentException(
            String.format("Doesn't support the relation type %s", relType));
    }
  }

  private <E extends Entity & HasIdentifier> void cacheIfEpochUnchanged(E entity, long epoch) {
    if (cache instanceof BaseEntityCache && !BaseEntityCache.isCacheable(entity.type())) {
      // Uncached entities still invoke the cache's related-invalidation hook. There is no stale
      // value to publish, and taking this entity's key lock can nest locks inside that hook.
      cache.put(entity);
      return;
    }
    NameIdentifier ident = entity.nameIdentifier();
    Entity.EntityType type = entity.type();
    if (isPendingWrite(ident, type) || cacheInvalidationEpoch.get() != epoch) {
      return;
    }
    // Hide the entry until publish finishes. Rollback invalidate runs outside the segment lock:
    // the SPI does not require that lock to be reentrant.
    String publishKey = "p\0" + type.name() + "\0" + ident;
    bumpBlock(publishKey);
    try {
      boolean[] published = new boolean[1];
      cache.withCacheLock(
          EntityCacheKey.of(ident, type),
          () -> {
            if (isPendingWrite(ident, type) || cacheInvalidationEpoch.get() != epoch) {
              return;
            }
            cache.put(entity);
            published[0] = true;
          });
      if (published[0] && (isPendingWrite(ident, type) || cacheInvalidationEpoch.get() != epoch)) {
        invalidateOrClear(ident, type);
      }
    } finally {
      dropBlock(publishKey);
    }
  }

  private void invalidateCache(NameIdentifier ident, Entity.EntityType type) {
    // Advance before removing, so a batchGet() that samples the epoch after this point reads the
    // backend after the change that triggered the invalidation is visible.
    cacheInvalidationEpoch.incrementAndGet();
    cache.invalidate(ident, type);
  }

  /** Drops {@code ident}. If that fails, clears the whole cache so no stale entry remains. */
  private void invalidateOrClear(NameIdentifier ident, Entity.EntityType type) {
    try {
      invalidateCache(ident, type);
    } catch (RuntimeException | Error e) {
      try {
        cache.clear();
      } catch (RuntimeException | Error clearFailure) {
        e.addSuppressed(clearFailure);
      }
      throw e;
    }
  }

  /**
   * Marks {@code ident} and its hierarchy prefixes pending, then invalidates. Register pending
   * before dropping the cache so a concurrent descendant read sees the block before it can
   * republish.
   *
   * <p>Does not hold the cache key lock across {@link EntityCache#invalidate}: the SPI does not
   * require {@code invalidate} to reenter that lock, and Caffeine acquires it internally.
   */
  private void beginPendingWrite(NameIdentifier ident, Entity.EntityType entityType) {
    retainPendingWrite(ident, entityType);
    try {
      invalidateOrClear(ident, entityType);
    } catch (RuntimeException | Error e) {
      // Roll the block back so a failed begin cannot leak it (rename records the new name only
      // after begin returns). invalidateOrClear already emptied the cache if the drop failed.
      releasePendingWrite(ident, entityType);
      throw e;
    }
  }

  /**
   * Invalidates while the write block is still held, then releases it. {@link #invalidateOrClear}
   * clears the whole cache if the targeted drop fails, so readers neither observe a stale entry nor
   * keep a leaked block. The epoch advances before the block is released.
   */
  private void endPendingWrite(NameIdentifier ident, Entity.EntityType entityType) {
    try {
      invalidateOrClear(ident, entityType);
    } finally {
      releasePendingWrite(ident, entityType);
    }
  }

  private void retainPendingWrite(NameIdentifier ident, Entity.EntityType entityType) {
    bumpBlock("w\0" + entityType.name() + "\0" + ident);
    for (String prefix : CaffeineEntityCache.descendantPrefixes(ident, entityType)) {
      bumpBlock("h\0" + prefix);
    }
  }

  private void releasePendingWrite(NameIdentifier ident, Entity.EntityType entityType) {
    dropBlock("w\0" + entityType.name() + "\0" + ident);
    for (String prefix : CaffeineEntityCache.descendantPrefixes(ident, entityType)) {
      dropBlock("h\0" + prefix);
    }
  }

  private void bumpBlock(String key) {
    cacheBlocks.compute(
        key,
        (ignored, count) -> {
          if (count == null) {
            return new AtomicInteger(1);
          }
          count.incrementAndGet();
          return count;
        });
  }

  private void dropBlock(String key) {
    cacheBlocks.compute(
        key,
        (ignored, count) -> {
          if (count == null) {
            return null;
          }
          return count.decrementAndGet() == 0 ? null : count;
        });
  }

  private boolean isPendingWrite(NameIdentifier ident, Entity.EntityType entityType) {
    AtomicInteger exact = cacheBlocks.get("w\0" + entityType.name() + "\0" + ident);
    if (exact != null && exact.get() > 0) {
      return true;
    }
    for (String prefix : CaffeineEntityCache.ancestorHierarchyPrefixes(ident)) {
      AtomicInteger count = cacheBlocks.get("h\0" + prefix);
      if (count != null && count.get() > 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns whether cache hits must be ignored: an in-flight write (this key or an ancestor) or a
   * publication that may still be rolled back.
   */
  private boolean isCacheObservationBlocked(NameIdentifier ident, Entity.EntityType entityType) {
    AtomicInteger publishing = cacheBlocks.get("p\0" + entityType.name() + "\0" + ident);
    return isPendingWrite(ident, entityType) || (publishing != null && publishing.get() > 0);
  }

  @VisibleForTesting
  void clearCache() {
    // Advance the epoch and drop cache entries only. In-flight writers own their blocks and must
    // release them; clearing the map would let readers refill while a write is still committing.
    cacheInvalidationEpoch.incrementAndGet();
    cache.clear();
  }
}
