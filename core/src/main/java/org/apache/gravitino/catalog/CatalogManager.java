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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.Catalog.PROPERTY_IN_USE;
import static org.apache.gravitino.Catalog.Type.FILESET;
import static org.apache.gravitino.StringIdentifier.DUMMY_ID;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForAlter;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.apache.gravitino.connector.BaseCatalogPropertiesMetadata.PROPERTY_METALAKE_IN_USE;
import static org.apache.gravitino.metalake.MetalakeManager.checkMetalake;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.CatalogChange.RemoveProperty;
import org.apache.gravitino.CatalogChange.SetProperty;
import org.apache.gravitino.CatalogProvider;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogDropAware;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptyCatalogException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.messaging.TopicCatalog;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.SupportsEntityChangeLog;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.ThrowableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages the catalog instances and operations. */
public class CatalogManager implements CatalogDispatcher, Closeable {

  private static final String CATALOG_DOES_NOT_EXIST_MSG = "Catalog %s does not exist";

  private static final Logger LOG = LoggerFactory.getLogger(CatalogManager.class);

  private static final Set<String> CONTRIB_CATALOGS_TYPES =
      ImmutableSet.of("jdbc-oceanbase", "jdbc-clickhouse", "jdbc-hologres");

  /**
   * Wrapper class for a catalog instance and its class loader.
   *
   * <p>A wrapper is shared by all threads that read it from the catalog cache, while cache eviction
   * (expiry, explicit invalidation, or remote change-log invalidation) happens outside the tree
   * lock. To keep an eviction from tearing down a catalog that an in-flight operation is still
   * using, the wrapper counts active operations: {@link #tryAcquire()} takes a lease, {@link
   * #release()} returns it, and {@link #retire()} (called from the cache removal listener) only
   * marks the wrapper unusable for new leases. The catalog and the ClassLoader are cleaned up
   * exactly once, when the wrapper is retired and the last lease has been released.
   */
  public static class CatalogWrapper {

    // Volatile because cleanup() nulls it outside leaseLock (holding the lock across a catalog
    // close would stall tryAcquire), while unleased readers such as callers of
    // loadCatalogAndWrap() may read it from another thread. Leased readers cannot race with
    // cleanup at all: cleanup is only claimed once the wrapper is retired and no lease is held.
    private volatile BaseCatalog catalog;

    private final IsolatedClassLoader classLoader;

    /** Guards {@link #activeOps}, {@link #retired} and {@link #cleanupStarted}. */
    private final Object leaseLock = new Object();

    /** Number of leases currently held by in-flight operations. */
    private int activeOps = 0;

    /** Set when the wrapper leaves the cache; no new lease can be acquired afterwards. */
    private boolean retired = false;

    /** Set by the thread that claims the (exactly-once) resource cleanup. */
    private boolean cleanupStarted = false;

    public CatalogWrapper(BaseCatalog catalog, IsolatedClassLoader classLoader) {
      this.catalog = catalog;
      this.classLoader = classLoader;
    }

    public BaseCatalog catalog() {
      return catalog;
    }

    /**
     * Tries to take a lease on this wrapper, keeping its catalog and ClassLoader alive until the
     * lease is released.
     *
     * @return true if the lease was taken, false if the wrapper has already been retired and the
     *     caller must load a fresh wrapper.
     */
    boolean tryAcquire() {
      synchronized (leaseLock) {
        if (retired) {
          return false;
        }
        activeOps++;
        return true;
      }
    }

    /**
     * Releases a lease taken by {@link #tryAcquire()}. Cleans up the catalog and the ClassLoader if
     * this was the last lease on an already retired wrapper.
     *
     * <p>Note that in that case the cleanup runs on the releasing thread, which is usually a
     * request thread, so a slow catalog close is charged to that request. This only happens when
     * the wrapper was evicted while the operation was in flight; the common case is that eviction
     * finds no lease and cleans up on the cache's own thread.
     */
    void release() {
      boolean shouldCleanup;
      synchronized (leaseLock) {
        Preconditions.checkState(activeOps > 0, "Releasing a lease that was never acquired");
        activeOps--;
        shouldCleanup = claimCleanupIfIdle();
      }

      if (shouldCleanup) {
        cleanup();
      }
    }

    /**
     * Retires this wrapper: no new lease can be taken. The catalog and the ClassLoader are cleaned
     * up immediately if no operation is in flight, otherwise by the last {@link #release()}.
     */
    void retire() {
      boolean shouldCleanup;
      synchronized (leaseLock) {
        retired = true;
        shouldCleanup = claimCleanupIfIdle();
      }

      if (shouldCleanup) {
        cleanup();
      }
    }

    /**
     * Returns whether this wrapper has been retired and can no longer serve new operations.
     *
     * @return true if the wrapper has been retired.
     */
    boolean isRetired() {
      synchronized (leaseLock) {
        return retired;
      }
    }

    @VisibleForTesting
    int activeOperations() {
      synchronized (leaseLock) {
        return activeOps;
      }
    }

    public <R> R doWithSchemaOps(ThrowableFunction<SupportsSchemas, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asSchemas() == null) {
              throw new UnsupportedOperationException("Catalog does not support schema operations");
            }
            return fn.apply(asSchemas());
          });
    }

    public <R> R doWithTableOps(ThrowableFunction<TableCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asTables() == null) {
              throw new UnsupportedOperationException("Catalog does not support table operations");
            }
            return fn.apply(asTables());
          });
    }

    public <R> R doWithViewOps(ThrowableFunction<ViewCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asViews() == null) {
              throw new UnsupportedOperationException("Catalog does not support view operations");
            }
            return fn.apply(asViews());
          });
    }

    public <R> R doWithFilesetOps(ThrowableFunction<FilesetCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asFilesets() == null) {
              throw new UnsupportedOperationException(
                  "Catalog does not support fileset operations");
            }
            return fn.apply(asFilesets());
          });
    }

    public <R> R doWithFilesetFileOps(ThrowableFunction<FilesetFileOps, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asFilesetFileOps() == null) {
              throw new UnsupportedOperationException(
                  "Catalog does not support fileset file operations");
            }
            return fn.apply(asFilesetFileOps());
          });
    }

    /**
     * Runs an operation against the catalog instance itself, with the catalog's ClassLoader in
     * place for the duration of the call.
     *
     * @param fn the operation to run
     * @param <R> the result type
     * @return the operation result
     * @throws Exception if the operation fails
     */
    public <R> R doWithCatalog(ThrowableFunction<BaseCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(cl -> fn.apply(catalog));
    }

    public <R> R doWithCredentialOps(ThrowableFunction<BaseCatalog, R> fn) throws Exception {
      return doWithCatalog(fn);
    }

    /**
     * Converts a connector-backed result into a snapshot that no longer depends on this catalog's
     * ClassLoader, reading the connector object with that ClassLoader installed.
     *
     * <p>Scoped to the conversion alone: the caller's surrounding work is Gravitino's own code and
     * must keep the application ClassLoader as its thread context ClassLoader.
     *
     * @param result the value returned by a connector operation
     * @param <R> the result type
     * @return a detached snapshot, or the value itself if it carries no connector state
     * @throws Exception if reading the connector object fails
     */
    <R> R detachConnectorResult(R result) throws Exception {
      return classLoader.withClassLoader(ignored -> ConnectorObjectSnapshot.detach(result));
    }

    public <R> R doWithTopicOps(ThrowableFunction<TopicCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asTopics() == null) {
              throw new UnsupportedOperationException("Catalog does not support topic operations");
            }
            return fn.apply(asTopics());
          });
    }

    public <R> R doWithModelOps(ThrowableFunction<ModelCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asModels() == null) {
              throw new UnsupportedOperationException("Catalog does not support model operations");
            }
            return fn.apply(asModels());
          });
    }

    public <R> R doWithCatalogOps(ThrowableFunction<CatalogOperations, R> fn) throws Exception {
      return classLoader.withClassLoader(cl -> fn.apply(catalog.ops()));
    }

    public <R> R doWithPartitionOps(
        NameIdentifier tableIdent, ThrowableFunction<SupportsPartitions, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            Preconditions.checkArgument(
                asTables() != null, "Catalog does not support table operations");
            Table table = asTables().loadTable(tableIdent);
            SupportsPartitions partitionOps = table.supportPartitions();
            Preconditions.checkArgument(
                partitionOps != null, "Table does not support partition operations");
            return fn.apply(partitionOps);
          });
    }

    public <R> R doWithPropertiesMeta(ThrowableFunction<HasPropertyMetadata, R> fn)
        throws Exception {
      return doWithCatalog(fn::apply);
    }

    public Capability capabilities() throws Exception {
      return classLoader.withClassLoader(cl -> catalog.capability());
    }

    /**
     * Retires the wrapper and, once no operation is in flight anymore, releases its resources. Kept
     * as an alias of {@link #retire()} so callers that own a wrapper exclusively (for example
     * {@link CatalogManager#testConnection}) can keep using the {@link java.io.Closeable}-style
     * API.
     */
    public void close() {
      retire();
    }

    /**
     * Claims the exactly-once resource cleanup when the wrapper is retired and idle. Must be called
     * while holding {@link #leaseLock}; the caller runs {@link #cleanup()} outside the lock so a
     * slow catalog close does not block {@link #tryAcquire()}.
     */
    private boolean claimCleanupIfIdle() {
      if (!retired || activeOps > 0 || cleanupStarted) {
        return false;
      }
      cleanupStarted = true;
      return true;
    }

    private void cleanup() {
      // Drop the reference before closing so a failing close() cannot leave a half-closed catalog
      // reachable: cleanup() runs exactly once, so a null assignment after close() would be skipped
      // on that path. Unleased readers then see null and fail fast instead of using a closed
      // catalog; leased readers cannot race with cleanup at all.
      BaseCatalog toClose = catalog;
      catalog = null;

      try {
        classLoader.withClassLoader(
            cl -> {
              if (toClose != null) {
                toClose.close();
              }
              return null;
            });
      } catch (Exception e) {
        LOG.warn("Failed to close catalog", e);
      } finally {
        classLoader.close();
      }
    }

    private SupportsSchemas asSchemas() {
      return catalog.ops() instanceof SupportsSchemas ? (SupportsSchemas) catalog.ops() : null;
    }

    private TableCatalog asTables() {
      return catalog.ops() instanceof TableCatalog ? (TableCatalog) catalog.ops() : null;
    }

    private ViewCatalog asViews() {
      return catalog.ops() instanceof ViewCatalog ? (ViewCatalog) catalog.ops() : null;
    }

    private FilesetCatalog asFilesets() {
      return catalog.ops() instanceof FilesetCatalog ? (FilesetCatalog) catalog.ops() : null;
    }

    private FilesetFileOps asFilesetFileOps() {
      return catalog.ops() instanceof FilesetFileOps ? (FilesetFileOps) catalog.ops() : null;
    }

    private TopicCatalog asTopics() {
      return catalog.ops() instanceof TopicCatalog ? (TopicCatalog) catalog.ops() : null;
    }

    private ModelCatalog asModels() {
      return catalog.ops() instanceof ModelCatalog ? (ModelCatalog) catalog.ops() : null;
    }
  }

  private final Config config;

  @Getter private final Cache<NameIdentifier, CatalogWrapper> catalogCache;

  private final EntityStore store;

  @Nullable private final CatalogChangeLogListener catalogChangeLogListener;

  private final IdGenerator idGenerator;

  // Copy-on-write: listeners may be registered while the cache's removal listener (running on a
  // cache executor thread) is iterating this list.
  private final List<Consumer<NameIdentifier>> removalListeners = new CopyOnWriteArrayList<>();
  private final ConcurrentHashMap<NameIdentifier, AtomicInteger> localMutationCounts =
      new ConcurrentHashMap<>();

  // Cache loads and publications take the read lock; close() takes the write lock. Operations do
  // not hold this lock after acquiring a CatalogLease because the lease itself keeps the wrapper
  // alive while close() retires it.
  private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();

  /** Guarded by {@link #lifecycleLock}. */
  private boolean closed;

  // Set to true when a CatalogChangeLogListener is active. markLocalMutation() is a no-op
  // unless this flag is set, preventing unbounded growth of localMutationCounts in deployments
  // that do not use a relational entity store (where the poller never runs).
  private volatile boolean trackLocalMutations = false;

  /**
   * Constructs a CatalogManager instance.
   *
   * @param config The configuration for the manager.
   * @param store The entity store to use.
   * @param idGenerator The id generator to use.
   */
  public CatalogManager(Config config, EntityStore store, IdGenerator idGenerator) {
    this.config = config;
    this.store = store;
    this.idGenerator = idGenerator;

    long cacheEvictionIntervalInMs = config.get(Configs.CATALOG_CACHE_EVICTION_INTERVAL_MS);
    this.catalogCache =
        Caffeine.newBuilder()
            .expireAfterAccess(cacheEvictionIntervalInMs, TimeUnit.MILLISECONDS)
            .removalListener(
                (k, v, c) -> {
                  LOG.debug("Removed catalog cache entry, identifier={}, cause={}", k, c);
                  try {
                    for (Consumer<NameIdentifier> listener : removalListeners) {
                      if (k != null) {
                        listener.accept((NameIdentifier) k);
                      }
                    }
                  } finally {
                    // Retire rather than close: an operation that already leased this wrapper
                    // keeps it alive, and the actual catalog/ClassLoader cleanup runs when the
                    // last lease is released. Keep this in finally so a faulty external listener
                    // cannot skip resource cleanup.
                    ((CatalogWrapper) v).retire();
                  }
                })
            .scheduler(
                Scheduler.forScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("catalog-cleaner-%d")
                            .build())))
            .build();

    // If the entity store maintains a change log, register a listener that invalidates this
    // manager's local catalog cache from cross-node changes, and enable local-mutation tracking so
    // changes made by this node are not redundantly re-invalidated. Registration is the last step
    // of
    // the constructor so the listener never sees a half-initialized manager (catalogCache and
    // localMutationCounts are already set). CatalogChangeLogListener stays an implementation detail
    // of this class rather than being wired externally.
    if (store instanceof SupportsEntityChangeLog) {
      this.trackLocalMutations = true;
      this.catalogChangeLogListener = new CatalogChangeLogListener(this);
      ((SupportsEntityChangeLog) store).registerEntityChangeLogListener(catalogChangeLogListener);
    } else {
      this.catalogChangeLogListener = null;
    }
  }

  /**
   * Closes the CatalogManager and invalidates all cached catalog instances. Idle resources are
   * released immediately; resources protected by active leases are released when their last lease
   * is closed.
   */
  @Override
  public void close() {
    lifecycleLock.writeLock().lock();
    try {
      if (closed) {
        return;
      }

      // Holding the write lock prevents new cache loads and publications until shutdown has
      // finished. Existing operations only hold CatalogLeases, so retiring their wrappers here is
      // non-blocking and their cleanup remains deferred until the last lease is released.
      unregisterChangeLogListener();
      retireCachedWrappers();
      closed = true;
    } finally {
      lifecycleLock.writeLock().unlock();
    }
  }

  private void unregisterChangeLogListener() {
    if (catalogChangeLogListener != null) {
      try {
        ((SupportsEntityChangeLog) store)
            .unregisterEntityChangeLogListener(catalogChangeLogListener);
      } catch (RuntimeException e) {
        LOG.warn("Failed to unregister the catalog change-log listener", e);
      } finally {
        trackLocalMutations = false;
        localMutationCounts.clear();
      }
    }
  }

  /**
   * Retires every wrapper still cached, so their resources are released as soon as the operations
   * holding them finish. The cache's removal listener is asynchronous, so the wrappers are retired
   * here synchronously; active leases defer the cleanup and keep their ClassLoader alive until the
   * last operation releases it.
   *
   * <p>The caller holds {@link #lifecycleLock}'s write lock, so no wrapper can be loaded or
   * published while the snapshot is taken.
   */
  private void retireCachedWrappers() {
    List<CatalogWrapper> wrappers = new ArrayList<>(catalogCache.asMap().values());
    catalogCache.invalidateAll();
    for (CatalogWrapper wrapper : wrappers) {
      try {
        wrapper.retire();
      } catch (RuntimeException e) {
        LOG.warn("Failed to retire a cached catalog wrapper while closing the CatalogManager", e);
      }
    }
  }

  /**
   * Adds a listener that will be notified when a catalog is removed from the cache.
   *
   * <p>Note: Cache eviction is invoked asynchronously but uses a single thread to process removal
   * events. To avoid blocking the eviction thread and delaying subsequent cache operations,
   * listeners should avoid performing heavy operations (such as I/O, network calls, or complex
   * computations) directly. Instead, consider offloading heavy work to a separate thread or
   * executor.
   *
   * @param listener The consumer to be called with the NameIdentifier of the removed catalog.
   */
  public void addCatalogCacheRemoveListener(Consumer<NameIdentifier> listener) {
    removalListeners.add(listener);
  }

  /**
   * Records that this process has just mutated the given catalog locally. The entity change log
   * poller will see the corresponding change log row and should skip cache invalidation for it
   * because this process already updated the cache.
   *
   * @param ident the catalog identifier (pre-mutation name for renames)
   */
  void markLocalMutation(NameIdentifier ident) {
    if (!trackLocalMutations) {
      return;
    }
    localMutationCounts.computeIfAbsent(ident, k -> new AtomicInteger()).incrementAndGet();
  }

  /**
   * Attempts to consume one local mutation marker for the given identifier.
   *
   * <p>Thread-safety note: {@link ConcurrentHashMap#computeIfPresent} executes the remapping
   * function atomically under a per-bucket lock, so {@code consumed[0]} is set and the counter is
   * decremented as a single atomic step. {@code consumed[0]} is a single-element array (the
   * standard Java pattern for a mutable capture in a lambda) that is only read by this thread after
   * {@code computeIfPresent} returns, so no additional synchronization is needed.
   *
   * @return true if a local mutation was pending and has been consumed (caller should skip
   *     invalidation), false if the change originated from a remote node
   */
  boolean consumeLocalMutation(NameIdentifier ident) {
    boolean[] consumed = {false};
    localMutationCounts.computeIfPresent(
        ident,
        (k, count) -> {
          consumed[0] = true;
          return count.decrementAndGet() <= 0 ? null : count;
        });
    return consumed[0];
  }

  @VisibleForTesting
  void setTrackLocalMutations(boolean trackLocalMutations) {
    this.trackLocalMutations = trackLocalMutations;
  }

  /**
   * Lists the catalogs within the specified namespace.
   *
   * @param namespace The namespace for which to list catalogs.
   * @return An array of NameIdentifier objects representing the catalogs.
   * @throws NoSuchMetalakeException If the specified metalake does not exist.
   */
  @Override
  public NameIdentifier[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    NameIdentifier metalakeIdent = NameIdentifier.of(namespace.levels());

    return TreeLockUtils.doWithTreeLock(
        metalakeIdent,
        LockType.READ,
        () -> {
          checkMetalake(metalakeIdent, store);
          try {
            return store.list(namespace, CatalogEntity.class, EntityType.CATALOG).stream()
                .map(entity -> NameIdentifier.of(namespace, entity.name()))
                .toArray(NameIdentifier[]::new);

          } catch (IOException ioe) {
            LOG.error("Failed to list catalogs in metalake {}", metalakeIdent, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public Catalog[] listCatalogsInfo(Namespace namespace) throws NoSuchMetalakeException {
    NameIdentifier metalakeIdent = NameIdentifier.of(namespace.levels());
    try {
      List<CatalogEntity> catalogEntities =
          TreeLockUtils.doWithTreeLock(
              metalakeIdent,
              LockType.READ,
              () -> {
                checkMetalake(metalakeIdent, store);
                return store.list(namespace, CatalogEntity.class, EntityType.CATALOG);
              });
      return catalogEntities.stream()
          // The old fileset catalog's provider is "hadoop", whereas the new fileset catalog's
          // provider is "fileset", still using "hadoop" will lead to catalog loading issue. So
          // after reading the catalog entity, we convert it to the new fileset catalog entity.
          .map(this::convertFilesetCatalogEntity)
          .map(e -> e.toCatalogInfoWithResolvedProps(getResolvedProperties(e)))
          .toArray(Catalog[]::new);
    } catch (IOException ioe) {
      LOG.error("Failed to list catalogs in metalake {}", metalakeIdent, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Loads the catalog with the specified identifier.
   *
   * @param ident The identifier of the catalog to load.
   * @return A metadata snapshot of the loaded catalog. Connector resources are not exposed.
   * @throws NoSuchCatalogException If the specified catalog does not exist.
   */
  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.READ,
        () -> {
          try (CatalogLease lease = acquireCatalogLease(ident)) {
            BaseCatalog baseCatalog = lease.catalog();
            baseCatalog.checkMetalakeInUse();
            return toCatalogInfo(lease.wrapper());
          }
        });
  }

  /**
   * Runs an operation against a catalog while keeping its catalog instance and ClassLoader alive.
   *
   * <p>Callers that need connector-only state, such as the authorization plugin or raw catalog
   * properties, must use this method instead of casting the metadata snapshot returned by {@link
   * #loadCatalog(NameIdentifier)}.
   *
   * <p><b>Note:</b> The callback must not retain the live catalog. Connector-backed metadata should
   * be converted to a detached value before the callback returns.
   *
   * @param ident The identifier of the catalog to use.
   * @param operation The operation to run against the live catalog instance.
   * @return The value returned by the operation.
   * @param <R> The result type of the operation.
   * @throws NoSuchCatalogException If the specified catalog does not exist.
   */
  public <R> R doWithCatalog(NameIdentifier ident, ThrowableFunction<BaseCatalog, R> operation)
      throws NoSuchCatalogException {
    try {
      // wrapper.doWithCatalog installs the catalog ClassLoader: the operation runs against the
      // connector's own catalog instance, for example to call its authorization plugin.
      return doWithCatalogWrapper(ident, wrapper -> wrapper.doWithCatalog(operation));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to operate on catalog: " + ident, e);
    }
  }

  /**
   * Runs a callback with one leased wrapper, preserving the caller's context ClassLoader. The lease
   * is kept inside CatalogManager so callers cannot release it before detaching connector-backed
   * results. Wrapper methods install the catalog ClassLoader around connector calls.
   */
  <R> R doWithCatalogWrapper(NameIdentifier ident, ThrowableFunction<CatalogWrapper, R> operation)
      throws Exception {
    try (CatalogLease lease = acquireCatalogLease(ident)) {
      // Deliberately no ClassLoader swap around the whole callback. The wrapper's doWithXxxOps,
      // doWithPropertiesMeta and capabilities() already install the catalog ClassLoader for the
      // connector calls that need it, while the rest of the callback is Gravitino's own code:
      // entity-store reads in particular must not run with a connector ClassLoader as the thread
      // context ClassLoader, because MyBatis resolves resources and drivers through it and a
      // catalog that bundles its own JDBC driver would win those lookups.
      return operation.apply(lease.wrapper());
    }
  }

  /**
   * Creates a new catalog with the provided details.
   *
   * @param ident The identifier of the new catalog.
   * @param type The type of the new catalog.
   * @param provider The provider of the new catalog.
   * @param comment The comment for the new catalog.
   * @param properties The properties of the new catalog.
   * @return A metadata snapshot of the created catalog.
   * @throws NoSuchMetalakeException If the specified metalake does not exist.
   * @throws CatalogAlreadyExistsException If a catalog with the same identifier already exists.
   */
  @Override
  public Catalog createCatalog(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());

    Map<String, String> mergedConfig = buildCatalogConf(provider, properties);
    long uid = idGenerator.nextId();
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    Instant now = Instant.now();
    String creator = PrincipalUtils.getCurrentPrincipal().getName();
    CatalogEntity e =
        CatalogEntity.builder()
            .withId(uid)
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withType(type)
            .withProvider(provider)
            .withComment(comment)
            .withProperties(StringIdentifier.newPropertiesWithId(stringId, mergedConfig))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(creator)
                    .withCreateTime(now)
                    .withLastModifier(creator)
                    .withLastModifiedTime(now)
                    .build())
            .build();

    return TreeLockUtils.doWithTreeLock(
        metalakeIdent,
        LockType.WRITE,
        () -> {
          checkMetalake(metalakeIdent, store);
          lifecycleLock.readLock().lock();
          boolean needClean = false;
          try {
            checkOpen();
            needClean = true;
            store.put(e, false /* overwrite */);
            try (CatalogLease lease =
                createAndCacheCatalogLease(ident, () -> createCatalogWrapper(e, mergedConfig))) {
              Catalog catalog = toCatalogInfo(lease.wrapper());
              needClean = false;
              return catalog;
            }

          } catch (EntityAlreadyExistsException e1) {
            needClean = false;
            LOG.warn("Catalog {} already exists", ident, e1);
            throw new CatalogAlreadyExistsException("Catalog %s already exists", ident);

          } catch (IllegalArgumentException | NoSuchMetalakeException e2) {
            throw e2;

          } catch (Exception e3) {
            catalogCache.invalidate(ident);
            LOG.error("Failed to create catalog {}", ident, e3);
            if (e3 instanceof RuntimeException) {
              throw (RuntimeException) e3;
            }
            throw new RuntimeException(e3);

          } finally {
            try {
              if (needClean) {
                // since we put the catalog entity into the store but failed to create the catalog
                // instance,
                // we need to clean up the entity stored.
                try {
                  if (store.delete(ident, EntityType.CATALOG, true)) {
                    // This cleanup deletion writes a DROP record to the entity change log. Mark it
                    // as
                    // a local mutation so the change-log poller consumes that record's token
                    // instead
                    // of one meant for a later mutation of the same identifier. Without this, the
                    // poller could treat a subsequent local change as remote and spuriously
                    // invalidate (and asynchronously close) a cached catalog wrapper that is still
                    // in
                    // use, causing a NullPointerException.
                    markLocalMutation(ident);
                  }
                } catch (IOException e4) {
                  LOG.error("Failed to clean up catalog {}", ident, e4);
                }
              }
            } finally {
              lifecycleLock.readLock().unlock();
            }
          }
        });
  }

  /**
   * Test whether a catalog can be created with the specified parameters, without actually creating
   * it.
   *
   * @param ident The identifier of the catalog to be tested.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   */
  @Override
  public void testConnection(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());

    checkMetalake(metalakeIdent, store);
    try {
      if (store.exists(ident, EntityType.CATALOG)) {
        throw new CatalogAlreadyExistsException("Catalog %s already exists", ident);
      }

      Map<String, String> mergedConfig = buildCatalogConf(provider, properties);
      Instant now = Instant.now();
      String creator = PrincipalUtils.getCurrentPrincipal().getName();
      CatalogEntity dummyEntity =
          CatalogEntity.builder()
              .withId(DUMMY_ID.id())
              .withName(ident.name())
              .withNamespace(ident.namespace())
              .withType(type)
              .withProvider(provider)
              .withComment(comment)
              .withProperties(StringIdentifier.newPropertiesWithId(DUMMY_ID, mergedConfig))
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(creator)
                      .withCreateTime(now)
                      .withLastModifier(creator)
                      .withLastModifiedTime(now)
                      .build())
              .build();

      CatalogWrapper wrapper = createCatalogWrapper(dummyEntity, mergedConfig);
      try {
        wrapper.doWithCatalogOps(
            c -> {
              c.testConnection(ident, type, provider, comment, mergedConfig);
              return null;
            });
      } finally {
        wrapper.close();
      }
    } catch (GravitinoRuntimeException | UnsupportedOperationException e) {
      throw e;
    } catch (Exception e) {
      LOG.warn("Failed to test catalog creation {}", ident, e);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Test the connection of an existing catalog using its stored configuration.
   *
   * @param ident The identifier of the existing catalog.
   */
  @Override
  public void testConnection(NameIdentifier ident) {
    TreeLockUtils.doWithTreeLock(
        ident,
        LockType.READ,
        () -> {
          try (CatalogLease lease = acquireCatalogLease(ident)) {
            lease.catalog().checkMetalakeAndCatalogInUse();
            lease
                .wrapper()
                .doWithCatalogOps(
                    c -> {
                      c.testConnection(ident);
                      return null;
                    });
          } catch (UnsupportedOperationException e) {
            throw e;
          } catch (Exception e) {
            LOG.warn("Failed to test existing catalog connection {}", ident, e);
            if (e instanceof RuntimeException) {
              throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
          }
          return null;
        });
  }

  /**
   * Test the connection of an existing catalog with proposed changes without persisting them.
   *
   * @param ident The identifier of the existing catalog.
   * @param changes The proposed changes to apply temporarily.
   */
  @Override
  public void testConnection(NameIdentifier ident, CatalogChange... changes) {
    Preconditions.checkArgument(changes != null, "changes must not be null");
    if (changes.length == 0) {
      testConnection(ident);
      return;
    }

    TreeLockUtils.doWithTreeLock(
        ident,
        LockType.READ,
        () -> {
          try (CatalogLease storedLease = acquireCatalogLease(ident)) {
            CatalogWrapper storedWrapper = storedLease.wrapper();
            BaseCatalog<?> storedCatalog = storedLease.catalog();
            storedCatalog.checkMetalakeAndCatalogInUse();
            storedWrapper.doWithPropertiesMeta(
                metadata -> {
                  Pair<Map<String, String>, Map<String, String>> alterProperty =
                      getCatalogAlterProperty(changes);
                  validatePropertyForAlter(
                      metadata.catalogPropertiesMetadata(),
                      alterProperty.getLeft(),
                      alterProperty.getRight());
                  return null;
                });

            CatalogEntity storedEntity = storedCatalog.entity();
            Map<String, String> effectiveProperties =
                storedEntity.getProperties() == null
                    ? new HashMap<>()
                    : new HashMap<>(storedEntity.getProperties());
            CatalogEntity effectiveEntity =
                updateEntity(
                        newCatalogBuilder(storedEntity.namespace(), storedEntity),
                        effectiveProperties,
                        changes)
                    .build();
            effectiveEntity = convertFilesetCatalogEntity(effectiveEntity);

            CatalogWrapper temporaryWrapper = createCatalogWrapper(effectiveEntity, null);
            try {
              NameIdentifier effectiveIdent = effectiveEntity.nameIdentifier();
              temporaryWrapper.doWithCatalogOps(
                  operations -> {
                    operations.testConnection(effectiveIdent);
                    return null;
                  });
            } finally {
              temporaryWrapper.close();
            }
          } catch (UnsupportedOperationException e) {
            throw e;
          } catch (Exception e) {
            LOG.warn(
                "Failed to test existing catalog connection {} with proposed changes", ident, e);
            if (e instanceof RuntimeException) {
              throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
          }
          return null;
        });
  }

  @Override
  public void enableCatalog(NameIdentifier ident)
      throws NoSuchCatalogException, CatalogNotInUseException {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());
    TreeLockUtils.doWithTreeLock(
        metalakeIdent,
        LockType.WRITE,
        () -> {
          try (CatalogLease lease = acquireCatalogLease(ident)) {
            BaseCatalog baseCatalog = lease.catalog();
            baseCatalog.checkMetalakeInUse();

            if (baseCatalog.catalogInUse()) {
              return null;
            }

            store.update(
                ident,
                CatalogEntity.class,
                EntityType.CATALOG,
                catalog -> {
                  CatalogEntity.Builder newCatalogBuilder =
                      newCatalogBuilder(ident.namespace(), catalog);

                  Map<String, String> newProps =
                      catalog.getProperties() == null
                          ? new HashMap<>()
                          : new HashMap<>(catalog.getProperties());
                  newProps.put(PROPERTY_IN_USE, "true");
                  newCatalogBuilder.withProperties(newProps);

                  return newCatalogBuilder.build();
                });
            markLocalMutation(ident);
            catalogCache.invalidate(ident);
            return null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public void disableCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());
    TreeLockUtils.doWithTreeLock(
        metalakeIdent,
        LockType.WRITE,
        () -> {
          try (CatalogLease lease = acquireCatalogLease(ident)) {
            BaseCatalog baseCatalog = lease.catalog();
            baseCatalog.checkMetalakeInUse();

            if (!baseCatalog.catalogInUse()) {
              return null;
            }

            store.update(
                ident,
                CatalogEntity.class,
                EntityType.CATALOG,
                catalog -> {
                  CatalogEntity.Builder newCatalogBuilder =
                      newCatalogBuilder(ident.namespace(), catalog);

                  Map<String, String> newProps =
                      catalog.getProperties() == null
                          ? new HashMap<>()
                          : new HashMap<>(catalog.getProperties());
                  newProps.put(PROPERTY_IN_USE, "false");
                  newCatalogBuilder.withProperties(newProps);

                  return newCatalogBuilder.build();
                });
            markLocalMutation(ident);
            catalogCache.invalidate(ident);
            return null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Alters an existing catalog with the specified changes.
   *
   * @param ident The identifier of the catalog to alter.
   * @param changes The changes to apply to the catalog.
   * @return A metadata snapshot of the altered catalog.
   * @throws NoSuchCatalogException If the specified catalog does not exist.
   * @throws IllegalArgumentException If an unsupported catalog change is provided.
   */
  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {

    TreeLockUtils.doWithTreeLock(
        ident,
        LockType.READ,
        () -> {
          // There could be a race issue that someone is using the catalog from cache while we are
          // updating it. The lease keeps the wrapper alive for the whole validation.
          try (CatalogLease lease = acquireCatalogLease(ident)) {
            BaseCatalog catalog = lease.catalog();
            catalog.checkMetalakeAndCatalogInUse();

            try {
              lease
                  .wrapper()
                  .doWithPropertiesMeta(
                      f -> {
                        Pair<Map<String, String>, Map<String, String>> alterProperty =
                            getCatalogAlterProperty(changes);
                        validatePropertyForAlter(
                            f.catalogPropertiesMetadata(),
                            alterProperty.getLeft(),
                            alterProperty.getRight());
                        return null;
                      });
            } catch (IllegalArgumentException e1) {
              throw e1;
            } catch (Exception e) {
              LOG.error("Failed to alter catalog {}", ident, e);
              throw new RuntimeException(e);
            }
          }
          return null;
        });

    boolean containsRenameCatalog =
        Arrays.stream(changes).anyMatch(c -> c instanceof CatalogChange.RenameCatalog);
    NameIdentifier nameIdentifierForLock =
        containsRenameCatalog ? NameIdentifier.of(ident.namespace().level(0)) : ident;

    return TreeLockUtils.doWithTreeLock(
        nameIdentifierForLock,
        LockType.WRITE,
        () -> {
          // Hold the lifecycle read lock across the whole mutation, from before the entity is
          // persisted until the refreshed wrapper is published. Taking it only inside
          // createAndCacheCatalogLease would leave a window in which close() grabs the write lock
          // after the entity was already updated, so checkOpen() would fail the
          // caller's alter even though the change took effect. Same lock ordering as everywhere
          // else: tree lock first, lifecycle lock second.
          lifecycleLock.readLock().lock();
          try {
            checkOpen();
            CatalogEntity updatedCatalog =
                store.update(
                    ident,
                    CatalogEntity.class,
                    EntityType.CATALOG,
                    catalog -> {
                      CatalogEntity.Builder newCatalogBuilder =
                          newCatalogBuilder(ident.namespace(), catalog);

                      Map<String, String> newProps =
                          catalog.getProperties() == null
                              ? new HashMap<>()
                              : new HashMap<>(catalog.getProperties());
                      newCatalogBuilder = updateEntity(newCatalogBuilder, newProps, changes);

                      return newCatalogBuilder.build();
                    });
            // Invalidate after store.update() so that any background thread that tries to reload
            // the old catalog identifier from the store (after the invalidate) will get
            // NoSuchCatalogException instead of stale data. Invalidating before the update creates
            // a window where the background thread repopulates the cache with the old entity.
            markLocalMutation(ident);
            catalogCache.invalidate(ident);
            // The old fileset catalog's provider is "hadoop", whereas the new fileset catalog's
            // provider is "fileset", still using "hadoop" will lead to catalog loading issue. So
            // after reading the catalog entity, we convert it to the new fileset catalog entity.
            CatalogEntity convertedCatalog = convertFilesetCatalogEntity(updatedCatalog);
            // Use put() instead of get() to force the updated wrapper into the cache, preventing
            // a background thread from overwriting it with stale data between invalidate and put.
            try (CatalogLease lease =
                createAndCacheCatalogLease(
                    convertedCatalog.nameIdentifier(),
                    () -> createCatalogWrapper(convertedCatalog, null))) {
              return toCatalogInfo(lease.wrapper());
            }

          } catch (NoSuchEntityException ne) {
            LOG.warn("Catalog {} does not exist", ident, ne);
            throw new NoSuchCatalogException(CATALOG_DOES_NOT_EXIST_MSG, ident);

          } catch (IllegalArgumentException iae) {
            LOG.warn("Failed to alter catalog {} with unknown change", ident, iae);
            throw iae;

          } catch (IOException ioe) {
            LOG.error("Failed to alter catalog {}", ident, ioe);
            throw new RuntimeException(ioe);

          } finally {
            lifecycleLock.readLock().unlock();
          }
        });
  }

  @Override
  public boolean dropCatalog(NameIdentifier ident, boolean force)
      throws NonEmptyEntityException, CatalogInUseException {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());

    return TreeLockUtils.doWithTreeLock(
        metalakeIdent,
        LockType.WRITE,
        () -> {
          try (CatalogLease lease = acquireCatalogLease(ident)) {
            CatalogWrapper catalogWrapper = lease.wrapper();
            catalogWrapper.catalog().checkMetalakeInUse();

            boolean catalogInUse = catalogWrapper.catalog().catalogInUse();
            if (catalogInUse && !force) {
              throw new CatalogInUseException(
                  "Catalog %s is in use, please disable it first or use force option", ident);
            }

            Namespace schemaNs = Namespace.of(ident.namespace().level(0), ident.name());
            List<SchemaEntity> schemaEntities =
                store.list(schemaNs, SchemaEntity.class, EntityType.SCHEMA);
            if (!force && containsUserCreatedSchemas(schemaEntities, catalogWrapper)) {
              throw new NonEmptyCatalogException(
                  "Catalog %s has schemas, please drop them first or use force option", ident);
            }

            if (isManagedStorageCatalog(catalogWrapper)) {
              // For managed catalog, we need to call drop schema API to drop the underlying
              // entities as well as the related resource first. Directly deleting the metadata from
              // the store is not enough.
              schemaEntities.forEach(
                  schema -> {
                    try {
                      catalogWrapper.doWithSchemaOps(
                          ops -> ops.dropSchema(schema.nameIdentifier(), true));
                    } catch (Exception e) {
                      LOG.warn("Failed to drop schema {}", schema.nameIdentifier());
                      throw new RuntimeException(
                          "Failed to drop schema " + schema.nameIdentifier(), e);
                    }
                  });
            }

            // Finally, delete the catalog entity as well as all its sub-entities from the store.
            // Invalidate after store.delete() to prevent a background thread from repopulating
            // the cache with stale data between invalidate and delete.
            boolean deleted = store.delete(ident, EntityType.CATALOG, true);
            if (deleted) {
              markLocalMutation(ident);
              try {
                catalogWrapper.doWithCatalogOps(
                    operations -> {
                      if (operations instanceof CatalogDropAware) {
                        ((CatalogDropAware) operations).onCatalogDropped();
                      }
                      return null;
                    });
              } catch (Exception e) {
                LOG.warn("Failed to clean up resources for dropped catalog {}", ident, e);
              }
            }
            catalogCache.invalidate(ident);
            return deleted;

          } catch (NoSuchMetalakeException | NoSuchCatalogException ignored) {
            return false;
          } catch (GravitinoRuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Check if the given list of schema entities contains any currently existing user-created
   * schemas.
   *
   * <p>This method determines if there are valid user-created schemas by comparing the provided
   * schema entities with the actual schemas currently existing in the external data source. It
   * excludes:
   *
   * <ul>
   *   <li>1. Automatically generated schemas (such as Kafka catalog's "default" schema or
   *       JDBC-PostgreSQL catalog's "public" schema).
   *   <li>2. Schemas that have been dropped externally but still exist in the entity store.
   * </ul>
   *
   * @param schemaEntities The list of schema entities to check.
   * @param catalogWrapper The catalog wrapper for the catalog.
   * @return True if the list of schema entities contains any valid user-created schemas, false
   *     otherwise.
   * @throws Exception If an error occurs while checking the schemas.
   */
  private boolean containsUserCreatedSchemas(
      List<SchemaEntity> schemaEntities, CatalogWrapper catalogWrapper) throws Exception {
    if (schemaEntities.isEmpty()) {
      return false;
    }

    if (isManagedStorageCatalog(catalogWrapper)) {
      // For managed storage catalog, any existing schema entities are considered user-created. At
      // this point we already know schemaEntities is not empty, so we can return true directly
      // without further checks.
      return true;
    }

    if (schemaEntities.size() == 1) {
      String provider = catalogWrapper.catalog().provider();
      if ("kafka".equalsIgnoreCase(provider)) {
        return false;
      } else if ("jdbc-postgresql".equalsIgnoreCase(provider)) {
        // PostgreSQL catalog includes the "public" schema, see
        // https://github.com/apache/gravitino/issues/2314
        return !schemaEntities.get(0).name().equals("public");
      } else if ("hive".equalsIgnoreCase(provider)) {
        return !schemaEntities.get(0).name().equals("default");
      }
    }

    NameIdentifier[] allSchemas =
        catalogWrapper.doWithSchemaOps(
            schemaOps ->
                schemaOps.listSchemas(
                    NamespaceUtil.ofSchema(
                        catalogWrapper.catalog().entity().namespace().level(0),
                        catalogWrapper.catalog().name())));
    if (allSchemas.length == 0) {
      return false;
    }

    Set<String> availableSchemaNames =
        Arrays.stream(allSchemas).map(NameIdentifier::name).collect(Collectors.toSet());

    // Some schemas are dropped externally but still exist in the entity store — those are invalid.
    // Among schemas that exist in the underlying catalog, only those created via Gravitino carry a
    // StringIdentifier in their external properties; imported schemas do not.
    for (SchemaEntity schemaEntity : schemaEntities) {
      if (!availableSchemaNames.contains(schemaEntity.name())) {
        continue;
      }

      try {
        Schema schema =
            catalogWrapper.doWithSchemaOps(ops -> ops.loadSchema(schemaEntity.nameIdentifier()));
        Map<String, String> props = schema.properties();
        // If the backend cannot store a StringIdentifier (null or empty properties, e.g. MySQL
        // which does not support schema comments), we cannot tell whether the schema was created
        // by Gravitino or imported. Be conservative and treat it as user-created to avoid
        // accidental data loss.
        // Only skip a schema when properties are non-null, non-empty, and contain no
        // StringIdentifier — the reliable signal that the schema was imported from an external
        // catalog on a backend that does support identifier storage.
        if (props == null || props.isEmpty() || StringIdentifier.fromProperties(props) != null) {
          return true;
        }
      } catch (NoSuchSchemaException ex) {
        // A race between listSchemas and loadSchema is expected; treat as non-user-created.
        LOG.debug(
            "Schema {} no longer exists while checking whether it is user-created",
            schemaEntity.nameIdentifier());
      }
    }

    return false;
  }

  /**
   * Loads the catalog with the specified identifier, wraps it in a CatalogWrapper, caches the
   * wrapper for reuse, and takes a lease on it. The lease keeps the catalog and its ClassLoader
   * alive for the duration of the operation even if the cache evicts the wrapper concurrently, so
   * the caller must close the lease when the operation is done, ideally with try-with-resources.
   *
   * <p>If the cached wrapper has already been retired (by an eviction, an invalidation or a drop),
   * the stale entry is evicted and a fresh wrapper is loaded and cached.
   *
   * <p>Lookup, loading, and lease acquisition are atomic per catalog identifier.
   *
   * @param ident The identifier of the catalog to load.
   * @return A lease on the CatalogWrapper containing the loaded catalog.
   * @throws NoSuchCatalogException If the specified catalog does not exist.
   */
  CatalogLease acquireCatalogLease(NameIdentifier ident) throws NoSuchCatalogException {
    lifecycleLock.readLock().lock();
    try {
      checkOpen();
      AtomicReference<CatalogLease> acquiredLease = new AtomicReference<>();
      AtomicReference<CatalogWrapper> newlyLoadedWrapper = new AtomicReference<>();
      try {
        catalogCache
            .asMap()
            .compute(
                ident,
                (key, cachedWrapper) -> {
                  CatalogWrapper wrapper = cachedWrapper;
                  if (wrapper == null || !wrapper.tryAcquire()) {
                    wrapper = loadCatalogInternal(key);
                    newlyLoadedWrapper.set(wrapper);
                    Preconditions.checkState(
                        wrapper.tryAcquire(), "A newly loaded catalog wrapper cannot be retired");
                  }

                  CatalogLease lease = new CatalogLease(wrapper);
                  if (!acquiredLease.compareAndSet(null, lease)) {
                    lease.close();
                    throw new IllegalStateException(
                        "Catalog cache compute invoked its mapping function more than once");
                  }
                  return wrapper;
                });
      } catch (RuntimeException | Error e) {
        CatalogWrapper newlyLoaded = newlyLoadedWrapper.get();
        if (newlyLoaded != null) {
          newlyLoaded.retire();
        }
        CatalogLease lease = acquiredLease.get();
        if (lease != null) {
          lease.close();
        }
        throw e;
      }
      return Preconditions.checkNotNull(acquiredLease.get(), "Catalog lease was not acquired");
    } finally {
      lifecycleLock.readLock().unlock();
    }
  }

  /**
   * Loads the catalog with the specified identifier, wraps it in a CatalogWrapper, and caches the
   * wrapper for reuse. If the cached wrapper has already been retired, the stale entry is evicted
   * and a fresh wrapper is loaded and cached.
   *
   * <p>The returned wrapper is not leased, so a concurrent cache eviction may retire and close it
   * while the caller is still using it. Prefer {@link #acquireCatalogLease(NameIdentifier)}, which
   * keeps the wrapper alive for the duration of the operation.
   *
   * @param ident The identifier of the catalog to load.
   * @return The wrapped CatalogWrapper containing the loaded catalog.
   * @throws NoSuchCatalogException If the specified catalog does not exist.
   */
  @VisibleForTesting
  CatalogWrapper loadCatalogAndWrap(NameIdentifier ident) throws NoSuchCatalogException {
    lifecycleLock.readLock().lock();
    try {
      checkOpen();
      return loadCatalogAndWrapInternal(ident);
    } finally {
      lifecycleLock.readLock().unlock();
    }
  }

  private CatalogWrapper loadCatalogAndWrapInternal(NameIdentifier ident)
      throws NoSuchCatalogException {
    CatalogWrapper wrapper = catalogCache.get(ident, this::loadCatalogInternal);
    if (wrapper.isRetired()) {
      // The cached wrapper has already been retired, e.g. by a prior dropCatalog or cache eviction.
      // Evict the stale entry and reload a fresh one. Use a conditional remove so we do not clobber
      // a wrapper that another thread may have concurrently reloaded into the cache between our
      // initial get and this remove.
      catalogCache.asMap().remove(ident, wrapper);
      wrapper = catalogCache.get(ident, this::loadCatalogInternal);
    }
    return wrapper;
  }

  private CatalogLease createAndCacheCatalogLease(
      NameIdentifier ident, Supplier<CatalogWrapper> wrapperSupplier) {
    lifecycleLock.readLock().lock();
    try {
      checkOpen();
      CatalogWrapper wrapper = wrapperSupplier.get();
      Preconditions.checkState(wrapper.tryAcquire(), "A new catalog wrapper cannot be retired");
      CatalogLease lease = new CatalogLease(wrapper);
      try {
        catalogCache.put(ident, wrapper);
        return lease;
      } catch (RuntimeException | Error e) {
        wrapper.retire();
        lease.close();
        throw e;
      }
    } finally {
      lifecycleLock.readLock().unlock();
    }
  }

  private boolean isManagedStorageCatalog(CatalogWrapper catalogWrapper) {
    try {
      Capability capability = catalogWrapper.capabilities();
      return capability.managedStorage(Capability.Scope.SCHEMA).supported()
          && (capability.managedStorage(Capability.Scope.TABLE).supported()
              || capability.managedStorage(Capability.Scope.FILESET).supported()
              || capability.managedStorage(Capability.Scope.MODEL).supported());
    } catch (Exception e) {
      // This should not be happened, because capabilities() will never throw an exception here.
      throw new RuntimeException(e);
    }
  }

  private CatalogEntity.Builder newCatalogBuilder(Namespace namespace, CatalogEntity catalog) {
    CatalogEntity.Builder builder =
        CatalogEntity.builder()
            .withId(catalog.id())
            .withName(catalog.name())
            .withNamespace(namespace)
            .withType(catalog.getType())
            .withProvider(catalog.getProvider())
            .withComment(catalog.getComment());

    AuditInfo newInfo =
        AuditInfo.builder()
            .withCreator(catalog.auditInfo().creator())
            .withCreateTime(catalog.auditInfo().createTime())
            .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
            .withLastModifiedTime(Instant.now())
            .build();
    return builder.withAuditInfo(newInfo);
  }

  private Map<String, String> buildCatalogConf(String provider, Map<String, String> properties) {
    Map<String, String> newProperties = Optional.ofNullable(properties).orElse(Maps.newHashMap());
    // load catalog-related configuration from catalog-specific configuration file
    Map<String, String> catalogSpecificConfig = loadCatalogSpecificConfig(newProperties, provider);
    return mergeConf(newProperties, catalogSpecificConfig);
  }

  private Pair<Map<String, String>, Map<String, String>> getCatalogAlterProperty(
      CatalogChange... catalogChanges) {
    Map<String, String> upserts = Maps.newHashMap();
    Map<String, String> deletes = Maps.newHashMap();

    Arrays.stream(catalogChanges)
        .forEach(
            catalogChange -> {
              if (catalogChange instanceof SetProperty) {
                SetProperty setProperty = (SetProperty) catalogChange;
                upserts.put(setProperty.getProperty(), setProperty.getValue());
              } else if (catalogChange instanceof RemoveProperty) {
                RemoveProperty removeProperty = (RemoveProperty) catalogChange;
                deletes.put(removeProperty.getProperty(), removeProperty.getProperty());
              }
            });

    return Pair.of(upserts, deletes);
  }

  private CatalogWrapper loadCatalogInternal(NameIdentifier ident) throws NoSuchCatalogException {
    try {
      CatalogEntity entity = store.get(ident, EntityType.CATALOG, CatalogEntity.class);
      // The old fileset catalog's provider is "hadoop", whereas the new fileset catalog's
      // provider is "fileset", still using "hadoop" will lead to catalog loading issue. So
      // after reading the catalog entity, we convert it to the new fileset catalog entity.
      CatalogEntity convertedEntity = convertFilesetCatalogEntity(entity);
      return createCatalogWrapper(convertedEntity, null);

    } catch (NoSuchEntityException ne) {
      LOG.warn("Catalog {} does not exist", ident, ne);
      throw new NoSuchCatalogException(CATALOG_DOES_NOT_EXIST_MSG, ident);

    } catch (IOException ioe) {
      LOG.error("Failed to load catalog {}", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Create a catalog wrapper from the catalog entity and validate the given properties for
   * creation. The properties can be null if it is not needed to validate.
   *
   * @param entity The catalog entity.
   * @param propsToValidate The properties to validate.
   * @return The created catalog wrapper.
   */
  CatalogWrapper createCatalogWrapper(
      CatalogEntity entity, @Nullable Map<String, String> propsToValidate) {
    Map<String, String> conf = entity.getProperties();
    String provider = entity.getProvider();

    IsolatedClassLoader classLoader = createClassLoader(provider, conf);
    BaseCatalog<?> catalog = createBaseCatalog(classLoader, entity);

    CatalogWrapper wrapper = new CatalogWrapper(catalog, classLoader);
    // Validate catalog properties and initialize the config
    classLoader.withClassLoader(
        cl -> {
          validatePropertyForCreate(catalog.catalogPropertiesMetadata(), propsToValidate);

          // Call wrapper.catalog.properties() to make BaseCatalog#properties in IsolatedClassLoader
          // not null. Why do we do this? Because wrapper.catalog.properties() needs to be called in
          // the IsolatedClassLoader, as it needs to load the specific catalog class
          // such as HiveCatalog or similar. To simplify, we will preload the value of properties
          // so that AppClassLoader can get the value of properties.
          wrapper.catalog.properties();
          wrapper.catalog.capability();
          return null;
        },
        IllegalArgumentException.class);

    return wrapper;
  }

  /**
   * Get the resolved properties (filter out the hidden properties and add some required default
   * properties) of the catalog entity.
   *
   * @param entity The catalog entity.
   * @return The resolved properties.
   */
  private Map<String, String> getResolvedProperties(CatalogEntity entity) {
    // Resolve properties while the cached wrapper is protected by an operation lease.
    try (CatalogLease lease = acquireCatalogLease(entity.nameIdentifier())) {
      CatalogWrapper catalogWrapper = lease.wrapper();
      return catalogWrapper.classLoader.withClassLoader(
          cl -> catalogWrapper.catalog.properties(), RuntimeException.class);
    }
  }

  private Catalog toCatalogInfo(CatalogWrapper wrapper) {
    try {
      return wrapper.doWithCatalog(
          catalog ->
              catalog.entity().toCatalogInfoWithResolvedProps(new HashMap<>(catalog.properties())));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create catalog metadata snapshot", e);
    }
  }

  private void checkOpen() {
    Preconditions.checkState(!closed, "CatalogManager is already closed");
  }

  private BaseCatalog<?> createBaseCatalog(IsolatedClassLoader classLoader, CatalogEntity entity) {
    // Load Catalog class instance
    BaseCatalog<?> catalog = createCatalogInstance(classLoader, entity.getProvider());
    catalog.withCatalogConf(entity.getProperties()).withCatalogEntity(entity);
    catalog.initAuthorizationPluginInstance(classLoader);
    return catalog;
  }

  private IsolatedClassLoader createClassLoader(String provider, Map<String, String> conf) {
    if (config.get(Configs.CATALOG_LOAD_ISOLATED)) {
      String catalogPkgPath = buildPkgPath(conf, provider);
      String catalogConfPath = buildConfPath(conf, provider);
      ArrayList<String> libAndResourcesPaths = Lists.newArrayList(catalogPkgPath, catalogConfPath);
      BaseAuthorization.buildAuthorizationPkgPath(conf).ifPresent(libAndResourcesPaths::add);
      return IsolatedClassLoader.buildClassLoader(libAndResourcesPaths);
    } else {
      // This will use the current class loader, it is mainly used for test.
      return new IsolatedClassLoader(
          Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }
  }

  private BaseCatalog<?> createCatalogInstance(IsolatedClassLoader classLoader, String provider) {
    BaseCatalog<?> catalog;
    try {
      catalog =
          classLoader.withClassLoader(
              cl -> {
                try {
                  Class<? extends CatalogProvider> providerClz =
                      lookupCatalogProvider(provider, cl);
                  return (BaseCatalog) providerClz.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                  LOG.error("Failed to load catalog with provider: {}", provider, e);
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      LOG.error("Failed to load catalog with class loader", e);
      throw new RuntimeException(e);
    }

    if (catalog == null) {
      throw new RuntimeException("Failed to load catalog with provider: " + provider);
    }
    return catalog;
  }

  private Map<String, String> loadCatalogSpecificConfig(
      Map<String, String> properties, String provider) {
    if ("test".equals(provider)) {
      return Maps.newHashMap();
    }

    String catalogSpecificConfigFile = provider + ".conf";
    Map<String, String> catalogSpecificConfig = Maps.newHashMap();

    String fullPath =
        buildConfPath(properties, provider) + File.separator + catalogSpecificConfigFile;
    try (InputStream inputStream = FileUtils.openInputStream(new File(fullPath))) {
      Properties loadProperties = new Properties();
      loadProperties.load(inputStream);
      loadProperties.forEach(
          (key, value) -> catalogSpecificConfig.put(key.toString(), value.toString()));
    } catch (Exception e) {
      LOG.warn(
          "Failed to load catalog specific configurations, file name: '{}'",
          catalogSpecificConfigFile,
          e);
    }
    return catalogSpecificConfig;
  }

  static Map<String, String> mergeConf(Map<String, String> properties, Map<String, String> conf) {
    Map<String, String> mergedConf = conf != null ? Maps.newHashMap(conf) : Maps.newHashMap();
    Optional.ofNullable(properties).ifPresent(mergedConf::putAll);
    return Collections.unmodifiableMap(mergedConf);
  }

  /**
   * Build the config path from the specific provider. Usually, the configuration file is under the
   * conf and conf and package are under the same directory.
   */
  private String buildConfPath(Map<String, String> properties, String provider) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(gravitinoHome != null, "GRAVITINO_HOME not set");
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;

    String confPath;
    String pkg = properties.get(Catalog.PROPERTY_PACKAGE);
    if (pkg != null) {
      confPath = String.join(File.separator, pkg, "conf");
    } else if (testEnv) {
      if (CONTRIB_CATALOGS_TYPES.contains(provider)) {
        confPath =
            String.join(
                File.separator,
                gravitinoHome,
                "catalogs-contrib",
                "catalog-" + provider,
                "build",
                "resources",
                "main");
        return confPath;
      }

      confPath =
          String.join(
              File.separator,
              gravitinoHome,
              "catalogs",
              "catalog-" + provider,
              "build",
              "resources",
              "main");
    } else {
      confPath = String.join(File.separator, gravitinoHome, "catalogs", provider, "conf");
    }
    return confPath;
  }

  private String buildPkgPath(Map<String, String> conf, String provider) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(gravitinoHome != null, "GRAVITINO_HOME not set");
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;

    String pkg = conf.get(Catalog.PROPERTY_PACKAGE);
    String pkgPath;
    if (pkg != null) {
      pkgPath = String.join(File.separator, pkg, "libs");
    } else if (testEnv) {
      // In test, the catalog package is under the build directory.
      if (CONTRIB_CATALOGS_TYPES.contains(provider)) {
        pkgPath =
            String.join(
                File.separator,
                gravitinoHome,
                "catalogs-contrib",
                "catalog-" + provider,
                "build",
                "libs");
        return pkgPath;
      }

      pkgPath =
          String.join(
              File.separator, gravitinoHome, "catalogs", "catalog-" + provider, "build", "libs");
    } else {
      // In real environment, the catalog package is under the catalog directory.
      pkgPath = String.join(File.separator, gravitinoHome, "catalogs", provider, "libs");
    }

    return pkgPath;
  }

  private Class<? extends CatalogProvider> lookupCatalogProvider(String provider, ClassLoader cl) {
    ServiceLoader<CatalogProvider> loader = ServiceLoader.load(CatalogProvider.class, cl);

    List<Class<? extends CatalogProvider>> providers =
        Streams.stream(loader.iterator())
            .filter(p -> p.shortName().equalsIgnoreCase(provider))
            .map(CatalogProvider::getClass)
            .collect(Collectors.toList());

    if (providers.isEmpty()) {
      throw new IllegalArgumentException("No catalog provider found for: " + provider);
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException("Multiple catalog providers found for: " + provider);
    } else {
      return Iterables.getOnlyElement(providers);
    }
  }

  private CatalogEntity.Builder updateEntity(
      CatalogEntity.Builder builder, Map<String, String> newProps, CatalogChange... changes) {
    for (CatalogChange change : changes) {
      if (change instanceof CatalogChange.RenameCatalog) {
        CatalogChange.RenameCatalog rename = (CatalogChange.RenameCatalog) change;

        if (Entity.SYSTEM_CATALOG_RESERVED_NAME.equals(
            ((CatalogChange.RenameCatalog) change).getNewName())) {
          throw new IllegalArgumentException(
              "Can't rename a catalog with with reserved name `system`");
        }

        builder.withName(rename.getNewName());

      } else if (change instanceof CatalogChange.UpdateCatalogComment) {
        CatalogChange.UpdateCatalogComment updateComment =
            (CatalogChange.UpdateCatalogComment) change;
        builder.withComment(updateComment.getNewComment());

      } else if (change instanceof CatalogChange.SetProperty) {
        CatalogChange.SetProperty setProperty = (CatalogChange.SetProperty) change;
        newProps.put(setProperty.getProperty(), setProperty.getValue());

      } else if (change instanceof CatalogChange.RemoveProperty) {
        CatalogChange.RemoveProperty removeProperty = (CatalogChange.RemoveProperty) change;
        newProps.remove(removeProperty.getProperty());

      } else {
        throw new IllegalArgumentException(
            "Unsupported catalog change: " + change.getClass().getSimpleName());
      }
    }

    return builder.withProperties(newProps);
  }

  private CatalogEntity convertFilesetCatalogEntity(CatalogEntity entity) {
    if (entity.getType() != FILESET) {
      return entity;
    }

    if ("hadoop".equalsIgnoreCase(entity.getProvider())) {
      // If the provider is "hadoop", we need to convert it to a fileset catalog entity.
      // This is a special case to maintain compatibility.
      return CatalogEntity.builder()
          .withId(entity.id())
          .withName(entity.name())
          .withNamespace(entity.namespace())
          .withType(FILESET)
          .withProvider("fileset")
          .withComment(entity.getComment())
          .withProperties(entity.getProperties())
          .withAuditInfo(
              AuditInfo.builder()
                  .withCreator(entity.auditInfo().creator())
                  .withCreateTime(entity.auditInfo().createTime())
                  .withLastModifier(entity.auditInfo().lastModifier())
                  .withLastModifiedTime(entity.auditInfo().lastModifiedTime())
                  .build())
          .build();
    }

    // If the provider is not "hadoop", we assume it is already a fileset catalog entity.
    return entity;
  }

  /**
   * Set the metalake in-use status in a specified catalog.
   *
   * @param nameIdentifier The name identifier of the catalog.
   * @param status The in-use status to set.
   */
  public void setMetalakeInUseStatus(NameIdentifier nameIdentifier, boolean status) {
    updateCatalogProperty(nameIdentifier, PROPERTY_METALAKE_IN_USE, String.valueOf(status));
  }

  private void updateCatalogProperty(
      NameIdentifier nameIdentifier, String propertyKey, String propertyValue) {
    try {
      store.update(
          nameIdentifier,
          CatalogEntity.class,
          EntityType.CATALOG,
          catalog -> {
            CatalogEntity.Builder newCatalogBuilder =
                newCatalogBuilder(nameIdentifier.namespace(), catalog);

            Map<String, String> newProps =
                catalog.getProperties() == null
                    ? new HashMap<>()
                    : new HashMap<>(catalog.getProperties());
            newProps.put(propertyKey, propertyValue);
            newCatalogBuilder.withProperties(newProps);

            return newCatalogBuilder.build();
          });
      markLocalMutation(nameIdentifier);
      catalogCache.invalidate(nameIdentifier);

    } catch (NoSuchCatalogException e) {
      LOG.error("Catalog {} does not exist", nameIdentifier, e);
      throw new RuntimeException(e);
    } catch (IllegalArgumentException e) {
      LOG.error(
          "Failed to update catalog {} property {} with unknown change",
          nameIdentifier,
          propertyKey,
          e);
      throw e;
    } catch (IOException ioe) {
      LOG.error("Failed to update catalog {} property {}", nameIdentifier, propertyKey, ioe);
      throw new RuntimeException(ioe);
    }
  }
}
