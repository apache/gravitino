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
package org.apache.gravitino.catalog.fileset;

import static org.apache.gravitino.catalog.hadoop.fs.Constants.CONFIG_RESOURCES;
import static org.apache.gravitino.catalog.hadoop.fs.HDFSFileSystemProvider.SCHEME_HDFS;
import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.apache.gravitino.file.Fileset.PROPERTY_ALLOW_EXISTING_LOCATION_AS_MANAGED;
import static org.apache.gravitino.file.Fileset.PROPERTY_CATALOG_PLACEHOLDER;
import static org.apache.gravitino.file.Fileset.PROPERTY_DEFAULT_LOCATION_NAME;
import static org.apache.gravitino.file.Fileset.PROPERTY_FILESET_PLACEHOLDER;
import static org.apache.gravitino.file.Fileset.PROPERTY_LOCATION_PLACEHOLDER_PREFIX;
import static org.apache.gravitino.file.Fileset.PROPERTY_MULTIPLE_LOCATIONS_PREFIX;
import static org.apache.gravitino.file.Fileset.PROPERTY_SCHEMA_PLACEHOLDER;
import static org.apache.gravitino.metrics.MetricNames.FILESYSTEM_CACHE;

import com.codahale.metrics.caffeine.MetricsStatsCounter;
import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityFieldLimits;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.catalog.FilesetFileOps;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.catalog.hadoop.fs.HDFSFileSystemProxy;
import org.apache.gravitino.catalog.hadoop.fs.InMemoryFileSystemCredentialsProvider;
import org.apache.gravitino.catalog.hadoop.fs.SupportsCredentialVending;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.credential.PathContext;
import org.apache.gravitino.connector.credential.SupportsPathBasedCredentials;
import org.apache.gravitino.credential.CatalogCredentialManager;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.CredentialUtils;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.dto.file.FileInfoDTO;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.file.FileInfo;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.FilesetCatalogMetricsSource;
import org.apache.gravitino.secret.SecretAlterChanges;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.secret.SecretVersionChanges;
import org.apache.gravitino.secret.SecretVersionChangesHolder;
import org.apache.gravitino.utils.ExceptionMessages;
import org.apache.gravitino.utils.FilesetUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.RenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilesetCatalogOperations extends ManagedSchemaOperations
    implements CatalogOperations, FilesetCatalog, FilesetFileOps, SupportsPathBasedCredentials {
  private static final String SCHEMA_DOES_NOT_EXIST_MSG = "Schema %s does not exist";
  private static final String FILESET_DOES_NOT_EXIST_MSG = "Fileset %s does not exist";
  private static final String SLASH = "/";
  private static final String CATALOG_PLACEHOLDER = "{{catalog}}";
  private static final String PROBE_UNSUPPORTED = "probe unsupported";
  private static final String PROBE_UNSUPPORTED_DETAIL_PREFIX = PROBE_UNSUPPORTED + ":";

  // location placeholder pattern format: {{placeholder}}
  private static final Pattern LOCATION_PLACEHOLDER_PATTERN = Pattern.compile("\\{\\{(.*?)\\}\\}");
  private static final Logger LOG = LoggerFactory.getLogger(FilesetCatalogOperations.class);

  private final EntityStore store;

  private final SecretManager secretManager;

  @Nullable private final Supplier<CatalogCredentialManager> credentialManagerSupplier;

  private HasPropertyMetadata propertiesMetadata;

  @VisibleForTesting Configuration hadoopConf;

  @VisibleForTesting Map<String, Path> catalogStorageLocations;

  @VisibleForTesting Map<String, String> conf;

  private CatalogInfo catalogInfo;

  private Map<String, FileSystemProvider> fileSystemProvidersMap;

  private FileSystemProvider defaultFileSystemProvider;

  private boolean disableFSOps;

  private FilesetCatalogMetricsSource catalogMetricsSource;

  @VisibleForTesting ScheduledThreadPoolExecutor scheduler;

  // TODO: Move the file system cache to the module of hadoop-common and reuse it in the
  // BaseGVFSOperations
  // The caches load their entries on a pool, so that a caller waits for an entry being loaded only
  // until its own deadline.
  @VisibleForTesting AsyncCache<FileSystemCacheKey, FileSystem> fileSystemCache;

  // Resolving the HDFS login settings parses XML, so the settings are cached.
  // Expiring after write lets edits to the config.resources files reach the key within about an
  // hour.
  // Settings that expand no Hadoop variable are the same for configurations that differ only in
  // their identifier, so they are cached by the configuration without it. Otherwise that entry is
  // empty, and the settings are cached by the whole configuration.
  @VisibleForTesting
  final AsyncCache<ConfKey, Optional<Map<String, String>>> sharedHdfsLoginSettingsCache =
      Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(1, TimeUnit.HOURS).buildAsync();

  @VisibleForTesting
  final AsyncCache<ConfKey, Map<String, String>> hdfsLoginSettingsCache =
      Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(1, TimeUnit.HOURS).buildAsync();

  @VisibleForTesting
  final ThreadPoolExecutor fileSystemExecutor =
      new ThreadPoolExecutor(
          Math.max(2, Math.min(Runtime.getRuntime().availableProcessors() * 2, 16)),
          Math.max(2, Math.min(Runtime.getRuntime().availableProcessors() * 2, 32)),
          5L,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(1000),
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("fileset-filesystem-getter-pool-%d")
              .build(),
          new ThreadPoolExecutor.AbortPolicy()) {
        {
          allowCoreThreadTimeOut(true);
        }
      };

  // A pool of its own, so that resolving login settings, which only reads files, does not wait
  // behind file system creations and logins that may hang.
  @VisibleForTesting
  final ThreadPoolExecutor loginSettingsExecutor =
      new ThreadPoolExecutor(
          Math.max(2, Math.min(Runtime.getRuntime().availableProcessors(), 4)),
          Math.max(2, Math.min(Runtime.getRuntime().availableProcessors(), 4)),
          5L,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(1000),
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("fileset-login-settings-pool-%d")
              .build(),
          new ThreadPoolExecutor.AbortPolicy()) {
        {
          allowCoreThreadTimeOut(true);
        }
      };

  FilesetCatalogOperations(EntityStore store, SecretManager secretManager) {
    this(store, secretManager, null);
  }

  FilesetCatalogOperations(
      EntityStore store,
      SecretManager secretManager,
      @Nullable Supplier<CatalogCredentialManager> credentialManagerSupplier) {
    this.store = store;
    this.secretManager = secretManager;
    this.credentialManagerSupplier = credentialManagerSupplier;
  }

  FilesetCatalogOperations(Supplier<CatalogCredentialManager> credentialManagerSupplier) {
    this(
        GravitinoEnv.getInstance().entityStore(),
        GravitinoEnv.getInstance().secretManager(),
        credentialManagerSupplier);
  }

  static class FileSystemCacheKey {
    /**
     * When the path is a path without scheme such as 'file','hdfs', etc., then the scheme and
     * authority are both null
     *
     * <p>NOTE: The filesystem cache key includes the scheme, authority, current user and, for HDFS,
     * the login settings of the configuration. Changes to other configuration of the fileset will
     * not affect the cached filesystem.
     */
    @Nullable private final String scheme;

    @Nullable private final String authority;
    private final String currentUser;
    private final Map<String, String> loginSettings;

    FileSystemCacheKey(String scheme, String authority, Map<String, String> loginSettings) {
      this.scheme = scheme;
      this.authority = authority;
      this.currentUser = PrincipalUtils.getCurrentUserName();
      this.loginSettings = loginSettings;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FileSystemCacheKey)) {
        return false;
      }
      FileSystemCacheKey that = (FileSystemCacheKey) o;
      return Objects.equals(scheme, that.scheme)
          && Objects.equals(authority, that.authority)
          && Objects.equals(currentUser, that.currentUser)
          && loginSettings.equals(that.loginSettings);
    }

    @Override
    public int hashCode() {
      int result = 31 * (scheme == null ? 0 : scheme.hashCode());
      result = 31 * result + (authority == null ? 0 : authority.hashCode());
      result = 31 * result + currentUser.hashCode();
      result = 31 * result + loginSettings.hashCode();
      return result;
    }
  }

  /**
   * The configuration a file system logs in with, its settings, and whether they expand variables.
   */
  static class HdfsLogin {
    final Configuration configuration;
    final Map<String, String> settings;
    final boolean expandsVariables;

    HdfsLogin(Configuration configuration, Map<String, String> settings, boolean expandsVariables) {
      this.configuration = configuration;
      this.settings = settings;
      this.expandsVariables = expandsVariables;
    }
  }

  /**
   * A configuration as a key of the login settings caches, without the identifier of its entity if
   * asked to. It wraps the configuration as it is, so a lookup copies nothing; a key that a cache
   * keeps holds a copy.
   */
  static final class ConfKey {
    private final Map<String, String> conf;
    private final boolean withoutIdentifier;
    private final int hashCode;

    private ConfKey(Map<String, String> conf, boolean withoutIdentifier) {
      this.conf = conf;
      this.withoutIdentifier = withoutIdentifier;
      int hash = conf.hashCode();
      if (withoutIdentifier && conf.containsKey(StringIdentifier.ID_KEY)) {
        // The hash code of a map is the sum of those of its entries.
        hash -=
            StringIdentifier.ID_KEY.hashCode()
                ^ Objects.hashCode(conf.get(StringIdentifier.ID_KEY));
      }
      this.hashCode = hash;
    }

    static ConfKey of(Map<String, String> conf) {
      return new ConfKey(conf, false);
    }

    // Filesets that inherit all other properties have configurations that differ only in their
    // identifiers.
    static ConfKey withoutIdentifier(Map<String, String> conf) {
      return new ConfKey(conf, true);
    }

    ConfKey copy() {
      return new ConfKey(Collections.unmodifiableMap(new HashMap<>(conf)), withoutIdentifier);
    }

    private boolean ignores(String key) {
      return withoutIdentifier && StringIdentifier.ID_KEY.equals(key);
    }

    private int size() {
      return withoutIdentifier && conf.containsKey(StringIdentifier.ID_KEY)
          ? conf.size() - 1
          : conf.size();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ConfKey)) {
        return false;
      }
      ConfKey that = (ConfKey) o;
      if (withoutIdentifier != that.withoutIdentifier
          || hashCode != that.hashCode
          || size() != that.size()) {
        return false;
      }
      for (Map.Entry<String, String> entry : conf.entrySet()) {
        if (ignores(entry.getKey())) {
          continue;
        }
        String value = that.conf.get(entry.getKey());
        if (!Objects.equals(entry.getValue(), value)
            || (value == null && !that.conf.containsKey(entry.getKey()))) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  /**
   * The result of a task on a pool, which fails at a deadline. Caffeine logs every entry whose load
   * fails with an exception other than a timeout, so a task that fails completes with null, which
   * removes an entry silently, and keeps its failure here.
   */
  static class TimedTask<T> extends CompletableFuture<T> {
    private final long deadlineNanos;
    @Nullable private volatile Throwable failure;

    private TimedTask(long deadlineNanos) {
      this.deadlineNanos = deadlineNanos;
    }

    // When the deadline passes first, the task leaves the queue or is interrupted, and a result
    // that it returns later is closed. A login can ignore interrupts.
    static <T> TimedTask<T> start(ExecutorService executor, Callable<T> task, long deadlineNanos) {
      TimedTask<T> timed = new TimedTask<>(deadlineNanos);
      long timeoutNanos = deadlineNanos - System.nanoTime();
      if (timeoutNanos <= 0) {
        // The caller has no time left, so it starts no task that it would not wait for.
        timed.completeExceptionally(new TimeoutException());
        return timed;
      }
      Future<?> running =
          executor.submit(
              () -> {
                T result;
                try {
                  result = task.call();
                } catch (Throwable t) {
                  timed.failure = t;
                  timed.complete(null);
                  return;
                }
                // close() may have interrupted this thread, and completing runs the stages that
                // close the result after close().
                Thread.interrupted();
                if (!timed.complete(result) && result instanceof Closeable) {
                  // The interrupt that cancels this task may still come, so the result is closed
                  // on another thread.
                  Runnable close = () -> closeQuietly(result);
                  if (!startThread("gravitino-fileset-late-result-closer", close)) {
                    close.run();
                  }
                }
              });
      timed.orTimeout(timeoutNanos, TimeUnit.NANOSECONDS);
      timed.whenComplete(
          (result, e) -> {
            if (e == null) {
              return;
            }
            // A task that has not started leaves the queue, so that it takes no room from others.
            if (executor instanceof ThreadPoolExecutor
                && ((ThreadPoolExecutor) executor).getQueue().remove(running)) {
              return;
            }
            // Interrupting a thread that reads a file through a channel waits until the read
            // returns, so neither the caller nor the timer of orTimeout does it. Without a thread,
            // the task is not interrupted, and its result is closed once it returns.
            startThread("gravitino-fileset-task-canceller", () -> running.cancel(true));
          });
      return timed;
    }

    private static boolean startThread(String name, Runnable action) {
      try {
        Thread thread = new Thread(action, name);
        thread.setDaemon(true);
        thread.start();
        return true;
      } catch (Throwable t) {
        LOG.warn("Failed to start the thread {}", name, t);
        return false;
      }
    }

    // Waits for the result until the deadline. A task that failed at the deadline of the caller
    // that started it fails with a timeout here too.
    @Nullable
    static <T> T await(CompletableFuture<T> future, long deadlineNanos)
        throws InterruptedException, ExecutionException, TimeoutException {
      T result;
      try {
        result = future.get(Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
      } catch (TimeoutException e) {
        // The task is due too when this caller started it. Failing it now, rather than when its
        // timer fires, closes a result that comes after this caller gave up instead of caching it.
        if (future instanceof TimedTask && ((TimedTask<?>) future).deadlineNanos == deadlineNanos) {
          future.completeExceptionally(new TimeoutException());
        }
        throw e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw e;
      } catch (ExecutionException e) {
        if (e.getCause() instanceof TimeoutException) {
          throw (TimeoutException) e.getCause();
        }
        throw e;
      }
      Throwable failure = future instanceof TimedTask ? ((TimedTask<?>) future).failure : null;
      if (failure != null) {
        throw new ExecutionException(failure);
      }
      return result;
    }

    // Gets the entry, loading it on a miss, and waits for it until the deadline. An entry that
    // fails at the earlier deadline of the caller that started it is loaded again with the time
    // this caller has left. A caller whose wait ends with the failure of a load another caller
    // started, with that caller's configuration, loads again until it gets a result or its own
    // failure, as each caller does with a synchronous cache. A load that returns null is not loaded
    // again: the caller gets null, which only a file system creation returns.
    @Nullable
    static <K, V> V awaitEntry(
        AsyncCache<K, V> cache,
        K key,
        BiFunction<? super K, Executor, CompletableFuture<V>> load,
        long deadlineNanos)
        throws InterruptedException, ExecutionException, TimeoutException {
      return awaitEntry(cache, key, null, load, deadlineNanos);
    }

    // As above, but a miss inserts the key that storedKey returns, which equals the given key, and
    // passes it to load.
    @Nullable
    static <K, V> V awaitEntry(
        AsyncCache<K, V> cache,
        K key,
        @Nullable Supplier<K> storedKey,
        BiFunction<? super K, Executor, CompletableFuture<V>> load,
        long deadlineNanos)
        throws InterruptedException, ExecutionException, TimeoutException {
      while (true) {
        AtomicBoolean started = new AtomicBoolean();
        CompletableFuture<V> entry = storedKey == null ? null : cache.getIfPresent(key);
        if (entry == null) {
          entry =
              cache.get(
                  storedKey == null ? key : storedKey.get(),
                  (k, executor) -> {
                    started.set(true);
                    return load.apply(k, executor);
                  });
        }
        try {
          return await(entry, deadlineNanos);
        } catch (TimeoutException e) {
          if (!entry.isCompletedExceptionally() || deadlineNanos - System.nanoTime() <= 0) {
            throw e;
          }
        } catch (ExecutionException e) {
          if (started.get() || deadlineNanos - System.nanoTime() <= 0) {
            throw e;
          }
        }
        // Caffeine removes a failed entry once it completes, maybe after this caller wakes up.
        cache.asMap().remove(key, entry);
      }
    }
  }

  public FilesetCatalogOperations() {
    this(
        GravitinoEnv.getInstance().entityStore(), GravitinoEnv.getInstance().secretManager(), null);
  }

  @Override
  public EntityStore store() {
    return store;
  }

  public CatalogInfo getCatalogInfo() {
    return catalogInfo;
  }

  public Map<String, String> getConf() {
    return conf;
  }

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    this.propertiesMetadata = propertiesMetadata;
    this.catalogInfo = info;
    this.conf = config;

    this.disableFSOps =
        (boolean)
            propertiesMetadata
                .catalogPropertiesMetadata()
                .getOrDefault(config, FilesetCatalogPropertiesMetadata.DISABLE_FILESYSTEM_OPS);

    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    // Metrics System could be null in UT.
    if (metricsSystem != null) {
      this.catalogMetricsSource =
          new FilesetCatalogMetricsSource(catalogInfo.namespace().toString(), catalogInfo.name());
    }

    if (!disableFSOps) {
      this.fileSystemProvidersMap =
          ImmutableMap.<String, FileSystemProvider>builder()
              .putAll(FileSystemUtils.getFileSystemProviders())
              .build();

      String defaultFileSystemProviderName =
          (String)
              propertiesMetadata
                  .catalogPropertiesMetadata()
                  .getOrDefault(config, FilesetCatalogPropertiesMetadata.DEFAULT_FS_PROVIDER);
      this.defaultFileSystemProvider =
          FileSystemUtils.getFileSystemProviderByName(
              fileSystemProvidersMap, defaultFileSystemProviderName);

      scheduler =
          new ScheduledThreadPoolExecutor(
              1,
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat("file-system-cache-for-fileset" + "-%d")
                  .build());

      Caffeine<Object, Object> cacheBuilder =
          Caffeine.newBuilder()
              .expireAfterAccess(1, TimeUnit.HOURS)
              .removalListener(
                  (key, value, cause) -> {
                    FileSystemCacheKey cacheKey = (FileSystemCacheKey) key;
                    LOG.debug(
                        "Removing FileSystem from cache: scheme={}, authority={}, user={}, cause={}",
                        cacheKey == null ? null : cacheKey.scheme,
                        cacheKey == null ? null : cacheKey.authority,
                        cacheKey == null ? null : cacheKey.currentUser,
                        cause);
                    if (value == null) {
                      return;
                    }
                    // Runs in a stage that nothing reads, so a failure is only logged here.
                    try {
                      ((FileSystem) value).close();
                    } catch (Throwable t) {
                      LOG.warn("Failed to close FileSystem instance in cache", t);
                    }
                  })
              .scheduler(Scheduler.forScheduledExecutorService(scheduler));

      // Metrics System could be null in UT.
      if (metricsSystem != null) {
        cacheBuilder.recordStats(
            () ->
                new MetricsStatsCounter(
                    catalogMetricsSource.getMetricRegistry(), FILESYSTEM_CACHE));
      }
      this.fileSystemCache = cacheBuilder.buildAsync();
    }

    try {
      this.catalogStorageLocations = getAndCheckCatalogStorageLocations(config);
    } catch (RuntimeException | Error e) {
      // A catalog keeps no operations whose initialization failed, so nothing else closes the file
      // systems created to check the locations.
      try {
        close();
      } catch (IOException | RuntimeException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }

    // Metrics System could be null in UT.
    if (metricsSystem != null) {
      metricsSystem.register(catalogMetricsSource);
    }
  }

  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    try {
      NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
      if (!store.exists(schemaIdent, Entity.EntityType.SCHEMA)) {
        throw new NoSuchSchemaException(SCHEMA_DOES_NOT_EXIST_MSG, schemaIdent);
      }

      List<FilesetEntity> filesets =
          store.list(namespace, FilesetEntity.class, Entity.EntityType.FILESET);
      return filesets.stream()
          .map(f -> NameIdentifier.of(namespace, f.name()))
          .toArray(NameIdentifier[]::new);
    } catch (IOException e) {
      throw ExceptionMessages.wrap("Failed to list filesets under namespace " + namespace, e);
    }
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    try {
      FilesetEntity filesetEntity =
          store.get(ident, Entity.EntityType.FILESET, FilesetEntity.class);

      return FilesetImpl.builder()
          .withName(ident.name())
          .withType(filesetEntity.filesetType())
          .withComment(filesetEntity.comment())
          .withStorageLocations(filesetEntity.storageLocations())
          .withProperties(filesetEntity.properties())
          .withAuditInfo(filesetEntity.auditInfo())
          .build();

    } catch (NoSuchEntityException exception) {
      throw new NoSuchFilesetException(exception, FILESET_DOES_NOT_EXIST_MSG, ident);
    } catch (IOException ioe) {
      throw ExceptionMessages.wrap("Failed to load fileset %s" + ident, ioe);
    }
  }

  @Override
  public FileInfo[] listFiles(NameIdentifier filesetIdent, String locationName, String subPath)
      throws NoSuchFilesetException, IOException {
    if (disableFSOps) {
      LOG.warn("Filesystem operations disabled, rejecting listFiles for {}", filesetIdent);
      throw new UnsupportedOperationException("Filesystem operations are disabled on this server");
    }

    Fileset fileset = loadFileset(filesetIdent);
    String actualPath = getFileLocation(fileset, subPath, locationName);

    Path actualPathObj = new Path(actualPath);
    Map<String, String> fsConf =
        mergeUpLevelConfigurations(filesetIdent, fileset.properties(), actualPathObj);
    FileSystem fileSystem = getFileSystemWithCache(actualPathObj, fsConf);
    Path formalizedPath =
        new Path(actualPath).makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());

    FileSystem fs = getFileSystemWithCache(formalizedPath, fsConf);
    if (!fs.exists(formalizedPath)) {
      throw new IllegalArgumentException(
          String.format(
              "Path %s does not exist in fileset %s", formalizedPath.toString(), filesetIdent));
    }

    String catalogName = filesetIdent.namespace().level(1);
    String schemaName = filesetIdent.namespace().level(2);
    String filesetName = filesetIdent.name();

    try {
      return Arrays.stream(fs.listStatus(formalizedPath))
          .map(
              status ->
                  FileInfoDTO.builder()
                      .name(status.getPath().getName())
                      .isDir(status.isDirectory())
                      .size(status.isDirectory() ? 0L : status.getLen())
                      .lastModified(status.getModificationTime())
                      .path(buildGVFSFilePath(catalogName, schemaName, filesetName, subPath))
                      .build())
          .toArray(FileInfo[]::new);

    } catch (IOException e) {
      throw ExceptionMessages.wrap("Failed to list files in fileset" + filesetIdent, e);
    }
  }

  @Override
  public Fileset createMultipleLocationFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      Map<String, String> storageLocations,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    // Check the comment before the storage locations are created, the entity validation only
    // happens after that.
    EntityFieldLimits.checkMaxLength(
        comment, EntityFieldLimits.MAX_COMMENT_LENGTH, "comment", Entity.EntityType.FILESET);

    storageLocations.forEach(
        (name, path) -> {
          if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Location name must not be blank");
          }
          EntityFieldLimits.checkMaxLength(
              name,
              EntityFieldLimits.MAX_STORAGE_LOCATION_NAME_LENGTH,
              "storage location name",
              Entity.EntityType.FILESET);
          if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException(
                "Storage location must not be blank for location name: " + name);
          }
        });

    try {
      if (store.exists(ident, Entity.EntityType.FILESET)) {
        throw new FilesetAlreadyExistsException("Fileset %s already exists", ident);
      }
    } catch (IOException ioe) {
      throw ExceptionMessages.wrap("Failed to check if fileset " + ident + " exists", ioe);
    }

    SchemaEntity schemaEntity;
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    try {
      schemaEntity = store.get(schemaIdent, Entity.EntityType.SCHEMA, SchemaEntity.class);
    } catch (NoSuchEntityException exception) {
      throw new NoSuchSchemaException(exception, SCHEMA_DOES_NOT_EXIST_MSG, schemaIdent);
    } catch (IOException ioe) {
      throw ExceptionMessages.wrap("Failed to load schema " + schemaIdent, ioe);
    }

    // For external fileset, the storageLocation must be set.
    if (type == Fileset.Type.EXTERNAL) {
      if (storageLocations.isEmpty()) {
        throw new IllegalArgumentException(
            "Storage location must be set for external fileset " + ident);
      }
      storageLocations.forEach(
          (locationName, location) -> {
            if (StringUtils.isBlank(location)) {
              throw new IllegalArgumentException(
                  "Storage location must be set for external fileset "
                      + ident
                      + " with location name "
                      + locationName);
            }
          });
    }

    // Either catalog property "location", or schema property "location", or storageLocation must be
    // set for managed fileset.
    Map<String, Path> schemaPaths =
        getAndCheckSchemaPaths(schemaIdent.name(), schemaEntity.properties());
    if (schemaPaths.isEmpty() && storageLocations.isEmpty()) {
      throw new IllegalArgumentException(
          "Storage location must be set for fileset "
              + ident
              + " when it's catalog and schema location are not set");
    }
    storageLocations.forEach((k, location) -> checkPlaceholderValue(location));

    Map<String, Path> filesetPaths =
        calculateFilesetPaths(
            schemaIdent.name(), ident.name(), storageLocations, schemaPaths, properties);
    // The names inherited from the catalog or schema locations are only known now, check them
    // before any directory is created.
    filesetPaths
        .keySet()
        .forEach(
            name ->
                EntityFieldLimits.checkMaxLength(
                    name,
                    EntityFieldLimits.MAX_STORAGE_LOCATION_NAME_LENGTH,
                    "storage location name",
                    Entity.EntityType.FILESET));
    properties = setDefaultLocationIfAbsent(properties, filesetPaths);

    ImmutableMap.Builder<String, Path> filesetPathsBuilder = ImmutableMap.builder();
    if (disableFSOps) {
      filesetPaths.forEach(
          (locationName, location) -> {
            // If the location does not have scheme and filesystem operations are disabled in the
            // server side, we cannot formalize the path by filesystem, neither can we do in the
            // client side, so we should throw an exception here.
            if (location.toUri().getScheme() == null) {
              throw new IllegalArgumentException(
                  "Storage location must have scheme for fileset if filesystem operations are "
                      + "disabled in the server side, location: "
                      + location
                      + ", location name: "
                      + locationName);
            }

            filesetPathsBuilder.put(locationName, location);
          });
    } else {
      try {
        boolean allowExistingLocationAsManaged =
            (boolean)
                propertiesMetadata
                    .filesetPropertiesMetadata()
                    .getOrDefault(properties, PROPERTY_ALLOW_EXISTING_LOCATION_AS_MANAGED);
        Map<String, Map<String, String>> fileSystemConfigurations = Maps.newHashMap();
        // Formalize and validate every location before creating any directories. Otherwise,
        // creating a child location can make another location in the request non-empty.
        for (Map.Entry<String, Path> entry : filesetPaths.entrySet()) {
          Map<String, String> fsConf =
              mergeUpLevelConfigurations(ident, properties, entry.getValue());
          fileSystemConfigurations.put(entry.getKey(), fsConf);
          FileSystem tmpFs = getFileSystemWithCache(entry.getValue(), fsConf);
          Path formalizePath =
              entry.getValue().makeQualified(tmpFs.getUri(), tmpFs.getWorkingDirectory());

          filesetPathsBuilder.put(entry.getKey(), formalizePath);
          FileSystem fs = getFileSystemWithCache(formalizePath, fsConf);

          if (fs.exists(formalizePath)) {
            if (fs.getFileStatus(formalizePath).isFile()) {
              throw new IllegalArgumentException(
                  "Fileset location cannot be a file: "
                      + formalizePath
                      + ", location name: "
                      + entry.getKey());
            }
            // A managed fileset takes its data with it on drop, so silently adopting a location
            // that already holds files would put data Gravitino never created at risk.
            if (type == Fileset.Type.MANAGED
                && !allowExistingLocationAsManaged
                && fs.listStatusIterator(formalizePath).hasNext()) {
              throw new IllegalArgumentException(
                  "Location "
                      + formalizePath
                      + " for managed fileset "
                      + ident
                      + " already exists and is not empty, location name: "
                      + entry.getKey()
                      + ". Register it as an "
                      + Fileset.Type.EXTERNAL
                      + " fileset with this location set explicitly, or set "
                      + PROPERTY_ALLOW_EXISTING_LOCATION_AS_MANAGED
                      + "=true to manage it anyway");
            }
          }
        }

        for (Map.Entry<String, Path> entry : filesetPathsBuilder.build().entrySet()) {
          Path formalizePath = entry.getValue();
          FileSystem fs =
              getFileSystemWithCache(formalizePath, fileSystemConfigurations.get(entry.getKey()));
          if (!fs.exists(formalizePath)) {
            if (!fs.mkdirs(formalizePath)) {
              throw new RuntimeException(
                  "Failed to create fileset "
                      + ident
                      + " location "
                      + formalizePath
                      + " with location name "
                      + entry.getKey());
            }

            LOG.info(
                "Created fileset {} location {} with location name {}",
                ident,
                formalizePath,
                entry.getKey());
          } else {
            LOG.info(
                "Fileset {} manages the existing location {} with location name {}",
                ident,
                formalizePath,
                entry.getKey());
          }
        }

      } catch (IOException ioe) {
        throw ExceptionMessages.wrap("Failed to create fileset " + ident, ioe);
      }
    }

    Map<String, String> formattedStorageLocations =
        Maps.transformValues(filesetPathsBuilder.build(), Path::toString);
    validateLocationHierarchy(
        Maps.transformValues(schemaPaths, Path::toString), formattedStorageLocations);

    StringIdentifier stringId = StringIdentifier.fromProperties(properties);
    Preconditions.checkArgument(stringId != null, "Property String identifier should not be null");

    FilesetEntity filesetEntity =
        FilesetEntity.builder()
            .withName(ident.name())
            .withId(stringId.id())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withFilesetType(type)
            // Store the storageLocation to the store. If the "storageLocation" is null for managed
            // fileset, Gravitino will get and store the location based on the catalog/schema's
            // location and store it to the store.
            .withStorageLocations(formattedStorageLocations)
            .withProperties(properties)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      // The existence check is advisory; the strict insert decides concurrent creates.
      store.put(filesetEntity, false /* overwrite */);
    } catch (EntityAlreadyExistsException exception) {
      throw new FilesetAlreadyExistsException(exception, "Fileset %s already exists", ident);
    } catch (NoSuchEntityException exception) {
      // The schema can disappear after the check near the start of this method. The relational
      // store detects that race while taking the parent-schema lock; translate its storage-level
      // exception into the catalog API's documented missing-schema exception.
      throw new NoSuchSchemaException(exception, SCHEMA_DOES_NOT_EXIST_MSG, schemaIdent);
    } catch (IOException ioe) {
      throw ExceptionMessages.wrap("Failed to create fileset " + ident, ioe);
    }

    return FilesetImpl.builder()
        .withName(ident.name())
        .withComment(comment)
        .withType(type)
        .withStorageLocations(formattedStorageLocations)
        .withProperties(filesetEntity.properties())
        .withAuditInfo(filesetEntity.auditInfo())
        .build();
  }

  private Map<String, String> setDefaultLocationIfAbsent(
      Map<String, String> properties, Map<String, Path> filesetPaths) {
    Preconditions.checkArgument(
        filesetPaths != null && !filesetPaths.isEmpty(), "Fileset paths must not be null or empty");

    if (filesetPaths.size() == 1) {
      // If the fileset has only one location, it is the default location.
      String defaultLocationName = filesetPaths.keySet().iterator().next();
      if (properties == null || properties.isEmpty()) {
        return Collections.singletonMap(PROPERTY_DEFAULT_LOCATION_NAME, defaultLocationName);
      }
      if (!properties.containsKey(PROPERTY_DEFAULT_LOCATION_NAME)) {
        return ImmutableMap.<String, String>builder()
            .putAll(properties)
            .put(PROPERTY_DEFAULT_LOCATION_NAME, defaultLocationName)
            .build();
      }

      Preconditions.checkArgument(
          defaultLocationName.equals(properties.get(PROPERTY_DEFAULT_LOCATION_NAME)),
          "Default location name must be the same as the fileset location name");
      return ImmutableMap.copyOf(properties);
    }

    // multiple locations
    Preconditions.checkArgument(
        properties != null
            && !properties.isEmpty()
            && properties.containsKey(PROPERTY_DEFAULT_LOCATION_NAME)
            && filesetPaths.containsKey(properties.get(PROPERTY_DEFAULT_LOCATION_NAME)),
        "The fileset property "
            + PROPERTY_DEFAULT_LOCATION_NAME
            + " must be set and must be one of the fileset locations, "
            + "location names: "
            + filesetPaths.keySet()
            + ", default location name: "
            + Optional.ofNullable(properties)
                .map(p -> p.get(PROPERTY_DEFAULT_LOCATION_NAME))
                .orElse(null));
    return ImmutableMap.copyOf(properties);
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, FilesetAlreadyExistsException, IllegalArgumentException {
    try {
      if (!store.exists(ident, Entity.EntityType.FILESET)) {
        throw new NoSuchFilesetException(FILESET_DOES_NOT_EXIST_MSG, ident);
      }
    } catch (IOException ioe) {
      throw ExceptionMessages.wrap("Failed to load fileset " + ident, ioe);
    }

    SecretVersionChangesHolder stagedSecrets = new SecretVersionChangesHolder();
    boolean alterCommitted = false;
    try {
      FilesetEntity updatedFilesetEntity =
          store.update(
              ident,
              FilesetEntity.class,
              Entity.EntityType.FILESET,
              existing -> {
                Map<String, String> currentProperties =
                    existing.properties() == null
                        ? new HashMap<>()
                        : new HashMap<>(existing.properties());
                Pair<FilesetChange[], SecretVersionChanges> secretResult =
                    SecretAlterChanges.prepareFilesetChanges(
                        secretManager, currentProperties, existing.id(), changes);
                stagedSecrets.set(secretResult.getRight());
                return updateFilesetEntity(ident, existing, secretResult.getLeft());
              });
      alterCommitted = true;
      secretManager.commitSecrets(stagedSecrets.get());
      return FilesetImpl.builder()
          .withName(updatedFilesetEntity.name())
          .withComment(updatedFilesetEntity.comment())
          .withType(updatedFilesetEntity.filesetType())
          .withStorageLocations(updatedFilesetEntity.storageLocations())
          .withProperties(updatedFilesetEntity.properties())
          .withAuditInfo(updatedFilesetEntity.auditInfo())
          .build();
    } catch (IOException ioe) {
      throw ExceptionMessages.wrap("Failed to update fileset " + ident, ioe);
    } catch (NoSuchEntityException nsee) {
      throw new NoSuchFilesetException(nsee, FILESET_DOES_NOT_EXIST_MSG, ident);
    } catch (EntityAlreadyExistsException e) {
      String newName =
          RenameUtils.getLastRenameName(
              ident.name(),
              changes,
              FilesetChange.RenameFileset.class,
              FilesetChange.RenameFileset::getNewName);
      throw new FilesetAlreadyExistsException(
          e, "Fileset %s already exists", NameIdentifier.of(ident.namespace(), newName));
    } finally {
      if (!alterCommitted) {
        secretManager.rollbackSecrets(stagedSecrets.get());
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Does not delete the fileset's write-through secrets: {@code
   * FilesetOperationDispatcher.dropFileset}, which every server drop goes through, deletes them
   * after this method returns. A cascading {@link #dropSchema} deletes them for the filesets it
   * drops itself, because a catalog force drop reaches it without the dispatcher.
   */
  @Override
  public boolean dropFileset(NameIdentifier ident) {
    return dropFilesetAndGet(ident).isPresent();
  }

  /**
   * Removes the storage of a managed fileset while its metadata delete can still be rolled back.
   *
   * <p>The first location that cannot be removed stops the loop, so the drop is rejected before it
   * takes away more data than it already has. The locations removed up to that point are gone for
   * good, but attempting the remaining ones would only widen that gap.
   */
  private void deleteManagedFilesetStorage(NameIdentifier ident, FilesetEntity filesetEntity)
      throws IOException {
    Map<String, Path> storageLocations =
        Maps.transformValues(filesetEntity.storageLocations(), Path::new);
    for (Map.Entry<String, Path> entry : storageLocations.entrySet()) {
      String locationName = entry.getKey();
      Path location = entry.getValue();
      Map<String, String> fsConf =
          mergeUpLevelConfigurations(ident, filesetEntity.properties(), location);
      FileSystem fs = getFileSystemWithCache(location, fsConf);
      if (!fs.exists(location)) {
        LOG.warn(
            "Fileset {} location {} with location name {} does not exist",
            ident,
            location,
            locationName);
        continue;
      }
      if (!fs.delete(location, true) && fs.exists(location)) {
        // A false return also covers a location that somebody else removed between the check above
        // and this call. Only a location that is still there is a reason to reject the drop.
        throw new IOException(
            String.format(
                "Failed to delete fileset %s location %s with location name %s",
                ident, location, locationName));
      }
    }
  }

  @Override
  public String getFileLocation(NameIdentifier ident, String subPath, String locationName)
      throws NoSuchFilesetException, NoSuchLocationNameException {

    Fileset fileset = loadFileset(ident);
    return getFileLocation(fileset, subPath, locationName);
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    // Check the comment before the schema directories are created, the entity validation only
    // happens after that.
    EntityFieldLimits.checkMaxLength(
        comment, EntityFieldLimits.MAX_COMMENT_LENGTH, "comment", Entity.EntityType.SCHEMA);

    if (disableFSOps) {
      return super.createSchema(ident, comment, properties);
    }

    try {
      if (store.exists(ident, Entity.EntityType.SCHEMA)) {
        throw new SchemaAlreadyExistsException("Schema %s already exists", ident);
      }
    } catch (IOException ioe) {
      throw ExceptionMessages.wrap("Failed to check if schema " + ident + " exists", ioe);
    }

    Map<String, Path> schemaPaths = getAndCheckSchemaPaths(ident.name(), properties);
    schemaPaths.forEach(
        (locationName, schemaPath) -> {
          if (schemaPath != null && !containsPlaceholder(schemaPath.toString())) {
            try {
              Map<String, String> fsConf =
                  mergeUpLevelConfigurations(ident, properties, schemaPath);
              FileSystem fs = getFileSystemWithCache(schemaPath, fsConf);
              if (fs.exists(schemaPath) && fs.getFileStatus(schemaPath).isFile()) {
                throw new IllegalArgumentException(
                    "Fileset schema location cannot be a file: "
                        + schemaPath
                        + ", location name: "
                        + locationName);
              }

              if (!fs.exists(schemaPath)) {
                if (!fs.mkdirs(schemaPath)) {
                  // Fail the operation when failed to create the schema path.
                  throw new RuntimeException(
                      "Failed to create schema "
                          + ident
                          + " location: "
                          + schemaPath
                          + " with location name: "
                          + locationName);
                }
                LOG.info(
                    "Created schema {} location: {} with location name: {}",
                    ident,
                    schemaPath,
                    locationName);
              } else {
                LOG.info(
                    "Schema {} manages the existing location: {} with location name: {}",
                    ident,
                    schemaPath,
                    locationName);
              }

            } catch (IOException ioe) {
              throw ExceptionMessages.wrap(
                  "Failed to create schema " + ident + " location " + schemaPath, ioe);
            }
          }
        });

    return super.createSchema(ident, comment, properties);
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    try {
      if (!store.exists(ident, Entity.EntityType.SCHEMA)) {
        throw new NoSuchSchemaException(SCHEMA_DOES_NOT_EXIST_MSG, ident);
      }
    } catch (IOException ioe) {
      throw ExceptionMessages.wrap("Failed to check if schema " + ident + " exists", ioe);
    }

    // note: we need to invalidate the related fileset cache when the schema rename change is
    // supported.
    return super.alterSchema(ident, changes);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    try {
      Namespace filesetNs =
          NamespaceUtil.ofFileset(
              ident.namespace().level(0), // metalake name
              ident.namespace().level(1), // catalog name
              ident.name() // schema name
              );

      List<FilesetEntity> filesets =
          store.list(filesetNs, FilesetEntity.class, Entity.EntityType.FILESET);
      if (!filesets.isEmpty() && !cascade) {
        throw new NonEmptySchemaException("Schema %s is not empty", ident);
      }

      // Catalog force-drop reaches this method without SchemaOperationDispatcher's fileset
      // cleanup. Drop each fileset before its schema so a storage failure preserves the failed
      // fileset and its parent for retry. Stop at the first failure; earlier drops stay committed.
      if (cascade) {
        for (FilesetEntity fileset : filesets) {
          dropFilesetAndGet(fileset.nameIdentifier())
              .ifPresent(
                  deleted -> secretManager.deleteSecretsFromProperties(deleted.properties()));
        }
      }

      // Only functions may be deleted with the schema; remaining filesets still reject the drop.
      // Keep function and schema metadata in one transaction until directory cleanup succeeds.
      boolean dropped =
          store
              .deleteSchemaAndGet(
                  ident,
                  cascade,
                  schemaEntity -> {
                    if (!disableFSOps) {
                      try {
                        deleteEmptySchemaLocations(ident, schemaEntity);
                      } catch (IOException ioe) {
                        throw new UncheckedIOException(ioe);
                      }
                    }
                  })
              .isPresent();
      if (dropped) {
        LOG.info("Deleted schema {}", ident);
      }
      return dropped;

    } catch (NonEmptyEntityException neee) {
      throw new NonEmptySchemaException(neee, "Schema %s is not empty", ident);
    } catch (NoSuchEntityException ne) {
      LOG.warn("Schema {} does not exist", ident);
      return false;
    } catch (UncheckedIOException uioe) {
      throw ExceptionMessages.wrap(
          "Failed to delete schema " + ident + " location", uioe.getCause());
    } catch (IOException ioe) {
      throw ExceptionMessages.wrap("Failed to delete schema " + ident + " location", ioe);
    }
  }

  @Override
  public void testConnection(NameIdentifier catalogIdent) {
    if (disableFSOps) {
      throw new UnsupportedOperationException(
          "Fileset connection testing requires filesystem operations to be enabled");
    }
    if (catalogStorageLocations.isEmpty()) {
      throw new IllegalArgumentException("Fileset catalog has no catalog-level location to test");
    }

    List<String> failures = new ArrayList<>();
    catalogStorageLocations.forEach(
        (locationName, location) -> {
          try {
            Path path = resolveProbeLocation(location.toString(), catalogIdent);
            probeLocation(path);
          } catch (Exception e) {
            String displayName = displayLocationName(locationName);
            String failure = failureDescription(e);
            LOG.warn(
                "Fileset catalog connection probe failed for catalog {} at {} ({})",
                catalogIdent,
                displayName,
                failure,
                e);
            failures.add(displayName + " (" + failure + ")");
          }
        });

    if (!failures.isEmpty()) {
      throw new ConnectionFailedException(
          "Failed to test Fileset catalog connection: %s", String.join("; ", failures));
    }
  }

  @Override
  public void close() throws IOException {
    // do nothing
    if (scheduler != null) {
      scheduler.shutdownNow();
    }

    if (!fileSystemExecutor.isShutdown()) {
      fileSystemExecutor.shutdownNow();
    }

    if (!loginSettingsExecutor.isShutdown()) {
      loginSettingsExecutor.shutdownNow();
    }

    if (fileSystemCache != null) {
      // Also closes the file systems still being created, once they are.
      fileSystemCache
          .asMap()
          .values()
          .forEach(
              entry ->
                  entry.thenAccept(
                      fs -> {
                        if (fs == null) {
                          return;
                        }
                        // Runs in a stage that nothing reads, so a failure is only logged here.
                        try {
                          fs.close();
                        } catch (Throwable t) {
                          LOG.warn("Failed to close FileSystem instance in cache", t);
                        }
                      }));
      fileSystemCache.synchronous().cleanUp();
    }
    // The keys hold the merged configuration, which may carry resolved secrets.
    sharedHdfsLoginSettingsCache.synchronous().invalidateAll();
    hdfsLoginSettingsCache.synchronous().invalidateAll();

    // Metrics System could be null in UT.
    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    if (metricsSystem != null) {
      metricsSystem.unregister(catalogMetricsSource);
    }
  }

  private void deleteEmptySchemaLocations(NameIdentifier ident, SchemaEntity schemaEntity)
      throws IOException {
    Map<String, String> properties =
        Optional.ofNullable(schemaEntity.properties()).orElse(Collections.emptyMap());
    Map<String, Path> schemaPaths = getAndCheckSchemaPaths(ident.name(), properties);
    for (Map.Entry<String, Path> entry : schemaPaths.entrySet()) {
      String locationName = entry.getKey();
      Path schemaPath = entry.getValue();
      Map<String, String> fsConf = mergeUpLevelConfigurations(ident, properties, schemaPath);
      FileSystem fs = getFileSystemWithCache(schemaPath, fsConf);
      // Filesystem changes cannot be rolled back with metadata. A retry may find a location gone.
      if (!fs.exists(schemaPath) || fs.listStatus(schemaPath).length != 0) {
        continue;
      }
      if (!fs.delete(schemaPath, true) && fs.exists(schemaPath)) {
        throw new IOException(
            String.format(
                "Failed to delete schema %s location %s with location name %s",
                ident, schemaPath, locationName));
      }
      LOG.info(
          "Deleted schema {} location {} with location name {}", ident, schemaPath, locationName);
    }
  }

  private Optional<FilesetEntity> dropFilesetAndGet(NameIdentifier ident) {
    try {
      // The relational store runs this cleanup after the metadata CAS wins but before committing
      // its transaction. The callback therefore sees the exact deleted snapshot, and an I/O
      // failure can still restore the metadata so the caller may fix permissions and retry.
      //
      // The price is that the recursive storage delete runs inside that transaction, holding the
      // fileset rows and a pooled connection for as long as the filesystem takes. Dropping a
      // fileset with a very large tree is therefore a slow write for that fileset, and enough
      // concurrent drops can hold up the connection pool.
      return store.deleteAndGet(
          ident,
          Entity.EntityType.FILESET,
          FilesetEntity.class,
          filesetEntity -> {
            if (!disableFSOps && filesetEntity.filesetType() == Fileset.Type.MANAGED) {
              try {
                deleteManagedFilesetStorage(ident, filesetEntity);
              } catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
              }
            }
          });
    } catch (NoSuchEntityException ne) {
      LOG.warn("Fileset {} does not exist", ident);
      return Optional.empty();
    } catch (UncheckedIOException uioe) {
      throw ExceptionMessages.wrap("Failed to delete fileset " + ident, uioe.getCause());
    } catch (IOException ioe) {
      throw ExceptionMessages.wrap("Failed to delete fileset " + ident, ioe);
    }
  }

  private void validateLocationHierarchy(
      Map<String, String> schemaLocations, Map<String, String> filesetLocations) {
    if (schemaLocations == null
        || filesetLocations == null
        || schemaLocations.isEmpty()
        || filesetLocations.isEmpty()) {
      return;
    }

    filesetLocations.forEach(
        (filesetLocationName, filesetLocation) ->
            schemaLocations.forEach(
                (schemaLocationName, schemaLocation) -> {
                  if (ensureTrailingSlash(schemaLocation)
                      .startsWith(ensureTrailingSlash(filesetLocation))) {
                    throw new IllegalArgumentException(
                        String.format(
                            "The fileset location %s with location name %s is not allowed "
                                + "to be the parent of the schema location %s with location name %s",
                            filesetLocation,
                            filesetLocationName,
                            schemaLocation,
                            schemaLocationName));
                  }
                }));
  }

  private boolean existBlankValue(Map<String, String> properties, String key) {
    return properties.containsKey(key) && StringUtils.isBlank(properties.get(key));
  }

  private Map<String, Path> getAndCheckCatalogStorageLocations(Map<String, String> properties) {
    if (existBlankValue(properties, FilesetCatalogPropertiesMetadata.LOCATION)) {
      // If the properties contain the catalog property "location", it must not be blank.
      throw new IllegalArgumentException(
          "The value of the catalog property "
              + FilesetCatalogPropertiesMetadata.LOCATION
              + " must not be blank");
    }

    ImmutableMap.Builder<String, Path> catalogStorageLocations = ImmutableMap.builder();
    String unnamedLocation =
        (String)
            propertiesMetadata
                .catalogPropertiesMetadata()
                .getOrDefault(properties, FilesetCatalogPropertiesMetadata.LOCATION);
    if (StringUtils.isNotBlank(unnamedLocation)) {
      checkPlaceholderValue(unnamedLocation);
      catalogStorageLocations.put(
          LOCATION_NAME_UNKNOWN, new Path(ensureTrailingSlash(unnamedLocation)));
    }

    properties.forEach(
        (k, v) -> {
          if (k.startsWith(PROPERTY_MULTIPLE_LOCATIONS_PREFIX)) {
            String locationName = k.substring(PROPERTY_MULTIPLE_LOCATIONS_PREFIX.length());
            if (StringUtils.isBlank(locationName)) {
              throw new IllegalArgumentException("Location name must not be blank");
            }

            if (StringUtils.isBlank(v)) {
              throw new IllegalArgumentException(
                  "Location value must not be blank for " + "location name: " + locationName);
            }

            checkPlaceholderValue(v);

            if (!disableFSOps && !containsPlaceholder(v)) {
              Path path = new Path(v);
              // At catalog initialization, only merge catalog config and user-defined location
              // configs.
              Map<String, String> fsConf = new HashMap<>(conf);
              fsConf.putAll(
                  FilesetUtil.getUserDefinedFileSystemConfigs(
                      path.toUri(),
                      conf,
                      FilesetCatalogPropertiesMetadata.FS_GRAVITINO_PATH_CONFIG_PREFIX));
              FileSystem fs = getFileSystemWithCache(path, fsConf);
              try {
                if (fs.exists(path) && fs.getFileStatus(path).isFile()) {
                  throw new IllegalArgumentException(
                      "Fileset catalog location cannot be a file: "
                          + v
                          + ", location name: "
                          + locationName);
                }
              } catch (IOException e) {
                // REST responses can include the cause chain. Keep backend diagnostics in the log.
                LOG.warn(
                    "Failed to check {} for Fileset catalog {}.{}",
                    k,
                    catalogInfo.namespace(),
                    catalogInfo.name(),
                    e);
                throw new ConnectionFailedException(
                    "Failed to check Fileset catalog storage at %s (%s)", k, failureCategory(e));
              }
            }

            catalogStorageLocations.put(locationName, new Path((ensureTrailingSlash(v))));
          }
        });
    return catalogStorageLocations.build();
  }

  private Map<String, Path> getAndCheckSchemaPaths(
      String schemaName, Map<String, String> schemaProps) {
    if (existBlankValue(schemaProps, FilesetSchemaPropertiesMetadata.LOCATION)) {
      // If the properties contain the schema property "location", it must not be blank.
      throw new IllegalArgumentException(
          "The value of the schema property "
              + FilesetSchemaPropertiesMetadata.LOCATION
              + " must not be blank");
    }

    Map<String, Path> schemaPaths = new HashMap<>();
    catalogStorageLocations.forEach(
        (name, path) -> {
          if (containsPlaceholder(path.toString())) {
            schemaPaths.put(name, path);
          } else {
            schemaPaths.put(name, new Path(path, schemaName));
          }
        });

    String unnamedSchemaLocation =
        (String)
            propertiesMetadata
                .schemaPropertiesMetadata()
                .getOrDefault(schemaProps, FilesetSchemaPropertiesMetadata.LOCATION);
    checkPlaceholderValue(unnamedSchemaLocation);
    Optional.ofNullable(unnamedSchemaLocation)
        .map(this::ensureTrailingSlash)
        .map(Path::new)
        .ifPresent(p -> schemaPaths.put(LOCATION_NAME_UNKNOWN, p));

    schemaProps.forEach(
        (k, path) -> {
          if (k.startsWith(PROPERTY_MULTIPLE_LOCATIONS_PREFIX)) {
            checkPlaceholderValue(path);
            String locationName = k.substring(PROPERTY_MULTIPLE_LOCATIONS_PREFIX.length());
            if (StringUtils.isBlank(locationName)) {
              throw new IllegalArgumentException("Location name must not be blank");
            }
            Optional.ofNullable(path)
                .map(this::ensureTrailingSlash)
                .map(Path::new)
                .ifPresent(p -> schemaPaths.put(locationName, p));
          }
        });
    return ImmutableMap.copyOf(schemaPaths);
  }

  private String ensureTrailingSlash(String path) {
    return path.endsWith(SLASH) ? path : path + SLASH;
  }

  private String removeTrailingSlash(String path) {
    return path.endsWith(SLASH) ? path.substring(0, path.length() - 1) : path;
  }

  private String ensureLeadingSlash(String path) {
    return path.startsWith(SLASH) ? path : SLASH + path;
  }

  private FilesetEntity updateFilesetEntity(
      NameIdentifier ident, FilesetEntity filesetEntity, FilesetChange... changes) {
    Map<String, String> props =
        filesetEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(filesetEntity.properties());
    String newName = ident.name();
    String newComment = filesetEntity.comment();
    Fileset.Type newType = filesetEntity.filesetType();

    for (FilesetChange change : changes) {
      if (change instanceof FilesetChange.SetProperty) {
        FilesetChange.SetProperty setProperty = (FilesetChange.SetProperty) change;
        props.put(setProperty.getProperty(), setProperty.getValue());
      } else if (change instanceof FilesetChange.RemoveProperty) {
        FilesetChange.RemoveProperty removeProperty = (FilesetChange.RemoveProperty) change;
        props.remove(removeProperty.getProperty());
      } else if (change instanceof FilesetChange.RenameFileset) {
        newName = ((FilesetChange.RenameFileset) change).getNewName();
      } else if (change instanceof FilesetChange.UpdateFilesetComment) {
        newComment = ((FilesetChange.UpdateFilesetComment) change).getNewComment();
      } else if (change instanceof FilesetChange.UpdateFilesetType) {
        newType = ((FilesetChange.UpdateFilesetType) change).getNewType();
      } else if (change instanceof FilesetChange.RemoveComment) {
        newComment = null;
      } else {
        throw new IllegalArgumentException(
            "Unsupported fileset change: " + change.getClass().getSimpleName());
      }
    }

    return FilesetEntity.builder()
        .withName(newName)
        .withNamespace(ident.namespace())
        .withId(filesetEntity.id())
        .withComment(newComment)
        .withFilesetType(newType)
        .withStorageLocations(filesetEntity.storageLocations())
        .withProperties(props)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(filesetEntity.auditInfo().creator())
                .withCreateTime(filesetEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  /**
   * Check whether the placeholder in the location is valid. Throw an exception if the location
   * contains a placeholder with an empty value.
   *
   * @param location the location to check.
   */
  private void checkPlaceholderValue(String location) {
    if (StringUtils.isBlank(location)) {
      return;
    }

    Matcher matcher = LOCATION_PLACEHOLDER_PATTERN.matcher(location);
    while (matcher.find()) {
      String placeholder = matcher.group(1);
      if (placeholder.isEmpty()) {
        throw new IllegalArgumentException(
            "Placeholder in location should not be empty, location: " + location);
      }
    }
  }

  /**
   * Check whether the location contains a placeholder. The placeholder is in the format of
   * {{name}}.
   *
   * @param location the location to check.
   * @return true if the location contains a placeholder, false otherwise.
   */
  private boolean containsPlaceholder(String location) {
    return StringUtils.isNotBlank(location)
        && LOCATION_PLACEHOLDER_PATTERN.matcher(location).find();
  }

  private Map<String, Path> calculateFilesetPaths(
      String schemaName,
      String filesetName,
      Map<String, String> storageLocations,
      Map<String, Path> schemaPaths,
      Map<String, String> properties) {
    ImmutableMap.Builder<String, Path> filesetPaths = ImmutableMap.builder();
    Set<String> locationNames = new HashSet<>(schemaPaths.keySet());
    locationNames.addAll(storageLocations.keySet());

    locationNames.forEach(
        locationName -> {
          String storageLocation = storageLocations.get(locationName);
          Path schemaPath = schemaPaths.get(locationName);
          filesetPaths.put(
              locationName,
              calculateFilesetPath(
                  schemaName, filesetName, storageLocation, schemaPath, properties));
        });
    return filesetPaths.build();
  }

  private Path calculateFilesetPath(
      String schemaName,
      String filesetName,
      String storageLocation,
      Path schemaPath,
      Map<String, String> properties) {
    // The specified storageLocation will take precedence
    // case 1: storageLocation is not empty and does not contain placeholder
    if (StringUtils.isNotBlank(storageLocation) && !containsPlaceholder(storageLocation)) {
      return new Path(storageLocation);
    }

    Map<String, String> placeholderMapping = new HashMap<>();
    properties.forEach(
        (k, v) -> {
          if (k.startsWith(PROPERTY_LOCATION_PLACEHOLDER_PREFIX)) {
            placeholderMapping.put(k.substring(PROPERTY_LOCATION_PLACEHOLDER_PREFIX.length()), v);
          }
        });
    placeholderMapping.put(
        PROPERTY_CATALOG_PLACEHOLDER.substring(PROPERTY_LOCATION_PLACEHOLDER_PREFIX.length()),
        catalogInfo.name());
    placeholderMapping.put(
        PROPERTY_SCHEMA_PLACEHOLDER.substring(PROPERTY_LOCATION_PLACEHOLDER_PREFIX.length()),
        schemaName);
    placeholderMapping.put(
        PROPERTY_FILESET_PLACEHOLDER.substring(PROPERTY_LOCATION_PLACEHOLDER_PREFIX.length()),
        filesetName);

    // case 2: storageLocation is not empty and contains placeholder
    if (StringUtils.isNotBlank(storageLocation)) {
      return new Path(replacePlaceholders(storageLocation, placeholderMapping));
    }

    // case 3: storageLocation is empty and schemaPath does not contain placeholder
    if (!containsPlaceholder(schemaPath.toString())) {
      return new Path(schemaPath, filesetName);
    }

    // case 4: storageLocation is empty and schemaPath contains placeholder
    return new Path(replacePlaceholders(schemaPath.toString(), placeholderMapping));
  }

  private String replacePlaceholders(String location, Map<String, String> placeholderMapping) {
    Matcher matcher = LOCATION_PLACEHOLDER_PATTERN.matcher(location);
    StringBuilder result = new StringBuilder();
    int currentPosition = 0;
    while (matcher.find()) {
      // Append the text before the match
      result.append(location, currentPosition, matcher.start());

      // Append the replacement
      String key = matcher.group(1);
      String replacement = placeholderMapping.get(key);
      if (replacement == null) {
        throw new IllegalArgumentException("No value found for placeholder: " + key);
      }
      result.append(replacement);

      currentPosition = matcher.end();
    }

    // Append the rest of the text
    if (currentPosition < location.length()) {
      result.append(location, currentPosition, location.length());
    }
    return result.toString();
  }

  @VisibleForTesting
  Path formalizePath(Path path, Map<String, String> configuration) throws IOException {
    FileSystem defaultFs = getFileSystem(path, configuration);
    return path.makeQualified(defaultFs.getUri(), defaultFs.getWorkingDirectory());
  }

  private boolean hasCallerContext() {
    return CallerContext.CallerContextHolder.get() != null
        && CallerContext.CallerContextHolder.get().context() != null
        && !CallerContext.CallerContextHolder.get().context().isEmpty();
  }

  @VisibleForTesting
  FileSystem getFileSystemWithCache(Path path, Map<String, String> conf) {
    String scheme = path.toUri().getScheme();
    FileSystemProvider provider = fileSystemProvidersMap.get(scheme);
    String authority =
        provider != null ? provider.getFullAuthority(path, conf) : path.toUri().getAuthority();
    // Resolving the settings and creating the file system share one connection timeout.
    long deadlineNanos =
        System.nanoTime() + TimeUnit.SECONDS.toNanos(connectionTimeoutSeconds(conf));
    // Only an HDFS file system logs in with the settings of the configuration, see
    // createFileSystem.
    String fsScheme = scheme != null ? scheme : defaultFileSystemProvider.scheme();
    if (!SCHEME_HDFS.equals(fsScheme)) {
      return cachedFileSystem(
          path,
          conf,
          new FileSystemCacheKey(scheme, authority, Collections.emptyMap()),
          null,
          deadlineNanos);
    }

    FileSystem fs = getHdfsFileSystemWithCache(path, conf, scheme, authority, deadlineNanos);
    if (fs == null) {
      // The config.resources files changed after their login settings were cached.
      sharedHdfsLoginSettingsCache.synchronous().invalidate(ConfKey.withoutIdentifier(conf));
      hdfsLoginSettingsCache.synchronous().invalidate(ConfKey.of(conf));
      fs = getHdfsFileSystemWithCache(path, conf, scheme, authority, deadlineNanos);
    }
    if (fs == null) {
      throw new GravitinoRuntimeException(
          "The login settings in config.resources changed while getting FileSystem for fileset:"
              + " path: %s",
          path);
    }
    return fs;
  }

  // Returns null if the config.resources files no longer resolve to the cached login settings: a
  // file system is cached only under the settings of the configuration it is created with.
  @Nullable
  private FileSystem getHdfsFileSystemWithCache(
      Path path,
      Map<String, String> conf,
      @Nullable String scheme,
      @Nullable String authority,
      long deadlineNanos) {
    Map<String, String> settings = hdfsLoginSettings(path, conf, deadlineNanos);
    return cachedFileSystem(
        path, conf, new FileSystemCacheKey(scheme, authority, settings), settings, deadlineNanos);
  }

  // A miss creates the file system on the pool, under the deadline of this caller. Callers that
  // find it being created wait for it until their own deadlines.
  @Nullable
  private FileSystem cachedFileSystem(
      Path path,
      Map<String, String> conf,
      FileSystemCacheKey key,
      @Nullable Map<String, String> expectedSettings,
      long deadlineNanos) {
    // A null result caches nothing; the cache statistics count it as a failed load.
    try {
      return TimedTask.awaitEntry(
          fileSystemCache,
          key,
          (cacheKey, executor) -> {
            fileSystemProvider(path);
            // The creation may outlive this caller, whose configuration is not copied on a hit.
            Map<String, String> keptConf = new HashMap<>(conf);
            // Create the file system for the user it is cached under.
            return TimedTask.start(
                fileSystemExecutor,
                () -> getFileSystem(path, keptConf, cacheKey.currentUser, expectedSettings),
                deadlineNanos);
          },
          deadlineNanos);
    } catch (TimeoutException
        | InterruptedException
        | RejectedExecutionException
        | ExecutionException e) {
      throw new GravitinoRuntimeException(
          fileSystemFailure(path, conf, e), "Failed to get FileSystem for fileset: path: %s", path);
    }
  }

  private Map<String, String> hdfsLoginSettings(
      Path path, Map<String, String> conf, long deadlineNanos) {
    // Settings resolved for the shared entry that expand variables, returned to this caller only:
    // they may be older than what another caller has cached for this configuration since.
    AtomicReference<Map<String, String>> resolved = new AtomicReference<>();
    Optional<Map<String, String>> shared =
        awaitLoginSettings(
            path,
            conf,
            sharedHdfsLoginSettingsCache,
            ConfKey.withoutIdentifier(conf),
            keptConf -> {
              HdfsLogin login = resolveHdfsLogin(keptConf);
              if (login.expandsVariables) {
                resolved.set(login.settings);
                return Optional.empty();
              }
              return Optional.of(login.settings);
            },
            deadlineNanos);
    if (shared.isPresent()) {
      return shared.get();
    }
    if (resolved.get() != null) {
      return resolved.get();
    }
    return awaitLoginSettings(
        path,
        conf,
        hdfsLoginSettingsCache,
        ConfKey.of(conf),
        keptConf -> resolveHdfsLogin(keptConf).settings,
        deadlineNanos);
  }

  // Reading the config.resources files may hang, so the login is resolved on a pool under the
  // timeout.
  private <T> T awaitLoginSettings(
      Path path,
      Map<String, String> conf,
      AsyncCache<ConfKey, T> cache,
      ConfKey key,
      Function<Map<String, String>, T> resolve,
      long deadlineNanos) {
    try {
      // The load resolves a copy of the configuration, never the caller's map.
      return TimedTask.awaitEntry(
          cache,
          key,
          key::copy,
          (keptKey, executor) ->
              TimedTask.start(
                  loginSettingsExecutor, () -> resolve.apply(keptKey.conf), deadlineNanos),
          deadlineNanos);
    } catch (RejectedExecutionException e) {
      // The pool is full, or closed with the catalog.
      throw new GravitinoRuntimeException(
          e, "Failed to start resolving the login settings for path: %s", path);
    } catch (TimeoutException e) {
      int timeoutSeconds = connectionTimeoutSeconds(conf);
      LOG.warn(
          "Timeout when resolving the login settings for path: {} within {} seconds",
          path,
          timeoutSeconds,
          e);
      throw new GravitinoRuntimeException(
          e,
          "Failed to resolve the login settings for path: %s within %s seconds, please check the "
              + "config.resources files or increase the file system connection timeout time by "
              + "setting catalog property: %s",
          path,
          timeoutSeconds,
          FilesetCatalogPropertiesMetadata.FILESYSTEM_CONNECTION_TIMEOUT_SECONDS);
    } catch (InterruptedException e) {
      throw ExceptionMessages.wrap("Interrupted when getting FileSystem for path: " + path, e);
    } catch (ExecutionException e) {
      // Other filesets may share the settings, so each caller reports its own path.
      throw new GravitinoRuntimeException(
          e.getCause(), "Failed to get FileSystem for fileset: path: %s", path);
    }
  }

  // Builds the Hadoop configuration from the config.resources files, and resolves its login
  // settings. Reading the files and expanding Hadoop variables can fail; the callers report the
  // path.
  @VisibleForTesting
  HdfsLogin resolveHdfsLogin(Map<String, String> conf) {
    Configuration hadoopConf = HDFSFileSystemProxy.hadoopConfiguration(conf);
    Map<String, String> settings =
        new HashMap<>(HDFSFileSystemProxy.loginSettings(conf, hadoopConf));
    // Other settings in these files, such as HA name services, decide which cluster is reached.
    settings.put(CONFIG_RESOURCES, conf.getOrDefault(CONFIG_RESOURCES, ""));
    return new HdfsLogin(
        hadoopConf,
        Collections.unmodifiableMap(settings),
        HDFSFileSystemProxy.loginSettingsExpandVariables(hadoopConf));
  }

  private String buildGVFSFilePath(
      String catalogName, String schemaName, String filesetName, String subPath) {
    String prefix = String.join(SLASH, "/fileset", catalogName, schemaName, filesetName);
    return StringUtils.isBlank(subPath)
        ? prefix
        : prefix + ensureLeadingSlash(removeTrailingSlash(subPath));
  }

  FileSystem getFileSystem(Path path, Map<String, String> config) throws IOException {
    // Read the user here: a pool thread keeps the Subject of the request that started it.
    String user = PrincipalUtils.getCurrentUserName();
    fileSystemProvider(path);
    try {
      return executeFileSystemTask(
          fileSystemExecutor,
          () -> getFileSystem(path, config, user, null),
          TimeUnit.SECONDS.toNanos(connectionTimeoutSeconds(config)));
    } catch (TimeoutException
        | InterruptedException
        | RejectedExecutionException
        | ExecutionException e) {
      throw fileSystemFailure(path, config, e);
    }
  }

  // Creates the file system on this thread. With expected login settings, returns null if the
  // configuration no longer resolves to them.
  @Nullable
  FileSystem getFileSystem(
      Path path,
      Map<String, String> config,
      String user,
      @Nullable Map<String, String> expectedSettings)
      throws IOException {
    FileSystemProvider provider = fileSystemProvider(path);
    String scheme =
        path.toUri().getScheme() != null
            ? path.toUri().getScheme()
            : defaultFileSystemProvider.scheme();
    Configuration loginConf = null;
    if (expectedSettings != null) {
      // Resolved here, so that reading the config.resources files is under the timeout too. The
      // file system logs in with the configuration that was checked.
      HdfsLogin login = resolveHdfsLogin(config);
      if (!login.settings.equals(expectedSettings)) {
        return null;
      }
      loginConf = login.configuration;
    }
    return createFileSystem(path, config, scheme, provider, user, loginConf);
  }

  private FileSystemProvider fileSystemProvider(Path path) {
    if (path == null) {
      throw new IllegalArgumentException("Path should not be null");
    }

    String scheme =
        path.toUri().getScheme() != null
            ? path.toUri().getScheme()
            : defaultFileSystemProvider.scheme();

    FileSystemProvider provider = fileSystemProvidersMap.get(scheme);
    if (provider == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported scheme: %s, path: %s, all supported schemes: %s and providers: %s",
              scheme, path, fileSystemProvidersMap.keySet(), fileSystemProvidersMap.values()));
    }
    return provider;
  }

  // Returns the failure to report for a file system that was not created in time, or throws it.
  private IOException fileSystemFailure(Path path, Map<String, String> config, Exception e) {
    String scheme =
        path.toUri().getScheme() != null
            ? path.toUri().getScheme()
            : defaultFileSystemProvider.scheme();
    FileSystemProvider provider = fileSystemProvidersMap.get(scheme);
    int timeoutSeconds = connectionTimeoutSeconds(config);
    if (e instanceof TimeoutException) {
      LOG.warn(
          "Timeout when getting FileSystem for path: {}, scheme: {}, provider: {} within {} seconds",
          path,
          scheme,
          provider,
          timeoutSeconds,
          e);

      return new IOException(
          String.format(
              "Failed to get FileSystem for path: %s, scheme: %s, provider: %s within %s "
                  + "seconds, please check the configuration or increase the "
                  + "file system connection timeout time by setting catalog property: %s",
              path,
              scheme,
              provider,
              timeoutSeconds,
              FilesetCatalogPropertiesMetadata.FILESYSTEM_CONNECTION_TIMEOUT_SECONDS),
          e);
    }
    if (e instanceof InterruptedException) {
      LOG.warn(
          "Interrupted when getting FileSystem for path: {}, possibly the server is"
              + " shutting down or catalog is been dropped",
          path);
      throw ExceptionMessages.wrap("Interrupted when getting FileSystem for path: " + path, e);
    }
    if (e instanceof RejectedExecutionException) {
      // The pool is full, or closed with the catalog.
      return new IOException("Failed to start creating FileSystem for path: " + path, e);
    }
    Throwable cause = e.getCause();
    if (cause instanceof IOException) {
      return (IOException) cause;
    }
    return new IOException("Failed to create FileSystem", cause);
  }

  private int connectionTimeoutSeconds(Map<String, String> conf) {
    return (int)
        propertiesMetadata
            .catalogPropertiesMetadata()
            .getOrDefault(
                conf, FilesetCatalogPropertiesMetadata.FILESYSTEM_CONNECTION_TIMEOUT_SECONDS);
  }

  @Nullable
  private <T> T executeFileSystemTask(ExecutorService executor, Callable<T> task, long timeoutNanos)
      throws InterruptedException, ExecutionException, TimeoutException {
    long deadlineNanos = System.nanoTime() + timeoutNanos;
    TimedTask<T> timed = TimedTask.start(executor, task, deadlineNanos);
    try {
      return TimedTask.await(timed, deadlineNanos);
    } catch (TimeoutException | InterruptedException e) {
      // Nobody else waits for the result. A task that has just returned it hands it over here.
      if (!timed.cancel(false) && !timed.isCompletedExceptionally()) {
        closeQuietly(timed.join());
      }
      throw e;
    }
  }

  private static void closeQuietly(@Nullable Object result) {
    if (result instanceof Closeable) {
      // May run on a thread of its own, where nobody sees what it throws.
      try {
        ((Closeable) result).close();
      } catch (Throwable t) {
        LOG.warn("Failed to close a file system that its caller stopped waiting for", t);
      }
    }
  }

  @VisibleForTesting
  Path resolveProbeLocation(String location, NameIdentifier catalogIdent) {
    String resolved = location.replace(CATALOG_PLACEHOLDER, catalogIdent.name());
    Matcher matcher = LOCATION_PLACEHOLDER_PATTERN.matcher(resolved);
    List<String> unresolvedPlaceholders = new ArrayList<>();
    while (matcher.find()) {
      unresolvedPlaceholders.add(matcher.group());
    }
    if (!unresolvedPlaceholders.isEmpty()) {
      String placeholderLabel =
          unresolvedPlaceholders.size() == 1
              ? "unresolved placeholder "
              : "unresolved placeholders ";
      throw new ProbeUnsupportedException(
          PROBE_UNSUPPORTED_DETAIL_PREFIX
              + " "
              + placeholderLabel
              + String.join(", ", unresolvedPlaceholders)
              + " in "
              + resolved
              + "; no filesystem path was tested");
    }

    if (StringUtils.isBlank(resolved)) {
      throw new InvalidProbeLocationException(
          "Fileset catalog location has no valid concrete target that can be tested");
    }

    try {
      return new Path(resolved);
    } catch (IllegalArgumentException e) {
      throw new InvalidProbeLocationException(
          "Fileset catalog location has no valid concrete target that can be tested", e);
    }
  }

  private void probeLocation(Path path) throws Exception {
    Map<String, String> probeConf = new HashMap<>(conf);
    probeConf.putAll(
        FilesetUtil.getUserDefinedFileSystemConfigs(
            path.toUri(),
            probeConf,
            FilesetCatalogPropertiesMetadata.FS_GRAVITINO_PATH_CONFIG_PREFIX));

    String scheme =
        path.toUri().getScheme() != null
            ? path.toUri().getScheme()
            : defaultFileSystemProvider.scheme();
    FileSystemProvider fileSystemProvider = fileSystemProvidersMap.get(scheme);
    if (fileSystemProvider == null) {
      throw new UnsupportedOperationException(
          String.format("Fileset connection testing does not support scheme %s", scheme));
    }

    // Read the user here: a pool thread keeps the Subject of the request that started it.
    String user = PrincipalUtils.getCurrentUserName();
    String credentialHandle = null;
    try {
      credentialHandle = addVendedCredential(path, scheme, fileSystemProvider, probeConf);
      executeFileSystemTask(
          fileSystemExecutor,
          () -> {
            try (FileSystem fileSystem =
                createProbeFileSystem(path, probeConf, scheme, fileSystemProvider, user)) {
              Path qualifiedPath =
                  path.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
              try {
                RemoteIterator<FileStatus> iterator = fileSystem.listStatusIterator(qualifiedPath);
                if (iterator.hasNext()) {
                  FileStatus first = iterator.next();
                  if (first.isFile() && first.getPath().equals(qualifiedPath)) {
                    throw new ProbeLocationNotDirectoryException(
                        "Fileset catalog location must be a directory");
                  }
                }
              } catch (FileNotFoundException ignored) {
                // A catalog location may be created later by schema or fileset operations.
              }
              return null;
            }
          },
          TimeUnit.SECONDS.toNanos(connectionTimeoutSeconds(probeConf)));
    } catch (TimeoutException e) {
      throw new IOException("Filesystem connection probe timed out", e);
    } catch (InterruptedException e) {
      throw new IOException("Filesystem connection probe was interrupted", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw new IOException("Filesystem connection probe failed", cause);
    } finally {
      InMemoryFileSystemCredentialsProvider.unregister(credentialHandle);
    }
  }

  @Nullable
  private String addVendedCredential(
      Path path,
      String scheme,
      FileSystemProvider fileSystemProvider,
      Map<String, String> probeConf) {
    Set<String> credentialTypes = CredentialUtils.getCredentialProvidersByOrder(() -> conf);
    if (credentialTypes.isEmpty()) {
      return null;
    }

    CatalogCredentialManager manager = catalogCredentialManager();
    List<String> matchingTypes =
        credentialTypes.stream()
            .filter(
                type ->
                    manager
                        .getCredentialProvider(type)
                        .map(provider -> provider.supportsScheme(scheme))
                        .orElse(false))
            .collect(Collectors.toList());
    if (matchingTypes.isEmpty()) {
      return null;
    }
    if (!(fileSystemProvider instanceof SupportsCredentialVending)) {
      throw new UnsupportedOperationException(
          String.format("Filesystem provider for scheme %s cannot use vended credentials", scheme));
    }

    PathBasedCredentialContext context =
        new PathBasedCredentialContext(
            PrincipalUtils.getCurrentUserName(),
            Collections.emptySet(),
            Collections.singleton(path.toString()));
    Credential[] credentials =
        matchingTypes.stream()
            .map(type -> manager.getCredential(type, context))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toArray(Credential[]::new);
    if (credentials.length == 0) {
      throw new ConnectionFailedException(
          "Credential providers returned no credential for Fileset connection test");
    }
    String handle = InMemoryFileSystemCredentialsProvider.register(credentials);
    try {
      probeConf.put(
          GravitinoFileSystemCredentialsProvider.GVFS_CREDENTIAL_PROVIDER,
          InMemoryFileSystemCredentialsProvider.class.getCanonicalName());
      probeConf.put(InMemoryFileSystemCredentialsProvider.CREDENTIAL_HANDLE, handle);
      probeConf.putAll(
          ((SupportsCredentialVending) fileSystemProvider)
              .getFileSystemCredentialConf(credentials));
      return handle;
    } catch (RuntimeException e) {
      InMemoryFileSystemCredentialsProvider.unregister(handle);
      throw e;
    }
  }

  @VisibleForTesting
  CatalogCredentialManager catalogCredentialManager() {
    Preconditions.checkState(
        credentialManagerSupplier != null,
        "Catalog credential manager is not available for Fileset connection testing");
    return Objects.requireNonNull(
        credentialManagerSupplier.get(), "Catalog credential manager must not be null");
  }

  @VisibleForTesting
  FileSystem createFileSystem(
      Path path,
      Map<String, String> config,
      String scheme,
      FileSystemProvider provider,
      String user,
      @Nullable Configuration hadoopConf)
      throws IOException {
    if (scheme.equals(SCHEME_HDFS)) {
      return new ImpersonationHDFSFileSystemProxy(path, config, hadoopConf, () -> user).getProxy();
    }
    return provider.getFileSystem(path, config);
  }

  private FileSystem createProbeFileSystem(
      Path path,
      Map<String, String> config,
      String scheme,
      FileSystemProvider provider,
      String user)
      throws IOException {
    try {
      return createFileSystem(path, config, scheme, provider, user, null);
    } catch (Exception e) {
      throw new ProbeInitializationException(e);
    }
  }

  private String displayLocationName(String locationName) {
    return LOCATION_NAME_UNKNOWN.equals(locationName) ? "location" : "location-" + locationName;
  }

  private String failureDescription(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof ProbeUnsupportedException) {
        return current.getMessage();
      }
      current = current.getCause();
    }
    return failureCategory(throwable);
  }

  private String failureCategory(Throwable throwable) {
    boolean initializationFailure = throwable instanceof ProbeInitializationException;
    Throwable current = throwable;
    while (current != null) {
      String simpleName = current.getClass().getSimpleName().toLowerCase(Locale.ROOT);
      String message =
          Optional.ofNullable(current.getMessage()).orElse("").toLowerCase(Locale.ROOT);
      if (current instanceof ProbeLocationNotDirectoryException) {
        return "location is not a directory";
      }
      if (current instanceof InvalidProbeLocationException) {
        return "invalid location configuration";
      }
      if (current instanceof TimeoutException || message.contains("timed out")) {
        return "timeout";
      }
      if (simpleName.contains("accessdenied")
          || simpleName.contains("forbidden")
          || message.contains("permission denied")
          || message.contains("access denied")) {
        return "access denied";
      }
      if (simpleName.contains("filenotfound") || simpleName.contains("notfound")) {
        return initializationFailure
            ? "filesystem initialization failed; check credentials and configuration"
            : "location not found";
      }
      if (current instanceof ConnectionFailedException) {
        return "credential generation failed";
      }
      if (current instanceof UnsupportedOperationException) {
        return PROBE_UNSUPPORTED;
      }
      current = current.getCause();
    }
    return "probe failed";
  }

  private String getFileLocation(Fileset fileset, String subPath, String locationName) {
    Preconditions.checkArgument(subPath != null, "subPath must not be null");
    String processedSubPath = subPath.trim();
    // Validate before constructing a Hadoop Path, which normalizes away parent directory segments.
    Preconditions.checkArgument(
        subPath.indexOf('\0') < 0, "subPath must not contain null characters");
    for (String segment : processedSubPath.split(SLASH)) {
      Preconditions.checkArgument(
          !"..".equals(segment), "subPath must not contain parent directory segments");
    }
    if (!processedSubPath.isEmpty() && !processedSubPath.startsWith(SLASH)) {
      processedSubPath = SLASH + processedSubPath;
    }

    String targetLocationName;
    if (locationName == null) {
      targetLocationName =
          fileset.storageLocations().size() == 1
              // to be compatible with the old version, the fileset in old version only has one
              // location and does not have the default-location-name property
              ? fileset.storageLocations().keySet().iterator().next()
              : fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME);
    } else {
      targetLocationName = locationName;
    }
    if (!fileset.storageLocations().containsKey(targetLocationName)) {
      throw new NoSuchLocationNameException(
          "Location name %s does not exist in fileset %s", targetLocationName, fileset.name());
    }

    String storageLocation = fileset.storageLocations().get(targetLocationName);
    String fileLocation =
        processedSubPath.isEmpty()
            ? storageLocation
            : removeTrailingSlash(storageLocation) + processedSubPath;

    // Compare normalized Path ancestors, including scheme and authority, rather than string
    // prefixes. Keep the original spelling in the response for existing clients.
    Path storagePath = normalizeLocationPath(storageLocation);
    Path filePath = normalizeLocationPath(fileLocation);
    Preconditions.checkArgument(
        isWithinStorageLocation(storagePath, filePath),
        "subPath must stay within the fileset storage location");

    // do checks for some data operations.
    if (hasCallerContext()) {
      Map<String, String> contextMap = CallerContext.CallerContextHolder.get().context();
      String operation =
          contextMap.getOrDefault(
              FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION,
              FilesetDataOperation.UNKNOWN.name());
      if (!FilesetDataOperation.checkValid(operation)) {
        LOG.warn(
            "The data operation: {} is not valid, we cannot do some checks for this operation.",
            operation);
      } else {
        FilesetDataOperation dataOperation = FilesetDataOperation.valueOf(operation);
        switch (dataOperation) {
          case RENAME:
            // Include aliases such as /./ so the fileset root cannot be renamed independently of
            // its metadata.
            if (filePath.equals(storagePath)) {
              throw new GravitinoRuntimeException(
                  "subPath cannot be blank when need to rename a file or a directory.");
            }
            break;
          default:
            break;
        }
      }
    }

    return fileLocation;
  }

  private Path normalizeLocationPath(String location) {
    // Represent authority-only locations (for example s3a://bucket) as filesystem roots. Hadoop
    // removes trailing slashes before normalizing dot segments, so parse again to remove a trailing
    // slash introduced by normalization of an alias such as /directory/./.
    Path path = new Path(ensureTrailingSlash(location));
    return new Path(path.toString());
  }

  private static boolean isWithinStorageLocation(Path storagePath, Path filePath) {
    for (Path current = filePath; current != null; current = current.getParent()) {
      if (storagePath.equals(current)) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  Map<String, String> mergeUpLevelConfigurations(
      NameIdentifier ident, Map<String, String> entityProperties, Path path) {
    // Merge configurations from catalog, schema, and entity (fileset) levels, and also include
    // user-defined configurations for the specified location.
    //
    // Catalog conf is already plaintext (CatalogManager.createBaseCatalog). Schema/fileset entity
    // properties keep secret URNs in storage; resolve them here so FileSystem ops see plaintext,
    // matching the catalog "storage=URN / connector=plaintext" split.
    Map<String, String> mergedProperties = new HashMap<>(conf);
    if (ident.namespace().levels().length == 2) {
      // schema level
      mergedProperties.putAll(secretManager.toPlaintextProperties(entityProperties));
      // Add user-defined configs for location if provided
      mergedProperties.putAll(
          FilesetUtil.getUserDefinedFileSystemConfigs(
              path.toUri(),
              mergedProperties,
              FilesetCatalogPropertiesMetadata.FS_GRAVITINO_PATH_CONFIG_PREFIX));
      return mergedProperties;
    }

    // fileset level
    NameIdentifierUtil.checkFileset(ident);
    NameIdentifier schemaIdent = NameIdentifierUtil.getSchemaIdentifier(ident);
    Schema schema = loadSchema(schemaIdent);
    mergedProperties.putAll(secretManager.toPlaintextProperties(schema.properties()));
    mergedProperties.putAll(secretManager.toPlaintextProperties(entityProperties));
    // Add user-defined configs for location if provided
    mergedProperties.putAll(
        FilesetUtil.getUserDefinedFileSystemConfigs(
            path.toUri(),
            mergedProperties,
            FilesetCatalogPropertiesMetadata.FS_GRAVITINO_PATH_CONFIG_PREFIX));
    return mergedProperties;
  }

  @Override
  public List<PathContext> getPathContext(NameIdentifier filesetIdentifier) {
    Fileset fileset = loadFileset(filesetIdentifier);
    String path = getTargetLocation(fileset);

    Set<String> providers =
        CredentialUtils.getCredentialProvidersByOrder(
            fileset::properties,
            () -> {
              Namespace namespace = filesetIdentifier.namespace();
              NameIdentifier schemaIdentifier =
                  NameIdentifierUtil.ofSchema(
                      namespace.level(0), namespace.level(1), namespace.level(2));
              return loadSchema(schemaIdentifier).properties();
            },
            () -> conf);
    return providers.stream()
        .map(provider -> new PathContext(path, provider))
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  protected String getTargetLocation(Fileset fileset) {
    CallerContext callerContext = CallerContext.CallerContextHolder.get();
    String targetLocationName;
    String targetLocation;
    if (callerContext != null
        && callerContext
            .context()
            .containsKey(CredentialConstants.HTTP_HEADER_CURRENT_LOCATION_NAME)) {
      // case 1: target location name is passed in the header
      targetLocationName =
          callerContext.context().get(CredentialConstants.HTTP_HEADER_CURRENT_LOCATION_NAME);
      Preconditions.checkArgument(
          fileset.storageLocations().containsKey(targetLocationName),
          "The location name %s is not in the fileset %s, expected location names are %s",
          targetLocationName,
          fileset.name(),
          fileset.storageLocations().keySet());
      targetLocation = fileset.storageLocations().get(targetLocationName);

    } else if (fileset.storageLocations().size() == 1) {
      // case 2: target location name is not passed in the header, but there is only one location.
      // note: mainly used for backward compatibility since the old code does not pass the header
      // and only supports one location
      targetLocation = fileset.storageLocations().values().iterator().next();
      targetLocationName = fileset.storageLocations().keySet().iterator().next();

    } else {
      // case 3: target location name is not passed in the header, and there are multiple locations.
      // use the default location name
      targetLocationName = fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME);
      // this should never happen, but just in case
      Preconditions.checkArgument(
          StringUtils.isNotBlank(targetLocationName),
          "The default location name of the fileset %s should not be empty.",
          fileset.name());
      targetLocation = fileset.storageLocations().get(targetLocationName);
    }

    Preconditions.checkArgument(
        StringUtils.isNotBlank(targetLocation),
        "The location with the location name %s of the fileset %s should not be empty.",
        targetLocationName,
        fileset.name());
    return targetLocation;
  }

  private static final class ProbeInitializationException extends IOException {

    private ProbeInitializationException(Throwable cause) {
      super("Failed to initialize filesystem for connection test", cause);
    }
  }

  private static final class InvalidProbeLocationException extends IllegalArgumentException {

    private InvalidProbeLocationException(String message) {
      super(message);
    }

    private InvalidProbeLocationException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static final class ProbeLocationNotDirectoryException extends IllegalArgumentException {

    private ProbeLocationNotDirectoryException(String message) {
      super(message);
    }
  }

  private static final class ProbeUnsupportedException extends UnsupportedOperationException {

    private ProbeUnsupportedException(String message) {
      super(message);
    }
  }
}
