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
package org.apache.gravitino.paimon.service;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.gravitino.paimon.PaimonConfig;
import org.apache.gravitino.paimon.common.PaimonCatalogOptions;
import org.apache.gravitino.paimon.common.PaimonCatalogWrapper;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProvider;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProvider.CatalogConfig;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProviderFactory;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.ServiceUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves catalog configurations and shares backend catalogs across requests. Active requests
 * retain their backend until they finish, including during service shutdown.
 */
public class PaimonCatalogWrapperManager implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonCatalogWrapperManager.class);

  /** Name of the catalog served when the client omits the prefix or warehouse. */
  public static final String DEFAULT_CATALOG_NAME = "default_catalog";

  private final PaimonConfigProvider configProvider;
  private final Clock clock;
  private final long cacheTtlMillis;
  @Nullable private final ScheduledExecutorService cleaner;
  private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();
  private boolean closed;
  private final ConcurrentMap<CatalogKey, ManagedCatalog> catalogs = new ConcurrentHashMap<>();

  /**
   * Creates the manager with the provider selected by the service configuration.
   *
   * @param config the service configuration
   */
  public PaimonCatalogWrapperManager(PaimonConfig config) {
    this(config, false);
  }

  /**
   * Creates a manager for the deployment mode.
   *
   * @param config service options
   * @param auxiliary whether the service runs inside Gravitino
   */
  public PaimonCatalogWrapperManager(PaimonConfig config, boolean auxiliary) {
    this(
        PaimonConfigProviderFactory.create(config.getAllConfig(), auxiliary),
        Clock.systemUTC(),
        Long.parseLong(config.getAllConfig().getOrDefault("catalog-cache-ttl-ms", "30000")));
  }

  PaimonCatalogWrapperManager(PaimonConfigProvider configProvider) {
    this(configProvider, Clock.systemUTC(), 30000);
  }

  PaimonCatalogWrapperManager(
      PaimonConfigProvider configProvider, Clock clock, long cacheTtlMillis) {
    if (cacheTtlMillis <= 0) {
      throw new IllegalArgumentException("catalog-cache-ttl-ms must be positive");
    }
    this.configProvider = configProvider;
    this.clock = clock;
    this.cacheTtlMillis = cacheTtlMillis;
    configProvider.addInvalidationListener(this::invalidate);
    if (configProvider.getMetalakeName().isPresent()) {
      cleaner =
          Executors.newSingleThreadScheduledExecutor(
              task -> {
                Thread thread = new Thread(task, "paimon-rest-catalog-cleaner");
                thread.setDaemon(true);
                return thread;
              });
      long interval = Math.min(cacheTtlMillis, TimeUnit.MINUTES.toMillis(1));
      cleaner.scheduleWithFixedDelay(
          this::cleanUpExpiredCatalogs, interval, interval, TimeUnit.MILLISECONDS);
    } else {
      cleaner = null;
    }
  }

  /**
   * Acquires the exact requested catalog. The caller must release it after using the backend.
   *
   * @param catalogName the catalog name from the request prefix or warehouse
   * @return the catalog lease
   */
  public CatalogLease acquire(String catalogName) {
    lifecycle.readLock().lock();
    try {
      if (closed) {
        throw new IllegalStateException("Paimon catalog manager is closed");
      }
      CatalogKey key =
          new CatalogKey(
              catalogName,
              configProvider.getMetalakeName().isPresent()
                  ? PrincipalUtils.getCurrentUserName()
                  : "");
      ManagedCatalog catalog =
          catalogs.compute(
              key,
              (ignored, current) -> {
                ManagedCatalog selected = current;
                if (current == null || clock.millis() >= current.refreshAtMillis) {
                  selected = loadCatalog(catalogName);
                  if (current != null) {
                    current.retire();
                  }
                }
                selected.retain();
                return selected;
              });
      return new CatalogLease(catalog);
    } finally {
      lifecycle.readLock().unlock();
    }
  }

  /**
   * Invalidates every caller's cached configuration for a changed or deleted catalog.
   *
   * @param catalogName the registered catalog name
   */
  public void invalidate(String catalogName) {
    lifecycle.readLock().lock();
    try {
      catalogs.keySet().stream()
          .filter(key -> key.name.equals(catalogName))
          .forEach(
              key ->
                  catalogs.computeIfPresent(
                      key,
                      (ignored, current) -> {
                        current.retire();
                        return null;
                      }));
    } finally {
      lifecycle.readLock().unlock();
    }
  }

  /**
   * Retires the cached catalogs and closes the configuration provider.
   *
   * @throws IOException if the configuration provider fails to close
   */
  @Override
  public void close() throws IOException {
    lifecycle.writeLock().lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      if (cleaner != null) {
        cleaner.shutdownNow();
      }
      catalogs.values().forEach(ManagedCatalog::retire);
      catalogs.clear();
      configProvider.close();
    } finally {
      lifecycle.writeLock().unlock();
    }
  }

  /** Retires idle dynamic backends as their fixed lifetime or credential refresh time elapses. */
  void cleanUpExpiredCatalogs() {
    lifecycle.readLock().lock();
    try {
      if (closed) {
        return;
      }
      catalogs
          .keySet()
          .forEach(
              key ->
                  catalogs.computeIfPresent(
                      key,
                      (ignored, current) -> {
                        if (clock.millis() >= current.refreshAtMillis) {
                          current.retire();
                          return null;
                        }
                        return current;
                      }));
    } finally {
      lifecycle.readLock().unlock();
    }
  }

  private ManagedCatalog loadCatalog(String name) {
    CatalogConfig config =
        configProvider
            .getCatalogConfig(name)
            .orElseThrow(
                () -> new NoSuchResourceException("catalog", name, "Catalog does not exist."));
    Options options = PaimonCatalogOptions.fromProperties(config.properties());
    boolean dataTokens = options.getBoolean("data-token.enabled", false);
    if (dataTokens
        && config.expiresAtMillis() == 0
        && (options.containsKey("s3.session-token")
            || options.containsKey("fs.oss.securityToken"))) {
      throw new IllegalArgumentException(
          "Static data-token catalogs require long-lived storage keys. Temporary S3/OSS session "
              + "credentials require a provider that supplies their actual expiry and refreshes them.");
    }
    long now = clock.millis();
    long expiry = config.expiresAtMillis();
    long safetyWindow =
        TimeUnit.MINUTES.toMillis(1) + (dataTokens ? RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS : 0);
    if (expiry != 0 && expiry - now <= safetyWindow) {
      throw new ServiceUnavailableException(
          "Paimon catalog credentials have insufficient remaining lifetime. Configure "
              + "s3-token-expire-in-secs / oss-token-expire-in-secs well above 3600 seconds "
              + "and allow headroom for the Gravitino credential cache.");
    }
    long refreshAt =
        configProvider.getMetalakeName().isPresent() ? now + cacheTtlMillis : Long.MAX_VALUE;
    if (expiry != 0) {
      refreshAt = Math.min(refreshAt, expiry - safetyWindow);
    }
    return new ManagedCatalog(new PaimonCatalogWrapper(name, options), expiry, refreshAt);
  }

  /** Keeps the resolved backend available during a request. */
  public static final class CatalogLease implements Closeable {
    private final ManagedCatalog catalog;
    private final AtomicBoolean closed = new AtomicBoolean();

    private CatalogLease(ManagedCatalog catalog) {
      this.catalog = catalog;
    }

    /**
     * @return the resolved backend catalog
     */
    public PaimonCatalogWrapper wrapper() {
      return catalog.wrapper;
    }

    /**
     * @return the actual storage credential expiry, or zero for long-lived keys
     */
    public long expiresAtMillis() {
      return catalog.expiresAtMillis;
    }

    /** Releases the request's reference to the backend. */
    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        catalog.release();
      }
    }
  }

  private static final class CatalogKey {
    private final String name;
    private final String principal;

    private CatalogKey(String name, String principal) {
      this.name = name;
      this.principal = principal;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof CatalogKey)) {
        return false;
      }
      CatalogKey key = (CatalogKey) other;
      return name.equals(key.name) && principal.equals(key.principal);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, principal);
    }
  }

  private static final class ManagedCatalog {
    private final PaimonCatalogWrapper wrapper;
    private final long expiresAtMillis;
    private final long refreshAtMillis;
    private int references;
    private boolean backendClosed;
    private boolean retired;

    private ManagedCatalog(
        PaimonCatalogWrapper wrapper, long expiresAtMillis, long refreshAtMillis) {
      this.wrapper = wrapper;
      this.expiresAtMillis = expiresAtMillis;
      this.refreshAtMillis = refreshAtMillis;
    }

    private synchronized void retain() {
      references++;
    }

    private synchronized void release() {
      references--;
      closeIfRetired();
    }

    private synchronized void retire() {
      retired = true;
      closeIfRetired();
    }

    private void closeIfRetired() {
      if (retired && references == 0 && !backendClosed) {
        backendClosed = true;
        try {
          wrapper.close();
        } catch (Exception e) {
          LOG.warn("Failed to close Paimon catalog {}", wrapper.name(), e);
        }
      }
    }
  }
}
