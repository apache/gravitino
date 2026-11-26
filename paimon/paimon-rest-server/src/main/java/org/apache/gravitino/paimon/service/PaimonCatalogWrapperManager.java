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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.gravitino.paimon.PaimonConfig;
import org.apache.gravitino.paimon.common.PaimonCatalogOptions;
import org.apache.gravitino.paimon.common.PaimonCatalogWrapper;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProvider;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProvider.CatalogConfig;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProviderFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves static catalog configurations and shares backend catalogs across requests. Active
 * requests retain their backend until they finish, including during service shutdown.
 */
public class PaimonCatalogWrapperManager implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonCatalogWrapperManager.class);

  /** Name of the catalog served when the client omits the prefix or warehouse. */
  public static final String DEFAULT_CATALOG_NAME = "default_catalog";

  private final PaimonConfigProvider configProvider;
  private final ConcurrentMap<String, ManagedCatalog> catalogs = new ConcurrentHashMap<>();

  /**
   * Creates the manager with the provider selected by the service configuration.
   *
   * @param config the service configuration
   */
  public PaimonCatalogWrapperManager(PaimonConfig config) {
    this(PaimonConfigProviderFactory.create(config.getAllConfig()));
  }

  PaimonCatalogWrapperManager(PaimonConfigProvider configProvider) {
    this.configProvider = configProvider;
  }

  /**
   * Acquires the exact requested catalog. The caller must release it after using the backend.
   *
   * @param catalogName the catalog name from the request prefix or warehouse
   * @return the catalog lease
   */
  public CatalogLease acquire(String catalogName) {
    ManagedCatalog catalog =
        catalogs.compute(
            catalogName,
            (name, current) -> {
              ManagedCatalog selected = current == null ? loadCatalog(name) : current;
              selected.retain();
              return selected;
            });
    return new CatalogLease(catalog);
  }

  /**
   * Retires the cached catalogs and closes the configuration provider.
   *
   * @throws IOException if the configuration provider fails to close
   */
  @Override
  public void close() throws IOException {
    catalogs.values().forEach(ManagedCatalog::retire);
    catalogs.clear();
    configProvider.close();
  }

  private ManagedCatalog loadCatalog(String name) {
    CatalogConfig config =
        configProvider
            .getCatalogConfig(name)
            .orElseThrow(
                () -> new NoSuchResourceException("catalog", name, "Catalog does not exist."));
    Options options = PaimonCatalogOptions.fromProperties(config.properties());
    if (options.getBoolean("data-token.enabled", false)
        && (options.containsKey("s3.session-token")
            || options.containsKey("fs.oss.securityToken"))) {
      throw new IllegalArgumentException(
          "Static data-token catalogs require long-lived storage keys. Temporary S3/OSS session "
              + "credentials require a provider that supplies their actual expiry and refreshes them.");
    }
    return new ManagedCatalog(new PaimonCatalogWrapper(name, options));
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

    /** Releases the request's reference to the backend. */
    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        catalog.release();
      }
    }
  }

  private static final class ManagedCatalog {
    private final PaimonCatalogWrapper wrapper;
    private int references;
    private boolean retired;

    private ManagedCatalog(PaimonCatalogWrapper wrapper) {
      this.wrapper = wrapper;
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
      if (retired && references == 0) {
        try {
          wrapper.close();
        } catch (Exception e) {
          LOG.warn("Failed to close Paimon catalog {}", wrapper.name(), e);
        }
      }
    }
  }
}
