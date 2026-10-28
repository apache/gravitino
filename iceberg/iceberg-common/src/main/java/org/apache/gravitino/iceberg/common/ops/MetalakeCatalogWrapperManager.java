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
package org.apache.gravitino.iceberg.common.ops;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metalake-scoped {@link CatalogWrapperManager} that resolves lakehouse-iceberg configs from {@link
 * CatalogManager} and caches {@link IcebergCatalogWrapper} instances in-process.
 *
 * <p>Same config semantics as the Iceberg REST dynamic provider, but does <strong>not</strong>
 * borrow the embedded IRC manager. jdbc / hive backends are in-process; {@code
 * catalog-backend=rest} still uses Iceberg's HTTP {@code RESTCatalog}.
 */
public final class MetalakeCatalogWrapperManager implements CatalogWrapperManager, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MetalakeCatalogWrapperManager.class);

  private final String metalake;
  private final CatalogManager catalogManager;
  private final IcebergCatalogWrapperCache cache;
  private final Consumer<NameIdentifier> removalListener;
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * Uses {@link GravitinoEnv#catalogManager()} and default cache eviction.
   *
   * @param metalake metalake name
   */
  public MetalakeCatalogWrapperManager(String metalake) {
    this(metalake, GravitinoEnv.getInstance().catalogManager(), new IcebergConfig(Map.of()));
  }

  /**
   * @param metalake metalake name
   * @param catalogManager catalog manager used to resolve configs
   * @param cacheConfig Iceberg config supplying cache eviction settings
   */
  public MetalakeCatalogWrapperManager(
      String metalake, CatalogManager catalogManager, IcebergConfig cacheConfig) {
    Preconditions.checkArgument(StringUtils.isNotBlank(metalake), "metalake must not be blank");
    Preconditions.checkNotNull(catalogManager, "catalogManager");
    Preconditions.checkNotNull(cacheConfig, "cacheConfig");
    this.metalake = metalake;
    this.catalogManager = catalogManager;
    this.cache = IcebergCatalogWrapperCache.create(cacheConfig);
    this.removalListener =
        ident -> {
          if (closed.get()) {
            return;
          }
          if (ident.namespace().length() == 1 && metalake.equals(ident.namespace().level(0))) {
            cache.invalidate(ident.name());
          }
        };
    catalogManager.addCatalogCacheRemoveListener(removalListener);
  }

  /**
   * @return metalake this manager resolves catalogs for
   */
  public String metalake() {
    return metalake;
  }

  @Override
  public IcebergCatalogWrapper getCatalogWrapper(String catalogName) {
    checkOpen();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogName), "catalogName must not be blank");
    return cache.get(catalogName, this::createWrapper);
  }

  @Override
  public IcebergCatalogWrapper getOrCreateCatalogWrapper(
      String catalogName, IcebergConfig icebergConfig) {
    checkOpen();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogName), "catalogName must not be blank");
    Preconditions.checkNotNull(icebergConfig, "icebergConfig");
    return cache.get(catalogName, name -> IcebergCatalogWrapper.create(icebergConfig));
  }

  @Override
  public void invalidateCatalogWrapper(String catalogName) {
    if (closed.get()) {
      return;
    }
    cache.invalidate(catalogName);
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    try {
      cache.close();
    } catch (Exception e) {
      LOG.warn("Failed to close MetalakeCatalogWrapperManager cache for metalake {}", metalake, e);
    }
  }

  private IcebergCatalogWrapper createWrapper(String catalogName) {
    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(metalake, catalogName);
    IcebergConfig icebergConfig;
    try {
      icebergConfig =
          catalogManager.doWithCatalog(
              catalogIdent, catalog -> IcebergCatalogConfigResolver.resolve(catalog));
    } catch (NoSuchCatalogException e) {
      throw e;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to resolve Iceberg config for catalog " + catalogIdent, e);
    }
    return IcebergCatalogWrapper.create(icebergConfig);
  }

  private void checkOpen() {
    if (closed.get()) {
      throw new IllegalStateException(
          "MetalakeCatalogWrapperManager for metalake " + metalake + " is closed");
    }
  }
}
