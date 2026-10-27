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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads an {@link IcebergCatalogWrapper} for lakehouse catalog operations.
 *
 * <p>Mirrors {@code DynamicIcebergConfigProvider}'s {@code CatalogFetcher} split for the reverse
 * direction (metalake → IRC):
 *
 * <ul>
 *   <li>{@link InternalCatalogWrapperFetcher} — aux IRC in the same JVM (like {@code
 *       InternalCatalogFetcher})
 *   <li>{@link LocalCatalogWrapperFetcher} — construct a lakehouse-owned wrapper (like the non-aux
 *       / self path)
 * </ul>
 *
 * <p>Aux binding is installed when {@code IcebergRESTServerContext} is created in auxiliary mode
 * (same lifecycle as that context singleton).
 */
public final class IcebergCatalogWrapperFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapperFetcher.class);

  private static final AtomicReference<Function<String, IcebergCatalogWrapper>> AUX_LOOKUP =
      new AtomicReference<>();

  /**
   * Binds the aux IRC catalog lookup (call from {@code IcebergRESTServerContext#create} in aux
   * mode).
   *
   * @param lookup {@code catalogWrapperManager::getCatalogWrapper}, or {@code null} to clear
   */
  public static void bindAuxLookup(@Nullable Function<String, IcebergCatalogWrapper> lookup) {
    AUX_LOOKUP.set(lookup);
    if (lookup == null) {
      LOG.info("Cleared aux Iceberg catalog wrapper lookup");
    } else {
      LOG.info("Bound aux Iceberg catalog wrapper lookup");
    }
  }

  /** Clears the aux IRC lookup. */
  public static void unbindAuxLookup() {
    bindAuxLookup(null);
  }

  /**
   * Creates a fetcher: internal when aux IRC is bound, otherwise local self-load.
   *
   * @return fetcher for {@link #load(String, IcebergConfig)}
   */
  public static IcebergCatalogWrapperFetcher create() {
    Function<String, IcebergCatalogWrapper> aux = AUX_LOOKUP.get();
    if (aux != null) {
      return new IcebergCatalogWrapperFetcher(new InternalCatalogWrapperFetcher(aux));
    }
    return new IcebergCatalogWrapperFetcher(new LocalCatalogWrapperFetcher());
  }

  private final CatalogWrapperFetcher delegate;

  private IcebergCatalogWrapperFetcher(CatalogWrapperFetcher delegate) {
    this.delegate = delegate;
  }

  /**
   * Loads the wrapper for {@code catalogName}.
   *
   * @param catalogName metalake catalog name
   * @param localConfig lakehouse catalog config (used by the local fetcher)
   * @return shared aux wrapper or a newly constructed local wrapper
   */
  public IcebergCatalogWrapper load(String catalogName, IcebergConfig localConfig) {
    return delegate.load(catalogName, localConfig);
  }

  /**
   * Whether this fetcher uses the aux IRC manager.
   *
   * @return true when bound to aux IRC
   */
  public boolean isInternal() {
    return delegate instanceof InternalCatalogWrapperFetcher;
  }

  /** Same role as {@code DynamicIcebergConfigProvider.CatalogFetcher}. */
  interface CatalogWrapperFetcher {
    IcebergCatalogWrapper load(String catalogName, IcebergConfig localConfig);
  }

  /**
   * Aux in-process path: reuse IRC {@code IcebergCatalogWrapperManager} (symmetric to {@code
   * InternalCatalogFetcher}).
   */
  static final class InternalCatalogWrapperFetcher implements CatalogWrapperFetcher {
    private final Function<String, IcebergCatalogWrapper> auxLookup;

    InternalCatalogWrapperFetcher(Function<String, IcebergCatalogWrapper> auxLookup) {
      this.auxLookup = auxLookup;
    }

    @Override
    public IcebergCatalogWrapper load(String catalogName, IcebergConfig localConfig) {
      try {
        IcebergCatalogWrapper shared = auxLookup.apply(catalogName);
        if (shared != null) {
          LOG.info("Using aux IRC catalog wrapper for lakehouse catalog '{}'", catalogName);
          return shared;
        }
      } catch (NoSuchCatalogException e) {
        LOG.debug(
            "Aux IRC has no catalog '{}'; falling back to local wrapper: {}",
            catalogName,
            e.getMessage());
      }
      return new LocalCatalogWrapperFetcher().load(catalogName, localConfig);
    }
  }

  /** Self-load path when aux IRC is absent or does not know the catalog name. */
  static final class LocalCatalogWrapperFetcher implements CatalogWrapperFetcher {
    @Override
    public IcebergCatalogWrapper load(String catalogName, IcebergConfig localConfig) {
      return new IcebergCatalogWrapper(localConfig);
    }
  }
}
