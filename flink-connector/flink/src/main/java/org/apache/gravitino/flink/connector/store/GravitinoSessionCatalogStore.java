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

package org.apache.gravitino.flink.connector.store;

import static org.apache.gravitino.flink.connector.utils.FactoryUtils.isBuiltInCatalog;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.table.catalog.AbstractCatalogStore;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.util.Preconditions;

/**
 * A catalog store that combines a session-scoped in-memory
 * {@link GenericInMemoryCatalogStore} with a persistent {@link GravitinoCatalogStore}.
 *
 * <p>Catalogs for built-in catalog types are stored only in the in-memory store, while all
 * other catalogs are stored in the Gravitino-backed store. When retrieving, listing, or
 * removing catalogs, entries in the in-memory store take precedence over entries in the
 * Gravitino-backed store.
 *
 * <p>This store is intended to be used per Flink session, keeping transient catalogs in
 * memory while delegating persistent catalogs to Apache Gravitino.
 */
public class GravitinoSessionCatalogStore extends AbstractCatalogStore {
  private final GenericInMemoryCatalogStore memoryCatalogStore;
  private final GravitinoCatalogStore gravitinoCatalogStore;

  public GravitinoSessionCatalogStore(
      GravitinoCatalogStore gravitinoCatalogStore, GenericInMemoryCatalogStore memoryCatalogStore) {
    this.gravitinoCatalogStore =
        Preconditions.checkNotNull(gravitinoCatalogStore, "CatalogStore cannot be null");
    this.memoryCatalogStore =
        Preconditions.checkNotNull(memoryCatalogStore, "MemoryCatalogStore cannot be null");
  }

  @Override
  public void storeCatalog(String catalogName, CatalogDescriptor descriptor)
      throws CatalogException {
    String catalogType = descriptor.getConfiguration().get(CommonCatalogOptions.CATALOG_TYPE);
    if (isBuiltInCatalog(catalogType)) {
      memoryCatalogStore.storeCatalog(catalogName, descriptor);
    } else {
      gravitinoCatalogStore.storeCatalog(catalogName, descriptor);
    }
  }
  /**
   * Removes the specified catalog.
   *
   * @param catalogName name of the catalog to remove
   * @param ignoreIfNotExists if true, ignore when the catalog does not exist
   * @throws CatalogException if the catalog cannot be removed
   */
  @Override
  public void removeCatalog(String catalogName, boolean ignoreIfNotExists) throws CatalogException {
    if (memoryCatalogStore.contains(catalogName)) {
      memoryCatalogStore.removeCatalog(catalogName, ignoreIfNotExists);
    } else {
      gravitinoCatalogStore.removeCatalog(catalogName, ignoreIfNotExists);
    }
  }

  /**
   * Get a catalog by name.
   *
   * @param catalogName name of the catalog to retrieve
   * @return the requested catalog or empty if the catalog does not exist
   * @throws CatalogException throw a CatalogException when the Catalog cannot be created.
   */
  @Override
  public Optional<CatalogDescriptor> getCatalog(String catalogName) throws CatalogException {
    if (memoryCatalogStore.contains(catalogName)) {
      Optional<CatalogDescriptor> descriptor = memoryCatalogStore.getCatalog(catalogName);
      if (descriptor.isPresent()) {
        return descriptor;
      }
    }
    return gravitinoCatalogStore.getCatalog(catalogName);
  }

  @Override
  public Set<String> listCatalogs() throws CatalogException {
    Set<String> catalogs = new HashSet<>();
    catalogs.addAll(memoryCatalogStore.listCatalogs());
    try {
      catalogs.addAll(gravitinoCatalogStore.listCatalogs());
    } catch (Exception e) {
      throw new CatalogException("Failed to list catalog.", e);
    }
    return catalogs;
  }

  @Override
  public boolean contains(String catalogName) throws CatalogException {
    return memoryCatalogStore.contains(catalogName) || gravitinoCatalogStore.contains(catalogName);
  }
}
