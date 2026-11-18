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

import static org.apache.gravitino.flink.connector.utils.FactoryUtils.isGravitinoManagedCatalogType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.table.catalog.AbstractCatalogStore;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A catalog store that combines a session-scoped in-memory {@link GenericInMemoryCatalogStore} with
 * a persistent {@link GravitinoCatalogStore}.
 *
 * <p>Routing is based on the catalog type declared in the {@link CatalogDescriptor}:
 *
 * <ul>
 *   <li><b>Gravitino-managed catalog types</b> (e.g. {@code gravitino-hive}, {@code
 *       gravitino-iceberg}) are persisted to the Gravitino-backed store and are visible across all
 *       Flink sessions and Gravitino clients that share the same metalake.
 *   <li><b>All other catalog types</b> (e.g. {@code generic_in_memory}, third-party connectors) are
 *       stored in the session-scoped in-memory store only and are not persisted to Gravitino.
 * </ul>
 *
 * <p>When retrieving or removing catalogs, entries in the in-memory store take precedence over
 * entries in the Gravitino-backed store.
 *
 * <p>Catalog names in the excluded set are never resolved against the Gravitino-backed store: every
 * lookup, existence check, listing, and removal for these names consults the in-memory store only.
 * This allows a session catalog to use the same name as an existing Gravitino catalog without
 * loading, modifying, or dropping the Gravitino catalog. Gravitino-managed catalog types cannot be
 * created under an excluded name because they would be persisted to Gravitino.
 *
 * <p>When session catalog shadowing is enabled, a session catalog can be created under the name of
 * a Gravitino catalog that this session has not accessed yet, without listing the name in advance.
 * Flink checks {@link #contains(String)} before it creates a catalog, but it also uses {@link
 * #contains(String)} before it drops or alters one. To keep both working, this store tracks the
 * Gravitino catalogs that the session has accessed, that is, loaded through {@link
 * #getCatalog(String)} or created through {@link #storeCatalog(String, CatalogDescriptor)}. These
 * catalogs keep the existing behavior. A Gravitino catalog that the session has not accessed is
 * reported as absent by {@link #contains(String)}, so a session catalog can take its name. The
 * session catalog then shadows the Gravitino catalog in this session, and dropping the session
 * catalog makes the Gravitino catalog visible again. Gravitino-managed catalog types cannot be
 * created under the name of an existing Gravitino catalog.
 *
 * <p>This store is intended to be used per Flink session, keeping transient catalogs in memory
 * while delegating persistent catalogs to Apache Gravitino.
 */
public class GravitinoSessionCatalogStore extends AbstractCatalogStore {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoSessionCatalogStore.class);

  /**
   * The name of the Gravitino catalog that {@link #contains(String)} has just reported as absent
   * because of shadowing. Flink calls {@link #contains(String)} and then, on the same thread,
   * creates the catalog without calling this store in between. The creation step reads the name to
   * reject Gravitino-managed catalog types and to log the shadowing. Every other method of this
   * store clears it, so it never outlives the catalog creation that follows the check.
   */
  private static final ThreadLocal<String> SHADOWED_GRAVITINO_CATALOG = new ThreadLocal<>();

  private final GenericInMemoryCatalogStore memoryCatalogStore;
  private final GravitinoCatalogStore gravitinoCatalogStore;
  private final Set<String> excludedCatalogs;
  private final boolean sessionCatalogShadowing;
  private final Set<String> accessedGravitinoCatalogs = ConcurrentHashMap.newKeySet();

  /**
   * Creates a session catalog store without excluded catalog names.
   *
   * @param gravitinoCatalogStore the Gravitino-backed catalog store
   * @param memoryCatalogStore the session-scoped in-memory catalog store
   */
  public GravitinoSessionCatalogStore(
      GravitinoCatalogStore gravitinoCatalogStore, GenericInMemoryCatalogStore memoryCatalogStore) {
    this(gravitinoCatalogStore, memoryCatalogStore, Collections.emptySet());
  }

  /**
   * Creates a session catalog store with session catalog shadowing disabled.
   *
   * @param gravitinoCatalogStore the Gravitino-backed catalog store
   * @param memoryCatalogStore the session-scoped in-memory catalog store
   * @param excludedCatalogs catalog names that are never resolved against the Gravitino-backed
   *     store
   */
  public GravitinoSessionCatalogStore(
      GravitinoCatalogStore gravitinoCatalogStore,
      GenericInMemoryCatalogStore memoryCatalogStore,
      Set<String> excludedCatalogs) {
    this(gravitinoCatalogStore, memoryCatalogStore, excludedCatalogs, false);
  }

  /**
   * Creates a session catalog store.
   *
   * @param gravitinoCatalogStore the Gravitino-backed catalog store
   * @param memoryCatalogStore the session-scoped in-memory catalog store
   * @param excludedCatalogs catalog names that are never resolved against the Gravitino-backed
   *     store
   * @param sessionCatalogShadowing whether a session catalog can shadow a Gravitino catalog with
   *     the same name that this session has not accessed yet
   */
  public GravitinoSessionCatalogStore(
      GravitinoCatalogStore gravitinoCatalogStore,
      GenericInMemoryCatalogStore memoryCatalogStore,
      Set<String> excludedCatalogs,
      boolean sessionCatalogShadowing) {
    Preconditions.checkArgument(gravitinoCatalogStore != null, "CatalogStore cannot be null");
    Preconditions.checkArgument(memoryCatalogStore != null, "MemoryCatalogStore cannot be null");
    Preconditions.checkArgument(excludedCatalogs != null, "ExcludedCatalogs cannot be null");
    this.gravitinoCatalogStore = gravitinoCatalogStore;
    this.memoryCatalogStore = memoryCatalogStore;
    this.excludedCatalogs = ImmutableSet.copyOf(excludedCatalogs);
    this.sessionCatalogShadowing = sessionCatalogShadowing;
  }

  /**
   * Rejects instantiating a Gravitino-managed catalog that would shadow a Gravitino catalog with
   * the same name. Gravitino catalog factories call this method before they create a catalog,
   * because Flink may instantiate and register a catalog before it stores the catalog in the
   * catalog store. Loading an existing Gravitino catalog is not affected, because Flink always
   * resolves it through {@link #getCatalog(String)} first.
   *
   * <p>This method is intended for Gravitino catalog factories only.
   *
   * @param catalogName the name of the catalog to create
   * @param catalogType the Gravitino-managed catalog type
   * @throws CatalogException if a Gravitino catalog with the same name exists and is shadowed in
   *     the current session
   */
  public static void checkGravitinoManagedCatalogNotShadowing(
      String catalogName, String catalogType) throws CatalogException {
    String shadowedCatalog = SHADOWED_GRAVITINO_CATALOG.get();
    SHADOWED_GRAVITINO_CATALOG.remove();
    if (shadowedCatalog != null && shadowedCatalog.equals(catalogName)) {
      throw gravitinoManagedCatalogAlreadyExists(catalogName, catalogType);
    }
  }

  /** Clears the Gravitino catalog name recorded on the current thread. Used by tests only. */
  @VisibleForTesting
  static void clearShadowedGravitinoCatalog() {
    SHADOWED_GRAVITINO_CATALOG.remove();
  }

  @Override
  public void storeCatalog(String catalogName, CatalogDescriptor descriptor)
      throws CatalogException {
    boolean shadowing = isShadowedGravitinoCatalog(catalogName);
    SHADOWED_GRAVITINO_CATALOG.remove();
    String catalogType = descriptor.getConfiguration().get(CommonCatalogOptions.CATALOG_TYPE);
    if (catalogType == null) {
      throw new CatalogException(
          String.format(
              "Cannot store catalog '%s': '%s' is not set in the catalog descriptor. "
                  + "Please specify a catalog type.",
              catalogName, CommonCatalogOptions.CATALOG_TYPE.key()));
    }

    if (isGravitinoManagedCatalogType(catalogType)) {
      if (isExcluded(catalogName)) {
        throw new CatalogException(
            String.format(
                "Cannot create Gravitino-managed catalog '%s' of type '%s': the catalog name is "
                    + "excluded from Gravitino by '%s', so only session catalogs can use it.",
                catalogName,
                catalogType,
                GravitinoCatalogStoreFactoryOptions.GRAVITINO_EXCLUDED_CATALOGS.key()));
      }
      if (shadowing) {
        throw gravitinoManagedCatalogAlreadyExists(catalogName, catalogType);
      }
      gravitinoCatalogStore.storeCatalog(catalogName, descriptor);
      if (sessionCatalogShadowing) {
        accessedGravitinoCatalogs.add(catalogName);
      }
    } else {
      memoryCatalogStore.storeCatalog(catalogName, descriptor);
      if (shadowing) {
        LOG.warn(
            "Session catalog '{}' of type '{}' shadows the Gravitino catalog with the same name "
                + "in this Flink session. The Gravitino catalog is not loaded, modified, or "
                + "dropped while it is shadowed.",
            catalogName,
            catalogType);
      }
    }
  }

  /**
   * Removes the specified catalog. Excluded catalog names are only removed from the in-memory
   * store, so the Gravitino catalog with the same name is never dropped.
   *
   * @param catalogName name of the catalog to remove
   * @param ignoreIfNotExists if true, ignore when the catalog does not exist
   * @throws CatalogException if the catalog cannot be removed
   */
  @Override
  public void removeCatalog(String catalogName, boolean ignoreIfNotExists) throws CatalogException {
    SHADOWED_GRAVITINO_CATALOG.remove();
    if (isExcluded(catalogName) || memoryCatalogStore.contains(catalogName)) {
      memoryCatalogStore.removeCatalog(catalogName, ignoreIfNotExists);
    } else {
      gravitinoCatalogStore.removeCatalog(catalogName, ignoreIfNotExists);
      accessedGravitinoCatalogs.remove(catalogName);
    }
  }

  /**
   * Get a catalog by name. Excluded catalog names are only looked up in the in-memory store.
   *
   * @param catalogName name of the catalog to retrieve
   * @return the requested catalog or empty if the catalog does not exist
   * @throws CatalogException throw a CatalogException when the Catalog cannot be created.
   */
  @Override
  public Optional<CatalogDescriptor> getCatalog(String catalogName) throws CatalogException {
    SHADOWED_GRAVITINO_CATALOG.remove();
    Optional<CatalogDescriptor> descriptor = memoryCatalogStore.getCatalog(catalogName);
    if (descriptor.isPresent() || isExcluded(catalogName)) {
      return descriptor;
    }
    Optional<CatalogDescriptor> gravitinoDescriptor = gravitinoCatalogStore.getCatalog(catalogName);
    if (sessionCatalogShadowing && gravitinoDescriptor.isPresent()) {
      accessedGravitinoCatalogs.add(catalogName);
    }
    return gravitinoDescriptor;
  }

  /**
   * Lists the catalogs of both stores. Gravitino catalogs whose names are excluded are omitted.
   *
   * @return the names of all catalogs visible in this session
   * @throws CatalogException if the catalogs cannot be listed
   */
  @Override
  public Set<String> listCatalogs() throws CatalogException {
    SHADOWED_GRAVITINO_CATALOG.remove();
    Set<String> catalogs = new HashSet<>();
    catalogs.addAll(memoryCatalogStore.listCatalogs());

    Set<String> gravitinoCatalogs;
    try {
      gravitinoCatalogs = gravitinoCatalogStore.listCatalogs();
    } catch (CatalogException e) {
      throw new CatalogException("Failed to list catalog.", e);
    } catch (RuntimeException e) {
      throw new CatalogException("Failed to list catalog.", e);
    }
    for (String catalogName : gravitinoCatalogs) {
      if (!isExcluded(catalogName)) {
        catalogs.add(catalogName);
      }
    }
    return catalogs;
  }

  /**
   * Checks whether a catalog exists. Excluded catalog names are only checked in the in-memory
   * store. When session catalog shadowing is enabled, a Gravitino catalog that this session has not
   * accessed is reported as absent, so that a session catalog can be created under its name.
   *
   * @param catalogName name of the catalog to check
   * @return true if the catalog exists in this session
   * @throws CatalogException if the existence cannot be checked
   */
  @Override
  public boolean contains(String catalogName) throws CatalogException {
    SHADOWED_GRAVITINO_CATALOG.remove();
    if (memoryCatalogStore.contains(catalogName)) {
      return true;
    }
    if (isExcluded(catalogName)) {
      return false;
    }
    if (!sessionCatalogShadowing || accessedGravitinoCatalogs.contains(catalogName)) {
      return gravitinoCatalogStore.contains(catalogName);
    }
    if (gravitinoCatalogStore.contains(catalogName)) {
      SHADOWED_GRAVITINO_CATALOG.set(catalogName);
    }
    return false;
  }

  private boolean isExcluded(String catalogName) {
    return excludedCatalogs.contains(catalogName);
  }

  private boolean isShadowedGravitinoCatalog(String catalogName) {
    return sessionCatalogShadowing && catalogName.equals(SHADOWED_GRAVITINO_CATALOG.get());
  }

  private static CatalogException gravitinoManagedCatalogAlreadyExists(
      String catalogName, String catalogType) {
    return new CatalogException(
        String.format(
            "Cannot create Gravitino-managed catalog '%s' of type '%s': Catalog %s already "
                + "exists in Gravitino. Session catalog shadowing only allows session catalogs to "
                + "use the name of an existing Gravitino catalog.",
            catalogName, catalogType, catalogName));
  }
}
