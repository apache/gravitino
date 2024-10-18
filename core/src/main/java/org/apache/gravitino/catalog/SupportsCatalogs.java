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

import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.CatalogProvider;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;

/**
 * Interface for supporting catalogs. It includes methods for listing, loading, creating, altering
 * and dropping catalogs.
 */
@Evolving
public interface SupportsCatalogs {

  /**
   * List all catalogs in the metalake under the namespace {@link Namespace}.
   *
   * @param namespace The namespace to list the catalogs under it.
   * @return The list of catalog's name identifiers.
   * @throws NoSuchMetalakeException If the metalake with namespace does not exist.
   */
  NameIdentifier[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException;

  /**
   * List all catalogs with their information in the metalake under the namespace {@link Namespace}.
   *
   * @param namespace The namespace to list the catalogs under it.
   * @return The list of catalog's information.
   * @throws NoSuchMetalakeException If the metalake with namespace does not exist.
   */
  Catalog[] listCatalogsInfo(Namespace namespace) throws NoSuchMetalakeException;

  /**
   * Load a catalog by its identifier.
   *
   * @param ident the identifier of the catalog.
   * @return The catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   */
  Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException;

  /**
   * Check if a catalog exists.
   *
   * @param ident The identifier of the catalog.
   * @return True if the catalog exists, false otherwise.
   */
  default boolean catalogExists(NameIdentifier ident) {
    try {
      loadCatalog(ident);
      return true;
    } catch (NoSuchCatalogException e) {
      return false;
    }
  }

  /**
   * Create a catalog with specified identifier.
   *
   * <p>The parameter "provider" is a short name of the catalog, used to tell Gravitino which
   * catalog should be created. The short name should be the same as the {@link CatalogProvider}
   * interface provided.
   *
   * @param ident the identifier of the catalog.
   * @param type the type of the catalog.
   * @param comment the comment of the catalog.
   * @param provider the provider of the catalog.
   * @param properties the properties of the catalog.
   * @return The created catalog.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws CatalogAlreadyExistsException If the catalog already exists.
   */
  Catalog createCatalog(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException;

  /**
   * Alter a catalog with specified identifier.
   *
   * @param ident the identifier of the catalog.
   * @param changes the changes to apply to the catalog.
   * @return The altered catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   * @throws IllegalArgumentException If the changes cannot be applied to the catalog.
   */
  Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException;

  /**
   * Drop a catalog with specified identifier. Please make sure:
   *
   * <ul>
   *   <li>There is no schema in the catalog. Otherwise, a {@link NonEmptyEntityException} will be
   *       thrown.
   *   <li>The method {@link #disableCatalog(NameIdentifier)} has been called before dropping the
   *       catalog.
   * </ul>
   *
   * It is equivalent to calling {@code dropCatalog(ident, false)}.
   *
   * @param ident the identifier of the catalog.
   * @return True if the catalog was dropped, false if the catalog does not exist.
   * @throws NonEmptyEntityException If the catalog is not empty.
   * @throws CatalogInUseException If the catalog is in use.
   */
  default boolean dropCatalog(NameIdentifier ident)
      throws NonEmptyEntityException, CatalogInUseException {
    return dropCatalog(ident, false);
  }

  /**
   * Drop a catalog with specified identifier. If the force flag is true, it will:
   *
   * <ul>
   *   <li>Cascade drop all sub-entities (schemas, tables, etc.) of the catalog in Gravitino store.
   *   <li>Drop the catalog even if it is in use.
   *   <li>External resources (e.g. database, table, etc.) associated with sub-entities will not be
   *       dropped unless it is managed (such as managed fileset).
   * </ul>
   *
   * @param ident The identifier of the catalog.
   * @param force Whether to force the drop.
   * @return True if the catalog was dropped, false if the catalog does not exist.
   * @throws NonEmptyEntityException If the catalog is not empty and force is false.
   * @throws CatalogInUseException If the catalog is in use and force is false.
   */
  boolean dropCatalog(NameIdentifier ident, boolean force)
      throws NonEmptyEntityException, CatalogInUseException;

  /**
   * Test whether the catalog with specified parameters can be connected to before creating it.
   *
   * @param ident The identifier of the catalog to be tested.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   * @throws Exception If the connection test fails.
   */
  void testConnection(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception;

  /**
   * Enable a catalog. If the catalog is already enabled, this method does nothing.
   *
   * @param ident The identifier of the catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   * @throws CatalogNotInUseException If its parent metalake is not in use.
   */
  void enableCatalog(NameIdentifier ident) throws NoSuchCatalogException, CatalogNotInUseException;

  /**
   * Disable a catalog. If the catalog is already disabled, this method does nothing. Once a catalog
   * is disabled:
   *
   * <ul>
   *   <li>It can only be listed, loaded, dropped, or disable.
   *   <li>Any other operations on the catalog will throw an {@link CatalogNotInUseException}.
   *   <li>Any operation on the sub-entities (schemas, tables, etc.) will throw an {@link
   *       CatalogNotInUseException}.
   * </ul>
   *
   * @param ident The identifier of the catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   */
  void disableCatalog(NameIdentifier ident) throws NoSuchCatalogException;
}
