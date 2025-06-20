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
package org.apache.gravitino;

import java.util.Map;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;

/**
 * Client interface for supporting catalogs. It includes methods for listing, loading, creating,
 * altering and dropping catalogs.
 */
@Evolving
public interface SupportsCatalogs {

  /**
   * List the name of all catalogs in the metalake.
   *
   * @return The list of catalog's names.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  String[] listCatalogs() throws NoSuchMetalakeException;

  /**
   * List all catalogs with their information in the metalake.
   *
   * @return The list of catalog's information.
   * @throws NoSuchMetalakeException If the metalake with namespace does not exist.
   */
  Catalog[] listCatalogsInfo() throws NoSuchMetalakeException;

  /**
   * Load a catalog by its name.
   *
   * @param catalogName the name of the catalog.
   * @return The catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   */
  Catalog loadCatalog(String catalogName) throws NoSuchCatalogException;

  /**
   * Check if a catalog exists.
   *
   * @param catalogName The name of the catalog.
   * @return True if the catalog exists, false otherwise.
   */
  default boolean catalogExists(String catalogName) {
    try {
      loadCatalog(catalogName);
      return true;
    } catch (NoSuchCatalogException e) {
      return false;
    }
  }

  /**
   * Create a catalog with specified catalog name, type, provider, comment, and properties.
   *
   * <p>The parameter "provider" is a short name of the catalog, used to tell Gravitino which
   * catalog should be created. The short name:
   *
   * <p>1) should be the same as the {@link CatalogProvider} interface provided. 2) can be "null" if
   * the created catalog is the managed catalog, like model, fileset catalog. For the details of the
   * provider definition, see {@link CatalogProvider}.
   *
   * @param catalogName the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog, or null if the catalog is a managed catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   * @return The created catalog.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws CatalogAlreadyExistsException If the catalog already exists.
   */
  Catalog createCatalog(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException;

  /**
   * Create a managed catalog with specified catalog name, type, comment, and properties.
   *
   * @param catalogName the name of the catalog.
   * @param type the type of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   * @return The created catalog.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws CatalogAlreadyExistsException If the catalog already exists.
   */
  default Catalog createCatalog(
      String catalogName, Catalog.Type type, String comment, Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    return createCatalog(catalogName, type, null, comment, properties);
  }

  /**
   * Alter a catalog with specified catalog name and changes.
   *
   * @param catalogName the name of the catalog.
   * @param changes the changes to apply to the catalog.
   * @return The altered catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   * @throws IllegalArgumentException If the changes cannot be applied to the catalog.
   */
  Catalog alterCatalog(String catalogName, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException;

  /**
   * Drop a catalog with specified name. Please make sure:
   *
   * <ul>
   *   <li>There is no schema in the catalog. Otherwise, a {@link NonEmptyEntityException} will be
   *       thrown.
   *   <li>The method {@link #disableCatalog(String)} has been called before dropping the catalog.
   *       Otherwise, a {@link CatalogInUseException} will be thrown.
   * </ul>
   *
   * It is equivalent to calling {@code dropCatalog(ident, false)}.
   *
   * @param catalogName the name of the catalog.
   * @return True if the catalog was dropped, false if the catalog does not exist.
   * @throws NonEmptyEntityException If the catalog is not empty.
   * @throws CatalogInUseException If the catalog is in use.
   */
  default boolean dropCatalog(String catalogName)
      throws NonEmptyEntityException, CatalogInUseException {
    return dropCatalog(catalogName, false);
  }

  /**
   * Drop a catalog with specified name. If the force flag is true, it will:
   *
   * <ul>
   *   <li>Cascade drop all sub-entities (schemas, tables, etc.) of the catalog in Gravitino store.
   *   <li>Drop the catalog even if it is in use.
   *   <li>External resources (e.g. database, table, etc.) associated with sub-entities will not be
   *       dropped unless it is managed (such as managed fileset).
   * </ul>
   *
   * If the force flag is false, it is equivalent to calling {@link #dropCatalog(String)}.
   *
   * @param catalogName The identifier of the catalog.
   * @param force Whether to force the drop.
   * @return True if the catalog was dropped, false if the catalog does not exist.
   * @throws NonEmptyEntityException If the catalog is not empty and force is false.
   * @throws CatalogInUseException If the catalog is in use and force is false.
   */
  boolean dropCatalog(String catalogName, boolean force)
      throws NonEmptyEntityException, CatalogInUseException;

  /**
   * Enable a catalog. If the catalog is already enabled, this method does nothing.
   *
   * @param catalogName The identifier of the catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   */
  void enableCatalog(String catalogName) throws NoSuchCatalogException;

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
   * @param catalogName The identifier of the catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   */
  void disableCatalog(String catalogName) throws NoSuchCatalogException;

  /**
   * Test whether the catalog with specified parameters can be connected to before creating it.
   *
   * @param catalogName the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   * @throws Exception if the test failed.
   */
  void testConnection(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception;
}
