/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import java.util.Map;

public interface SupportCatalogs {

  /**
   * List all catalogs in the metalake under the namespace {@link Namespace}.
   *
   * @param namespace The namespace to list the catalogs under it.
   * @return The list of catalogs.
   * @throws NoSuchMetalakeException If the metalake with namespace does not exist.
   */
  Catalog[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException;

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
   * @param ident the identifier of the catalog.
   * @param type the type of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   * @return The created catalog.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws CatalogAlreadyExistsException If the catalog already exists.
   */
  Catalog createCatalog(
      NameIdentifier ident, Catalog.Type type, String comment, Map<String, String> properties)
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
   * Drop a catalog with specified identifier.
   *
   * @param ident the identifier of the catalog.
   * @return True if the catalog was dropped, false otherwise.
   */
  boolean dropCatalog(NameIdentifier ident);
}
