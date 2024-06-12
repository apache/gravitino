/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import java.util.Map;

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
   * Load a catalog by its identifier.
   *
   * @param catalogName the identifier of the catalog.
   * @return The catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   */
  Catalog loadCatalog(String catalogName) throws NoSuchCatalogException;

  /**
   * Check if a catalog exists.
   *
   * @param catalogName The identifier of the catalog.
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
   * Create a catalog with specified identifier.
   *
   * <p>The parameter "provider" is a short name of the catalog, used to tell Gravitino which
   * catalog should be created. The short name should be the same as the {@link
   * com.datastrato.gravitino.CatalogProvider} interface provided.
   *
   * @param catalogName the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
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
   * Alter a catalog with specified identifier.
   *
   * @param catalogName the identifier of the catalog.
   * @param changes the changes to apply to the catalog.
   * @return The altered catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   * @throws IllegalArgumentException If the changes cannot be applied to the catalog.
   */
  Catalog alterCatalog(String catalogName, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException;

  /**
   * Drop a catalog with specified identifier.
   *
   * @param catalogName the name of the catalog.
   * @return True if the catalog was dropped, false otherwise.
   */
  boolean dropCatalog(String catalogName);
}
