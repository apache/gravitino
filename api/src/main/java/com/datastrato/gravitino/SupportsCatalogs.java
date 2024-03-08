/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import java.util.Map;

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
   * Drop a catalog with specified identifier.
   *
   * @param ident the identifier of the catalog.
   * @return True if the catalog was dropped, false otherwise.
   */
  boolean dropCatalog(NameIdentifier ident);
}
