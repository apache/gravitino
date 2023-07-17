package com.datastrato.graviton;

/**
 * A Catalog provider is a class that provides a short name for a catalog. This short is used when
 * creating a catalog, Graviton will search the short name and its related class name to create the
 * catalog.
 */
public interface CatalogProvider {

  /**
   * The string that represents the catalog that this provider uses. This is overridden by children
   * to provide a nice alias for the catalog.
   */
  String shortName();
}
