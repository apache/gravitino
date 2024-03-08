/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.annotation.Evolving;

/**
 * A Catalog provider is a class that provides a short name for a catalog. This short name is used
 * when creating a catalog.
 */
@Evolving
public interface CatalogProvider {

  /**
   * The string that represents the catalog that this provider uses. This is overridden by children
   * to provide a nice alias for the catalog.
   *
   * @return The string that represents the catalog that this provider uses.
   */
  String shortName();
}
