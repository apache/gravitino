/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import com.datastrato.gravitino.Catalog;

/** Help Gravitino connector access CatalogMetadata from gravitino client. */
public class GravitinoCatalog {

  private final Catalog catalog;

  public GravitinoCatalog(Catalog catalog) {
    this.catalog = catalog;
  }

  public String getProvider() {
    return catalog.provider();
  }

  public String getName() {
    return catalog.name();
  }

  public String getProperties(String name, String defaultValue) {
    return catalog.properties().getOrDefault(name, defaultValue);
  }
}
