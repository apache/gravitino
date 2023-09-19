/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package io.trino.plugin.graviton.metadata;

import com.datastrato.graviton.Catalog;

public class GravitonCatalog {

  private final Catalog catalog;

  public GravitonCatalog(Catalog catalog) {
    this.catalog = catalog;
  }

  public String getProvider() {
    return catalog.properties().getOrDefault("provider", "");
  }

  public String getName() {
    return catalog.name();
  }

  public String getProperties(String name, String defaultValue) {
    return catalog.properties().getOrDefault(name, defaultValue);
  }
}
