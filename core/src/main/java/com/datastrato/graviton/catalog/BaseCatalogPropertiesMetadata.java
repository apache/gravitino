/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog;

import static com.datastrato.graviton.Catalog.PROPERTY_PACKAGE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.Map;

public abstract class BaseCatalogPropertiesMetadata extends BasePropertiesMetadata {

  protected static final Map<String, PropertyEntry<?>> BASIC_CATALOG_PROPERTY_ENTRIES =
      Maps.uniqueIndex(
          ImmutableList.of(
              PropertyEntry.stringPropertyEntry(
                  PROPERTY_PACKAGE,
                  "The path of the catalog-related classes and resources",
                  false,
                  true,
                  null,
                  false,
                  false)),
          PropertyEntry::getName);
}
