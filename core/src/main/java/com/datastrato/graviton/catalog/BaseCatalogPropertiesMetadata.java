/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog;

import static com.datastrato.graviton.Catalog.PROPERTY_PACKAGE;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class BaseCatalogPropertiesMetadata extends BasePropertiesMetadata {
  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return ImmutableMap.<String, PropertyEntry<?>>builder()
        .put(
            PROPERTY_PACKAGE,
            PropertyEntry.stringPropertyEntry(
                PROPERTY_PACKAGE,
                "The path of the catalog-related classes and resources",
                false,
                true,
                null,
                false,
                false))
        .build();
  }
}
