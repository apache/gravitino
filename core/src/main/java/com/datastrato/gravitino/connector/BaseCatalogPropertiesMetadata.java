/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.connector;

import static com.datastrato.gravitino.Catalog.PROPERTY_PACKAGE;

import com.datastrato.gravitino.annotation.Evolving;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.Map;

@Evolving
public abstract class BaseCatalogPropertiesMetadata extends BasePropertiesMetadata {

  protected static final Map<String, PropertyEntry<?>> BASIC_CATALOG_PROPERTY_ENTRIES =
      Maps.uniqueIndex(
          ImmutableList.of(
              PropertyEntry.stringImmutablePropertyEntry(
                  PROPERTY_PACKAGE,
                  "The path of the catalog-related classes and resources",
                  false,
                  null,
                  false,
                  false),
              PropertyEntry.stringImmutablePropertyEntry(
                  BaseCatalog.CATALOG_OPERATION_IMPL,
                  "The classname of custom catalog operation to replace the default implementation",
                  false,
                  null,
                  false,
                  false)),
          PropertyEntry::getName);
}
