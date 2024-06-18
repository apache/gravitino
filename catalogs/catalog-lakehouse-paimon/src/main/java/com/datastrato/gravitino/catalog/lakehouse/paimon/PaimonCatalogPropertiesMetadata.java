/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import com.datastrato.gravitino.connector.BaseCatalogPropertiesMetadata;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * Implementation of {@link PropertiesMetadata} that represents Paimon catalog properties metadata.
 */
public class PaimonCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA = ImmutableMap.of();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
