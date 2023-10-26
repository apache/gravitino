/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import static com.datastrato.gravitino.catalog.PropertyEntry.stringReservedPropertyEntry;

import com.datastrato.gravitino.catalog.BasePropertiesMetadata;
import com.datastrato.gravitino.catalog.PropertyEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public class IcebergSchemaPropertiesMetadata extends BasePropertiesMetadata {

  public static final String COMMENT = "comment";
  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(stringReservedPropertyEntry(COMMENT, "Schema comment", true));
    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
