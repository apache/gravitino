/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import static com.datastrato.gravitino.catalog.PropertyEntry.stringOptionalPropertyEntry;

import com.datastrato.gravitino.catalog.BasePropertiesMetadata;
import com.datastrato.gravitino.catalog.PropertyEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public class HiveSchemaPropertiesMetadata extends BasePropertiesMetadata {
  public static final String LOCATION = "location";
  private static final Map<String, PropertyEntry<?>> propertiesMetadata;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringOptionalPropertyEntry(
                LOCATION, "The directory for Hive database storage", false, null, false));

    propertiesMetadata = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return propertiesMetadata;
  }
}
