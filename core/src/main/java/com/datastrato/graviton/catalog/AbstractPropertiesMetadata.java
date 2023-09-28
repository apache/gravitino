/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

import static com.datastrato.graviton.StringIdentifier.ID_KEY;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public abstract class AbstractPropertiesMetadata implements PropertiesMetadata {

  private static final Map<String, PropertyEntry<?>> BASIC_TABLE_PROPERTY_ENTRIES;

  private volatile Map<String, PropertyEntry<?>> propertyEntries;

  static {
    List<PropertyEntry<?>> basicTablePropertyEntries =
        ImmutableList.of(
            PropertyEntry.stringReservedPropertyEntry(
                ID_KEY,
                "To differentiate the entities created directly by the underlying sources",
                true));

    BASIC_TABLE_PROPERTY_ENTRIES =
        Maps.uniqueIndex(basicTablePropertyEntries, PropertyEntry::getName);
  }

  @Override
  public Map<String, PropertyEntry<?>> propertyEntries() {
    if (propertyEntries == null) {
      synchronized (this) {
        if (propertyEntries == null) {
          ImmutableMap.Builder<String, PropertyEntry<?>> builder = ImmutableMap.builder();
          Map<String, PropertyEntry<?>> properties = properties();
          builder.putAll(properties);

          BASIC_TABLE_PROPERTY_ENTRIES.forEach(
              (name, entry) -> {
                Preconditions.checkArgument(
                    !properties.containsKey(name), "Property metadata already exists: " + name);
                builder.put(name, entry);
              });

          propertyEntries = builder.build();
        }
      }
    }
    return propertyEntries;
  }

  protected abstract Map<String, PropertyEntry<?>> properties();
}
