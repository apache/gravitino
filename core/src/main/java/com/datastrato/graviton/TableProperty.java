/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import static com.datastrato.graviton.StringIdentifier.ID_KEY;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public abstract class TableProperty implements PropertyMetadata {

  private static final Map<String, PropertyEntry<?>> BASIC_TABLE_PROPERTY;

  static {
    List<PropertyEntry<?>> basicTableProperty =
        ImmutableList.of(
            PropertyEntry.stringProperty(
                ID_KEY,
                "To differentiate the entities created directly by the underlying sources",
                false,
                true,
                "",
                true,
                true));

    BASIC_TABLE_PROPERTY = Maps.uniqueIndex(basicTableProperty, PropertyEntry::getName);
  }

  @Override
  public Map<String, PropertyEntry<?>> property() {
    ImmutableMap.Builder<String, PropertyEntry<?>> builder = ImmutableMap.builder();
    Map<String, PropertyEntry<?>> catalogTableProperty = tableProperty();
    builder.putAll(catalogTableProperty);

    BASIC_TABLE_PROPERTY.forEach(
        (name, entry) -> {
          Preconditions.checkArgument(
              !catalogTableProperty.containsKey(name), "Property metadata already exists: " + name);
          builder.put(name, entry);
        });

    return builder.build();
  }

  protected abstract Map<String, PropertyEntry<?>> tableProperty();
}
