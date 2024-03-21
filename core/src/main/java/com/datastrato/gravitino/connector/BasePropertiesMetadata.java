/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector;

import static com.datastrato.gravitino.StringIdentifier.ID_KEY;

import com.datastrato.gravitino.annotation.Evolving;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

/**
 * An abstract class representing a base properties metadata for entities. Developers should extend
 * this class to implement a custom properties metadata for their entities, like table, fileset,
 * schema.
 *
 * <p>Note: For catalog related properties metadata, use {@link BaseCatalogPropertiesMetadata}.
 *
 * <p>This class defines two reserved properties metadata for Gravitino use only. Developers should
 * not override these properties.
 */
@Evolving
public abstract class BasePropertiesMetadata implements PropertiesMetadata {

  // "gravitino.managed.entity" is an internal and reserved property to indicate whether the
  // entity is managed by Gravitino's own store or not.
  public static final String GRAVITINO_MANAGED_ENTITY = "gravitino.managed.entity";

  private static final Map<String, PropertyEntry<?>> BASIC_PROPERTY_ENTRIES;

  private volatile Map<String, PropertyEntry<?>> propertyEntries;

  static {
    // basicPropertyEntries is shared by all entities
    List<PropertyEntry<?>> basicPropertyEntries =
        ImmutableList.of(
            PropertyEntry.stringReservedPropertyEntry(
                ID_KEY,
                "To differentiate the entities created directly by the underlying sources",
                true),
            PropertyEntry.booleanReservedPropertyEntry(
                GRAVITINO_MANAGED_ENTITY,
                "Whether the entity is managed by Gravitino's own store or not",
                false,
                true));

    BASIC_PROPERTY_ENTRIES = Maps.uniqueIndex(basicPropertyEntries, PropertyEntry::getName);
  }

  @Override
  public Map<String, PropertyEntry<?>> propertyEntries() {
    if (propertyEntries == null) {
      synchronized (this) {
        if (propertyEntries == null) {
          ImmutableMap.Builder<String, PropertyEntry<?>> builder = ImmutableMap.builder();
          Map<String, PropertyEntry<?>> properties = specificPropertyEntries();
          builder.putAll(properties);

          BASIC_PROPERTY_ENTRIES.forEach(
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

  /**
   * Returns the specific property entries for the entity. Developers should override this method to
   * provide the specific property entries for their entities.
   *
   * @return The specific property entries for the entity.
   */
  @Evolving
  protected abstract Map<String, PropertyEntry<?>> specificPropertyEntries();
}
