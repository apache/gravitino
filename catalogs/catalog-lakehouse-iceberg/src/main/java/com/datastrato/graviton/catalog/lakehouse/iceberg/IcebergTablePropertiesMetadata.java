/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg;

import static com.datastrato.graviton.catalog.PropertyEntry.stringReservedPropertyEntry;

import com.datastrato.graviton.catalog.BasePropertiesMetadata;
import com.datastrato.graviton.catalog.PropertyEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public class IcebergTablePropertiesMetadata extends BasePropertiesMetadata {
  public static final String COMMENT = "comment";
  public static final String CREATOR = "creator";
  public static final String LOCATION = "location";
  public static final String CURRENT_SNAPSHOT_ID = "current-snapshot-id";
  public static final String CHERRY_PICK_SNAPSHOT_ID = "cherry-pick-snapshot-id";
  public static final String SORT_ORDER = "sort-order";
  public static final String IDENTIFIER_FIELDS = "identifier-fields";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringReservedPropertyEntry(COMMENT, "table comment", true),
            stringReservedPropertyEntry(CREATOR, "table creator info", false),
            stringReservedPropertyEntry(LOCATION, "Iceberg location for table storage", false),
            stringReservedPropertyEntry(
                CURRENT_SNAPSHOT_ID,
                "The snapshot representing the current state of the table",
                false),
            stringReservedPropertyEntry(
                CHERRY_PICK_SNAPSHOT_ID,
                "Selecting a specific snapshots in a merge operation",
                false),
            stringReservedPropertyEntry(
                SORT_ORDER, "Selecting a specific snapshots in a merge operation", false),
            stringReservedPropertyEntry(
                IDENTIFIER_FIELDS, "The identifier field(s) for defining the table", false));
    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
