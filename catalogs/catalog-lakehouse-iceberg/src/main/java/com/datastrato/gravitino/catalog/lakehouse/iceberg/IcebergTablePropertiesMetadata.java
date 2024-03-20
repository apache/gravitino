/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import static com.datastrato.gravitino.connector.PropertyEntry.stringImmutablePropertyEntry;
import static com.datastrato.gravitino.connector.PropertyEntry.stringReservedPropertyEntry;

import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableProperties;

public class IcebergTablePropertiesMetadata extends BasePropertiesMetadata {
  public static final String COMMENT = "comment";
  public static final String CREATOR = "creator";
  public static final String LOCATION = "location";
  public static final String CURRENT_SNAPSHOT_ID = "current-snapshot-id";
  public static final String CHERRY_PICK_SNAPSHOT_ID = "cherry-pick-snapshot-id";
  public static final String SORT_ORDER = "sort-order";
  public static final String IDENTIFIER_FIELDS = "identifier-fields";

  public static final String DISTRIBUTION_MODE = TableProperties.WRITE_DISTRIBUTION_MODE;

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringReservedPropertyEntry(COMMENT, "The table comment", true),
            stringReservedPropertyEntry(CREATOR, "The table creator", false),
            stringImmutablePropertyEntry(
                LOCATION, "Iceberg location for table storage", false, null, false, false),
            stringReservedPropertyEntry(
                CURRENT_SNAPSHOT_ID,
                "The snapshot represents the current state of the table",
                false),
            stringReservedPropertyEntry(
                CHERRY_PICK_SNAPSHOT_ID,
                "Selecting a specific snapshot in a merge operation",
                false),
            stringReservedPropertyEntry(
                SORT_ORDER, "Selecting a specific snapshot in a merge operation", false),
            stringReservedPropertyEntry(
                IDENTIFIER_FIELDS, "The identifier field(s) for defining the table", false),
            stringReservedPropertyEntry(DISTRIBUTION_MODE, "Write distribution mode", false));
    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
