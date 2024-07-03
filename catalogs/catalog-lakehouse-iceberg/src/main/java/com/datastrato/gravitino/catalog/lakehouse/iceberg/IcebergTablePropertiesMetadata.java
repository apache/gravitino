/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
  public static final String PROVIDER = "provider";
  public static final String FORMAT = "format";
  public static final String FORMAT_VERSION = "format-version";
  public static final String DISTRIBUTION_MODE = TableProperties.WRITE_DISTRIBUTION_MODE;

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringReservedPropertyEntry(COMMENT, "The table comment", true),
            stringReservedPropertyEntry(CREATOR, "The table creator", false),
            stringImmutablePropertyEntry(
                LOCATION, "Iceberg location for table storage", false, null, false, false),
            stringImmutablePropertyEntry(FORMAT, "The table format", false, null, false, false),
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
            stringReservedPropertyEntry(DISTRIBUTION_MODE, "Write distribution mode", false),
            stringImmutablePropertyEntry(
                FORMAT_VERSION, "The Iceberg table format version, ", false, null, false, false),
            stringImmutablePropertyEntry(
                PROVIDER,
                "Iceberg provider for Iceberg table fileFormat, such as parquet, orc, avro, iceberg",
                false,
                null,
                false,
                false));
    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
