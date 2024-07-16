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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import static org.apache.gravitino.connector.PropertyEntry.stringImmutablePropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringReservedPropertyEntry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.iceberg.TableProperties;

public class IcebergTablePropertiesMetadata extends BasePropertiesMetadata {
  public static final String COMMENT = IcebergConstants.COMMENT;
  public static final String CREATOR = IcebergConstants.CREATOR;
  public static final String LOCATION = IcebergConstants.LOCATION;
  public static final String CURRENT_SNAPSHOT_ID = IcebergConstants.CURRENT_SNAPSHOT_ID;
  public static final String CHERRY_PICK_SNAPSHOT_ID = IcebergConstants.CHERRY_PICK_SNAPSHOT_ID;
  public static final String SORT_ORDER = IcebergConstants.SORT_ORDER;
  public static final String IDENTIFIER_FIELDS = IcebergConstants.IDENTIFIER_FIELDS;
  public static final String PROVIDER = IcebergConstants.PROVIDER;
  public static final String FORMAT = IcebergConstants.FORMAT;
  public static final String FORMAT_VERSION = IcebergConstants.FORMAT_VERSION;
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
                "Iceberg provider for Iceberg table fileFormat, such as Parquet, Orc, Avro, or Iceberg",
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
