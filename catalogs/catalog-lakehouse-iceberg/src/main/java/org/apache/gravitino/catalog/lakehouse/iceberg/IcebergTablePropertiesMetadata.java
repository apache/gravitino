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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
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

  /**
   * The default Iceberg table format version Gravitino applies when {@link #FORMAT_VERSION} is not
   * explicitly set. Gravitino owns this default rather than deferring to the Iceberg library's own
   * version-dependent default, and stamps it onto the table at creation.
   */
  public static final int ICEBERG_DEFAULT_FORMAT_VERSION = 2;

  /**
   * The Iceberg table format versions Gravitino allows creating: {@code 1}–{@code 4}, the range the
   * bundled Iceberg version (1.11.0) can write. Gravitino is not more restrictive than Iceberg for
   * the create passthrough; each version's feature set is defined by the Iceberg spec (v1/v2/v3 are
   * adopted, v3 is required for V3 types such as {@code variant}, and v4 is under active
   * development and not yet finalized). An unset (empty) value defaults to {@link
   * #ICEBERG_DEFAULT_FORMAT_VERSION}. Extend this set as newer Iceberg writer versions ship.
   */
  public static final Set<Integer> SUPPORTED_FORMAT_VERSIONS = ImmutableSet.of(1, 2, 3, 4);

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
            formatVersionPropertyEntry(),
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

  /**
   * Builds the property entry for {@link #FORMAT_VERSION}, an immutable property that accepts an
   * unset value or one of {@link #SUPPORTED_FORMAT_VERSIONS} and defaults to {@link
   * #ICEBERG_DEFAULT_FORMAT_VERSION}.
   *
   * @return the {@code format-version} property entry.
   */
  private static PropertyEntry<Integer> formatVersionPropertyEntry() {
    return new PropertyEntry.Builder<Integer>()
        .withName(FORMAT_VERSION)
        .withDescription(
            "The Iceberg table format version. Gravitino supports creating tables at versions 1 to "
                + "4 (the range the bundled Iceberg version can write) and defaults to 2 when unset. "
                + "Version 3 is required for V3 types such as variant; version 4 is not yet a "
                + "finalized Iceberg spec.")
        .withRequired(false)
        .withImmutable(true)
        .withJavaType(Integer.class)
        .withDefaultValue(ICEBERG_DEFAULT_FORMAT_VERSION)
        .withDecoder(IcebergTablePropertiesMetadata::decodeFormatVersion)
        .withEncoder(String::valueOf)
        .withHidden(false)
        .withReserved(false)
        .build();
  }

  /**
   * Decodes and validates a user-supplied {@link #FORMAT_VERSION} value. An unset (null or blank)
   * value is allowed and resolves to {@link #ICEBERG_DEFAULT_FORMAT_VERSION}; otherwise the value
   * must be an integer in {@link #SUPPORTED_FORMAT_VERSIONS}.
   *
   * @param value the raw property value.
   * @return the parsed format version, or {@link #ICEBERG_DEFAULT_FORMAT_VERSION} when the value is
   *     unset.
   * @throws IllegalArgumentException if the value is neither blank nor a version in {@link
   *     #SUPPORTED_FORMAT_VERSIONS}.
   */
  private static Integer decodeFormatVersion(String value) {
    if (StringUtils.isBlank(value)) {
      return ICEBERG_DEFAULT_FORMAT_VERSION;
    }
    int version = Integer.parseInt(value.trim());
    Preconditions.checkArgument(
        SUPPORTED_FORMAT_VERSIONS.contains(version),
        "Unsupported Iceberg format-version: %s, supported versions are %s",
        version,
        SUPPORTED_FORMAT_VERSIONS);
    return version;
  }
}
