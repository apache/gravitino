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
package org.apache.gravitino.catalog.lakehouse;

import static org.apache.gravitino.connector.PropertyEntry.booleanPropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.enumPropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class GenericLakehouseTablePropertiesMetadata extends BasePropertiesMetadata {
  public static final String LAKEHOUSE_LOCATION = "location";
  public static final String LAKEHOUSE_FORMAT = "format";
  public static final String LANCE_TABLE_STORAGE_OPTION_PREFIX = "lance.storage.";
  public static final String LAKEHOUSE_REGISTER = "register";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringOptionalPropertyEntry(
                LAKEHOUSE_LOCATION,
                "The root directory of the lakehouse catalog.",
                false /* immutable */,
                null, /* defaultValue */
                false /* hidden */),
            enumPropertyEntry(
                LAKEHOUSE_FORMAT,
                "The table format of the lakehouse table (e.g., iceberg, delta, lance)",
                true /* required */,
                true /* immutable */,
                LakehouseTableFormat.class /* enumClass */,
                null /* defaultValue */,
                false /* hidden */,
                false /* reserved */),
            PropertyEntry.stringOptionalPropertyPrefixEntry(
                LANCE_TABLE_STORAGE_OPTION_PREFIX,
                "The storage options passed to Lance table.",
                false /* immutable */,
                null /* default value*/,
                false /* hidden */,
                false /* reserved */),
            booleanPropertyEntry(
                LAKEHOUSE_REGISTER,
                "Whether this is a table registration operation.",
                true /* immutable */,
                false /* defaultValue */,
                false /* hidden */,
                false,
                false));

    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
