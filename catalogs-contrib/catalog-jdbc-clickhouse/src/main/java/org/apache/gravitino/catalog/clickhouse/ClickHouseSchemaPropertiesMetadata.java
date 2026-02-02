/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.catalog.clickhouse;

import static org.apache.gravitino.connector.PropertyEntry.booleanPropertyEntry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.jdbc.JdbcSchemaPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class ClickHouseSchemaPropertiesMetadata extends JdbcSchemaPropertiesMetadata {
  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            booleanPropertyEntry(
                ClickHouseConfig.CK_ON_CLUSTER.getKey(),
                ClickHouseConfig.CK_ON_CLUSTER.getDoc(),
                false /* required */,
                false /* immutable */,
                false /* defaultValue */,
                false /* hidden */,
                false /* reserved */));
    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  public Map<String, PropertyEntry<?>> propertyEntries() {
    Map<String, PropertyEntry<?>> stringPropertyEntryMap = super.propertyEntries();
    return new ImmutableMap.Builder<String, PropertyEntry<?>>()
        .putAll(stringPropertyEntryMap)
        .putAll(PROPERTIES_METADATA)
        .build();
  }
}
