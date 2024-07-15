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
package com.apache.gravitino.catalog.lakehouse.paimon;

import static com.apache.gravitino.connector.PropertyEntry.enumImmutablePropertyEntry;
import static com.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;
import static com.apache.gravitino.connector.PropertyEntry.stringRequiredPropertyEntry;

import com.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import com.apache.gravitino.connector.PropertiesMetadata;
import com.apache.gravitino.connector.PropertyEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link PropertiesMetadata} that represents Paimon catalog properties metadata.
 */
public class PaimonCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  @VisibleForTesting public static final String GRAVITINO_CATALOG_BACKEND = "catalog-backend";
  @VisibleForTesting public static final String PAIMON_METASTORE = "metastore";
  @VisibleForTesting public static final String WAREHOUSE = "warehouse";
  @VisibleForTesting public static final String URI = "uri";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;
  private static final Map<String, String> GRAVITINO_CONFIG_TO_PAIMON =
      ImmutableMap.of(GRAVITINO_CATALOG_BACKEND, PAIMON_METASTORE, WAREHOUSE, WAREHOUSE, URI, URI);

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            enumImmutablePropertyEntry(
                GRAVITINO_CATALOG_BACKEND,
                "Paimon catalog backend type",
                true,
                PaimonCatalogBackend.class,
                null,
                false,
                false),
            stringRequiredPropertyEntry(WAREHOUSE, "Paimon catalog warehouse config", false, false),
            stringOptionalPropertyEntry(URI, "Paimon catalog uri config", false, null, false));
    HashMap<String, PropertyEntry<?>> result = Maps.newHashMap(BASIC_CATALOG_PROPERTY_ENTRIES);
    result.putAll(Maps.uniqueIndex(propertyEntries, PropertyEntry::getName));
    PROPERTIES_METADATA = ImmutableMap.copyOf(result);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  protected Map<String, String> transformProperties(Map<String, String> properties) {
    Map<String, String> gravitinoConfig = Maps.newHashMap();
    properties.forEach(
        (key, value) -> {
          if (GRAVITINO_CONFIG_TO_PAIMON.containsKey(key)) {
            gravitinoConfig.put(GRAVITINO_CONFIG_TO_PAIMON.get(key), value);
          }
        });
    return gravitinoConfig;
  }
}
