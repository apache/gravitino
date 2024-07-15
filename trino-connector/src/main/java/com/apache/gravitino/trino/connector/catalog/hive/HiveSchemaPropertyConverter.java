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
package com.apache.gravitino.trino.connector.catalog.hive;

import static com.apache.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_SCHEMA_LOCATION;

import com.apache.gravitino.catalog.hive.HiveSchemaPropertiesMetadata;
import com.apache.gravitino.catalog.property.PropertyConverter;
import com.apache.gravitino.connector.BasePropertiesMetadata;
import com.apache.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class HiveSchemaPropertyConverter extends PropertyConverter {
  private final BasePropertiesMetadata hiveSchemaPropertiesMetadata =
      new HiveSchemaPropertiesMetadata();

  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put(HIVE_SCHEMA_LOCATION, HiveSchemaPropertiesMetadata.LOCATION)
              .build());

  @Override
  public TreeBidiMap<String, String> engineToGravitinoMapping() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }

  @Override
  public Map<String, PropertyEntry<?>> gravitinoPropertyMeta() {
    return hiveSchemaPropertiesMetadata.propertyEntries();
  }
}
