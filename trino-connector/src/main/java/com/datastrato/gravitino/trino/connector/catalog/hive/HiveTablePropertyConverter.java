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
package com.datastrato.gravitino.trino.connector.catalog.hive;

import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_EXTERNAL;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_FORMAT;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_INPUT_FORMAT;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_LOCATION;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_NUM_FILES;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_OUTPUT_FORMAT;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_SERDE_LIB;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_SERDE_NAME;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_TOTAL_SIZE;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_TYPE;

import com.apache.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class HiveTablePropertyConverter extends PropertyConverter {
  private final BasePropertiesMetadata hiveTablePropertiesMetadata =
      new HiveTablePropertiesMetadata();
  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  @VisibleForTesting
  static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put(HIVE_TABLE_FORMAT, HiveTablePropertiesMetadata.FORMAT)
              .put(HIVE_TABLE_TOTAL_SIZE, HiveTablePropertiesMetadata.TOTAL_SIZE)
              .put(HIVE_TABLE_NUM_FILES, HiveTablePropertiesMetadata.NUM_FILES)
              .put(HIVE_TABLE_EXTERNAL, HiveTablePropertiesMetadata.EXTERNAL)
              .put(HIVE_TABLE_LOCATION, HiveTablePropertiesMetadata.LOCATION)
              .put(HIVE_TABLE_TYPE, HiveTablePropertiesMetadata.TABLE_TYPE)
              .put(HIVE_TABLE_INPUT_FORMAT, HiveTablePropertiesMetadata.INPUT_FORMAT)
              .put(HIVE_TABLE_OUTPUT_FORMAT, HiveTablePropertiesMetadata.OUTPUT_FORMAT)
              .put(HIVE_TABLE_SERDE_LIB, HiveTablePropertiesMetadata.SERDE_LIB)
              .put(HIVE_TABLE_SERDE_NAME, HiveTablePropertiesMetadata.SERDE_NAME)
              .build());

  @Override
  public TreeBidiMap<String, String> engineToGravitinoMapping() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }

  @Override
  public Map<String, PropertyEntry<?>> gravitinoPropertyMeta() {
    return hiveTablePropertiesMetadata.propertyEntries();
  }
}
