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
package org.apache.gravitino.trino.connector.catalog.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.catalog.property.PropertyConverter;

/** Converts table properties between Trino and Apache Gravitino for Hive catalogs. */
public class HiveTablePropertyConverter extends PropertyConverter {
  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  @VisibleForTesting
  static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put(HivePropertyMeta.HIVE_TABLE_FORMAT, HiveConstants.FORMAT)
              .put(HivePropertyMeta.HIVE_TABLE_TOTAL_SIZE, HiveConstants.TOTAL_SIZE)
              .put(HivePropertyMeta.HIVE_TABLE_NUM_FILES, HiveConstants.NUM_FILES)
              .put(HivePropertyMeta.HIVE_TABLE_EXTERNAL, HiveConstants.EXTERNAL)
              .put(HivePropertyMeta.HIVE_TABLE_LOCATION, HiveConstants.LOCATION)
              .put(HivePropertyMeta.HIVE_TABLE_TYPE, HiveConstants.TABLE_TYPE)
              .put(HivePropertyMeta.HIVE_TABLE_INPUT_FORMAT, HiveConstants.INPUT_FORMAT)
              .put(HivePropertyMeta.HIVE_TABLE_OUTPUT_FORMAT, HiveConstants.OUTPUT_FORMAT)
              .put(HivePropertyMeta.HIVE_TABLE_SERDE_LIB, HiveConstants.SERDE_LIB)
              .put(HivePropertyMeta.HIVE_TABLE_SERDE_NAME, HiveConstants.SERDE_NAME)
              .build());

  @Override
  public TreeBidiMap<String, String> engineToGravitinoMapping() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }
}
