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
 *
 */
package org.apache.gravitino.lance.common.utils;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_STORAGE_OPTIONS_PREFIX;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** Utility methods for Lance storage properties. */
public final class LancePropertiesUtils {

  private LancePropertiesUtils() {
    // Utility class.
  }

  /**
   * Extracts Lance storage options from a property map.
   *
   * @param tableProperties the source properties
   * @return the Lance storage options without the `lance.storage.` prefix
   */
  public static Map<String, String> getLanceStorageOptions(Map<String, String> tableProperties) {
    if (tableProperties == null || tableProperties.isEmpty()) {
      return Map.of();
    }

    return tableProperties.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(LANCE_STORAGE_OPTIONS_PREFIX))
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().substring(LANCE_STORAGE_OPTIONS_PREFIX.length()),
                Map.Entry::getValue,
                (left, right) -> right,
                LinkedHashMap::new));
  }

  /**
   * Resolves the effective Lance storage options using table properties first and catalog
   * properties as defaults.
   *
   * @param catalogProperties the catalog properties
   * @param tableProperties the table properties
   * @return the effective storage options
   */
  public static Map<String, String> resolveLanceStorageOptions(
      Map<String, String> catalogProperties, Map<String, String> tableProperties) {
    Map<String, String> effectiveStorageOptions =
        new LinkedHashMap<>(getLanceStorageOptions(catalogProperties));
    effectiveStorageOptions.putAll(getLanceStorageOptions(tableProperties));
    return effectiveStorageOptions;
  }
}
