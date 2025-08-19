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

package org.apache.gravitino.utils;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Utility class for working with maps. */
public class MapUtils {
  private MapUtils() {}

  /**
   * Returns a map with all keys that start with the given prefix.
   *
   * @param m The map to filter.
   * @param prefix The prefix to filter by.
   * @return A map with all keys that start with the given prefix.
   */
  public static Map<String, String> getPrefixMap(Map<String, String> m, String prefix) {
    return getPrefixMap(m, prefix, false);
  }

  /**
   * Returns a map with all keys that start with the given prefix.
   *
   * @param m The map to filter.
   * @param prefix The prefix to filter by.
   * @param keepPrefix Whether to keep the prefix in the key or not.
   * @return A map with all keys that start with the given prefix.
   */
  public static Map<String, String> getPrefixMap(
      Map<String, String> m, String prefix, boolean keepPrefix) {
    if (m == null || prefix == null) {
      throw new IllegalArgumentException("Map and prefix cannot be null");
    }

    return m.entrySet().stream()
        .filter(entry -> entry.getKey() != null && entry.getKey().startsWith(prefix))
        .collect(
            Collectors.collectingAndThen(
                Collectors.toMap(
                    entry ->
                        keepPrefix ? entry.getKey() : entry.getKey().substring(prefix.length()),
                    Map.Entry::getValue),
                Collections::unmodifiableMap));
  }

  /**
   * Returns a new map containing entries whose keys do NOT start with the given prefix.
   *
   * @param m the original map
   * @param prefix the prefix to exclude
   * @return a filtered map without entries whose keys start with the prefix
   */
  public static Map<String, String> getMapWithoutPrefix(Map<String, String> m, String prefix) {
    if (m == null || prefix == null) {
      throw new IllegalArgumentException("Map and prefix cannot be null");
    }

    return m.entrySet().stream()
        .filter(entry -> entry.getKey() == null || !entry.getKey().startsWith(prefix))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Returns a map with all keys that match the predicate.
   *
   * @param m The map to filter.
   * @param predicate The predicate expression to filter the keys.
   * @return A map with all keys that match the predicate.
   */
  public static Map<String, String> getFilteredMap(Map<String, String> m, Predicate predicate) {
    Map<String, String> configs = Maps.newHashMap();
    m.forEach(
        (k, v) -> {
          if (predicate.test(k)) {
            configs.put(k, v);
          }
        });

    return Collections.unmodifiableMap(configs);
  }

  /**
   * Returns an unmodifiable map.
   *
   * @param m The map to make unmodifiable.
   * @return An unmodifiable map.
   */
  public static Map<String, String> unmodifiableMap(Map<String, String> m) {
    return Collections.unmodifiableMap(m);
  }

  /**
   * Extract an integer value from the properties map with provided key. If provided key not exist
   * in the properties map, it will return default value.
   *
   * @param properties input map
   * @param property provided key
   * @param defaultValue default value
   * @return integer value from the properties map with provided key.
   */
  public static int propertyAsInt(
      Map<String, String> properties, String property, int defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format(
                "%s in %s is invalid. %s", value, property, "The value must be an integer number"));
      }
    }
    return defaultValue;
  }

  /**
   * Extract a long value from the properties map with provided key. If provided key not exist in
   * the properties map, it will return default value.
   *
   * @param properties input map
   * @param property provided key
   * @param defaultValue default value
   * @return long value from the properties map with provided key.
   */
  public static long propertyAsLong(
      Map<String, String> properties, String property, long defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      try {
        return Long.parseLong(value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format(
                "%s in %s is invalid. %s", value, property, "The value must be a long number"));
      }
    }
    return defaultValue;
  }
}
