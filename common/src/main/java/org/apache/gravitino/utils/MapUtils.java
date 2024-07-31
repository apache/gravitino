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
    Map<String, String> configs = Maps.newHashMap();
    m.forEach(
        (k, v) -> {
          if (k.startsWith(prefix)) {
            String newKey = k.substring(prefix.length());
            configs.put(newKey, v);
          }
        });

    return Collections.unmodifiableMap(configs);
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
}
