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

package org.apache.gravitino.spark.connector.jdbc.doris;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConverter;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Converts catalog-managed properties for the specialized Doris read path. */
final class DorisPropertiesConverter35 implements PropertiesConverter {

  private static final DorisPropertiesConverter35 INSTANCE = new DorisPropertiesConverter35();
  private static final String NATIVE_PREFIX = "doris.";
  private static final Set<String> NATIVE_OPTIONS =
      ImmutableSet.of(
          "doris.fenodes",
          "doris.read.mode",
          "doris.read.arrow-flight-sql.port",
          "doris.request.tablet.size",
          "doris.batch.size");
  private static final Set<String> PROTECTED_OPTIONS =
      ImmutableSet.of(
          "url",
          "user",
          "password",
          "driver",
          "jdbc-url",
          "jdbc-user",
          "jdbc-password",
          "jdbc-driver",
          "dbtable",
          "dbname",
          "createdatabaseifnotexist",
          "query",
          "preparequery",
          "sessioninitstatement");

  private DorisPropertiesConverter35() {}

  static DorisPropertiesConverter35 getInstance() {
    return INSTANCE;
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(
      CaseInsensitiveStringMap options, Map<String, String> properties) {
    validateReadOptions(options);
    if (properties != null) {
      for (String key : properties.keySet()) {
        if (key.startsWith(SPARK_PROPERTY_PREFIX)) {
          validateReadOption(key.substring(SPARK_PROPERTY_PREFIX.length()));
        }
      }
    }
    Map<String, String> converted =
        new HashMap<>(
            JdbcPropertiesConverter.getInstance().toSparkCatalogProperties(null, properties));
    if (!nativeOptions(properties).isEmpty()) {
      // Native reader options are never passed to Spark's JDBC catalog or its driver.
      converted.keySet().removeIf(key -> key.toLowerCase(Locale.ROOT).startsWith(NATIVE_PREFIX));
    }
    // Specialized mode accepts only the credential vended by Gravitino. The catalog-level
    // jdbc-user and jdbc-password remain available to the generic JDBC path.
    converted.remove("user");
    converted.remove("password");
    if (options != null) {
      for (Map.Entry<String, String> option : options.asCaseSensitiveMap().entrySet()) {
        converted.keySet().removeIf(key -> key.equalsIgnoreCase(option.getKey()));
        converted.put(option.getKey(), option.getValue());
      }
    }
    return converted;
  }

  static void validateReadOptions(CaseInsensitiveStringMap options) {
    if (options == null) {
      return;
    }
    for (String key : options.keySet()) {
      validateReadOption(key);
    }
  }

  static Map<String, String> nativeOptions(Map<String, String> properties) {
    Map<String, String> nativeOptions = new HashMap<>();
    List<String> otherDorisOptions = new ArrayList<>();
    if (properties == null) {
      return nativeOptions;
    }
    properties.forEach(
        (key, value) -> {
          String lower = key.toLowerCase(Locale.ROOT);
          String prefix = SPARK_PROPERTY_PREFIX + NATIVE_PREFIX;
          if (lower.startsWith(prefix)) {
            String option = lower.substring(SPARK_PROPERTY_PREFIX.length());
            if (!NATIVE_OPTIONS.contains(option)) {
              otherDorisOptions.add(option);
            } else if (value == null
                || value.trim().isEmpty()
                || nativeOptions.put(option, value) != null) {
              throw new IllegalArgumentException(
                  "Unsupported or duplicate Doris native option: " + option);
            }
          }
        });
    if (!nativeOptions.isEmpty() && !otherDorisOptions.isEmpty()) {
      throw new IllegalArgumentException(
          "Unsupported Doris native option: " + otherDorisOptions.get(0));
    }
    if (!nativeOptions.isEmpty()
        && (!nativeOptions.containsKey("doris.fenodes")
            || nativeOptions.get("doris.fenodes").trim().isEmpty())) {
      throw new IllegalArgumentException(
          "Doris native reads require catalog-managed doris.fenodes");
    }
    String mode = nativeOptions.get("doris.read.mode");
    if (mode != null && !"arrow".equalsIgnoreCase(mode) && !"thrift".equalsIgnoreCase(mode)) {
      throw new IllegalArgumentException("Doris native read mode must be arrow or thrift");
    }
    validatePositiveInteger(nativeOptions, "doris.request.tablet.size", Integer.MAX_VALUE);
    validatePositiveInteger(nativeOptions, "doris.batch.size", 65535);
    validatePositiveInteger(nativeOptions, "doris.read.arrow-flight-sql.port", 65535);
    return nativeOptions;
  }

  static void validateNativeReadOptions(CaseInsensitiveStringMap options) {
    validateReadOptions(options);
    if (options == null) {
      return;
    }
    for (String key : options.keySet()) {
      String lower = key.toLowerCase(Locale.ROOT).replace('_', '.');
      if (lower.startsWith(NATIVE_PREFIX)) {
        throw new IllegalArgumentException(
            "Doris native read options must be catalog-managed: " + key);
      }
    }
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(Map<String, String> properties) {
    return toSparkCatalogProperties(null, properties);
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  private static void validateReadOption(String key) {
    if (PROTECTED_OPTIONS.contains(key.toLowerCase(Locale.ROOT))) {
      throw new IllegalArgumentException("Protected Doris Spark option is not allowed: " + key);
    }
  }

  private static void validatePositiveInteger(
      Map<String, String> options, String key, int maximum) {
    String value = options.get(key);
    if (value == null) {
      return;
    }
    try {
      int number = Integer.parseInt(value);
      if (number <= 0 || number > maximum) {
        throw new NumberFormatException("value out of range");
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be between 1 and " + maximum);
    }
  }
}
