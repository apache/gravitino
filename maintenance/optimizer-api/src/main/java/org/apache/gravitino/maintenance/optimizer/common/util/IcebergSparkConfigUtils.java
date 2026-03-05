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

package org.apache.gravitino.maintenance.optimizer.common.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** Shared utilities for Iceberg Spark template configs and config validation. */
public final class IcebergSparkConfigUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String SPARK_SQL_EXTENSIONS_KEY = "spark.sql.extensions";
  private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";
  private static final String CATALOG_TYPE_REST = "rest";
  private static final String CATALOG_TYPE_HIVE = "hive";
  private static final String CATALOG_TYPE_HADOOP = "hadoop";

  public static final String ICEBERG_SPARK_EXTENSIONS =
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";

  private IcebergSparkConfigUtils() {}

  /** Build default Spark template configs for Iceberg jobs. */
  public static Map<String, String> buildTemplateSparkConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put("spark.master", "{{spark_master}}");
    configs.put("spark.executor.instances", "{{spark_executor_instances}}");
    configs.put("spark.executor.cores", "{{spark_executor_cores}}");
    configs.put("spark.executor.memory", "{{spark_executor_memory}}");
    configs.put("spark.driver.memory", "{{spark_driver_memory}}");
    configs.put(
        SPARK_SQL_CATALOG_PREFIX + "{{catalog_name}}", "org.apache.iceberg.spark.SparkCatalog");
    configs.put(SPARK_SQL_CATALOG_PREFIX + "{{catalog_name}}.type", "{{catalog_type}}");
    configs.put(SPARK_SQL_CATALOG_PREFIX + "{{catalog_name}}.uri", "{{catalog_uri}}");
    configs.put(SPARK_SQL_CATALOG_PREFIX + "{{catalog_name}}.warehouse", "{{warehouse_location}}");
    configs.put(SPARK_SQL_EXTENSIONS_KEY, ICEBERG_SPARK_EXTENSIONS);
    return Collections.unmodifiableMap(configs);
  }

  /**
   * Parse a flat JSON map from CLI/config option.
   *
   * <p>Nested objects/arrays are rejected.
   */
  public static Map<String, String> parseFlatJsonMap(String json, String optionName) {
    if (StringUtils.isBlank(json)) {
      return ImmutableMap.of();
    }
    try {
      Map<String, Object> parsedMap =
          MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
      Map<String, String> result = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry : parsedMap.entrySet()) {
        Object value = entry.getValue();
        Preconditions.checkArgument(
            !(value instanceof Map || value instanceof List),
            "Option --%s must be a flat key-value JSON map, but key '%s' has non-scalar value",
            optionName,
            entry.getKey());
        result.put(entry.getKey(), value == null ? "" : value.toString());
      }
      return ImmutableMap.copyOf(result);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT, "Option --%s is not valid JSON: %s", optionName, e.getMessage()),
          e);
    }
  }

  /** Validate required Spark/Iceberg catalog configs for one table identifier. */
  public static void validateSparkConfigsForCatalog(
      Map<String, String> sparkConfigs, String catalogName, String tableIdentifier) {
    Preconditions.checkArgument(
        sparkConfigs != null && !sparkConfigs.isEmpty(),
        "Missing spark config. Set --spark-conf or "
            + "gravitino.optimizer.jobSubmitterConfig.spark_conf in the config file");
    Preconditions.checkArgument(StringUtils.isNotBlank(catalogName), "catalogName is blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(tableIdentifier), "tableIdentifier is blank");

    String extensions = StringUtils.trimToNull(sparkConfigs.get(SPARK_SQL_EXTENSIONS_KEY));
    Preconditions.checkArgument(
        StringUtils.isNotBlank(extensions),
        "Spark config must contain key '%s' and include '%s'",
        SPARK_SQL_EXTENSIONS_KEY,
        ICEBERG_SPARK_EXTENSIONS);
    Preconditions.checkArgument(
        extensions.contains(ICEBERG_SPARK_EXTENSIONS),
        "Spark config key '%s' must include '%s'",
        SPARK_SQL_EXTENSIONS_KEY,
        ICEBERG_SPARK_EXTENSIONS);

    String catalogPrefix = SPARK_SQL_CATALOG_PREFIX + catalogName;
    Preconditions.checkArgument(
        StringUtils.isNotBlank(sparkConfigs.get(catalogPrefix)),
        "Spark config must contain key '%s' for identifier '%s'",
        catalogPrefix,
        tableIdentifier);

    String typeKey = catalogPrefix + ".type";
    String catalogType = StringUtils.trimToNull(sparkConfigs.get(typeKey));
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogType),
        "Spark config must contain key '%s' for identifier '%s'",
        typeKey,
        tableIdentifier);

    String normalizedCatalogType = catalogType.toLowerCase(Locale.ROOT);
    if (CATALOG_TYPE_REST.equals(normalizedCatalogType)
        || CATALOG_TYPE_HIVE.equals(normalizedCatalogType)) {
      String uriKey = catalogPrefix + ".uri";
      Preconditions.checkArgument(
          StringUtils.isNotBlank(sparkConfigs.get(uriKey)),
          "Spark config must contain key '%s' when catalog type is '%s' for identifier '%s'",
          uriKey,
          normalizedCatalogType,
          tableIdentifier);
    } else if (CATALOG_TYPE_HADOOP.equals(normalizedCatalogType)) {
      String warehouseKey = catalogPrefix + ".warehouse";
      Preconditions.checkArgument(
          StringUtils.isNotBlank(sparkConfigs.get(warehouseKey)),
          "Spark config must contain key '%s' when catalog type is '%s' for identifier '%s'",
          warehouseKey,
          normalizedCatalogType,
          tableIdentifier);
    }
  }
}
