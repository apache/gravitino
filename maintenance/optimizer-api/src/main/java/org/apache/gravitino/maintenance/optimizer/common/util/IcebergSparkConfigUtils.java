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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;

/** Shared utilities for Iceberg Spark template configs and config validation. */
public final class IcebergSparkConfigUtils {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

  private static final String SPARK_SQL_EXTENSIONS_KEY = "spark.sql.extensions";
  private static final String SPARK_SQL_DEFAULT_CATALOG_KEY = "spark.sql.defaultCatalog";
  private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";
  private static final String CATALOG_TYPE_REST = "rest";
  private static final String CATALOG_TYPE_HIVE = "hive";
  private static final String CATALOG_TYPE_HADOOP = "hadoop";

  public static final String ICEBERG_SPARK_EXTENSIONS =
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";

  public static final String SPARK_MASTER_KEY = "spark.master";
  public static final String SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances";
  public static final String SPARK_EXECUTOR_CORES_KEY = "spark.executor.cores";
  public static final String SPARK_EXECUTOR_MEMORY_KEY = "spark.executor.memory";
  public static final String SPARK_DRIVER_MEMORY_KEY = "spark.driver.memory";

  public static final String PLACEHOLDER_SPARK_MASTER = "spark_master";
  public static final String PLACEHOLDER_SPARK_EXECUTOR_INSTANCES = "spark_executor_instances";
  public static final String PLACEHOLDER_SPARK_EXECUTOR_CORES = "spark_executor_cores";
  public static final String PLACEHOLDER_SPARK_EXECUTOR_MEMORY = "spark_executor_memory";
  public static final String PLACEHOLDER_SPARK_DRIVER_MEMORY = "spark_driver_memory";
  public static final String PLACEHOLDER_CATALOG_TYPE = "catalog_type";
  public static final String PLACEHOLDER_CATALOG_URI = "catalog_uri";
  public static final String PLACEHOLDER_WAREHOUSE_LOCATION = "warehouse_location";

  private static final String DEFAULT_SPARK_EXECUTOR_INSTANCES = "1";
  private static final String DEFAULT_SPARK_EXECUTOR_CORES = "1";
  private static final String DEFAULT_SPARK_EXECUTOR_MEMORY = "1g";
  private static final String DEFAULT_SPARK_DRIVER_MEMORY = "1g";

  /**
   * Nested Spark resource keys → flat template placeholder names (update-stats / rewrite
   * templates).
   */
  public static final Map<String, String> SPARK_KEY_TO_PLACEHOLDER =
      ImmutableMap.of(
          SPARK_MASTER_KEY,
          PLACEHOLDER_SPARK_MASTER,
          SPARK_EXECUTOR_INSTANCES_KEY,
          PLACEHOLDER_SPARK_EXECUTOR_INSTANCES,
          SPARK_EXECUTOR_CORES_KEY,
          PLACEHOLDER_SPARK_EXECUTOR_CORES,
          SPARK_EXECUTOR_MEMORY_KEY,
          PLACEHOLDER_SPARK_EXECUTOR_MEMORY,
          SPARK_DRIVER_MEMORY_KEY,
          PLACEHOLDER_SPARK_DRIVER_MEMORY);

  private IcebergSparkConfigUtils() {}

  /**
   * Builds the shared Iceberg template keys that adapters must submit for {@code runJob}.
   *
   * <p>Copies {@code catalog_name}, expands flat {@code spark_*} / catalog connection placeholders
   * for {@code withConfigs(buildTemplateSparkConfigs())}, overlays those flats onto nested {@code
   * spark_conf}, and emits {@code spark_conf} JSON. Callers should put only template parameters on
   * top of this map and must not forward the raw submitter config wholesale.
   *
   * @param catalogName Spark catalog alias for this job
   * @param submitterConfigs optimizer {@code jobSubmitterConfig.*} map (may be null/empty)
   * @return mutable map with {@code catalog_name}, resource/catalog placeholders, and {@code
   *     spark_conf} when non-empty
   */
  public static Map<String, String> buildIcebergTemplateSubmitterParams(
      String catalogName, Map<String, String> submitterConfigs) {
    Preconditions.checkArgument(StringUtils.isNotBlank(catalogName), "catalogName is blank");
    Map<String, String> flat = submitterConfigs == null ? Collections.emptyMap() : submitterConfigs;
    Map<String, String> sparkConf =
        new LinkedHashMap<>(parseFlatJsonMap(flat.get("spark_conf"), "spark-conf"));
    overlayFlatResourceAndCatalogKeys(sparkConf, flat, catalogName);

    Map<String, String> params = new LinkedHashMap<>();
    params.put("catalog_name", catalogName);
    params.putAll(resolveTemplateSparkPlaceholders(catalogName, sparkConf, flat));
    if (!sparkConf.isEmpty()) {
      params.put("spark_conf", toSparkConfJson(sparkConf));
    }
    return params;
  }

  /**
   * Serializes a flat Spark conf map to JSON for the {@code spark_conf} template parameter.
   *
   * @param sparkConf flat Spark configuration entries
   * @return canonical JSON object text
   */
  public static String toSparkConfJson(Map<String, String> sparkConf) {
    Preconditions.checkNotNull(sparkConf, "sparkConf");
    try {
      return MAPPER.writeValueAsString(new TreeMap<>(sparkConf));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize spark_conf", e);
    }
  }

  /**
   * Resolves flat SparkJobTemplate placeholders. Priority: {@code flatSubmitterConfigs}, then
   * nested {@code sparkConfigs}, then defaults. Nested {@code spark.master} is ignored for master
   * (only flat {@code spark_master} applies).
   *
   * @param catalogName Spark catalog alias for catalog_* placeholders
   * @param sparkConfigs nested spark_conf map (may be empty)
   * @param flatSubmitterConfigs top-level flat keys (may be empty)
   * @return placeholder name → value for template expansion
   */
  public static Map<String, String> resolveTemplateSparkPlaceholders(
      String catalogName,
      Map<String, String> sparkConfigs,
      Map<String, String> flatSubmitterConfigs) {
    Preconditions.checkArgument(StringUtils.isNotBlank(catalogName), "catalogName is blank");
    Map<String, String> spark = sparkConfigs == null ? Collections.emptyMap() : sparkConfigs;
    Map<String, String> flat =
        flatSubmitterConfigs == null ? Collections.emptyMap() : flatSubmitterConfigs;
    Map<String, String> placeholders = new LinkedHashMap<>();
    String catalogPrefix = SPARK_SQL_CATALOG_PREFIX + catalogName;

    // Empty master leaves it to the job executor; nested spark.master is not used (flat only).
    placeholders.put(
        PLACEHOLDER_SPARK_MASTER,
        firstNonBlank(flat.get(PLACEHOLDER_SPARK_MASTER), flat.get(SPARK_MASTER_KEY)));
    placeholders.put(
        PLACEHOLDER_SPARK_EXECUTOR_INSTANCES,
        firstNonBlank(
            flat.get(PLACEHOLDER_SPARK_EXECUTOR_INSTANCES),
            flat.get(SPARK_EXECUTOR_INSTANCES_KEY),
            spark.get(SPARK_EXECUTOR_INSTANCES_KEY),
            DEFAULT_SPARK_EXECUTOR_INSTANCES));
    placeholders.put(
        PLACEHOLDER_SPARK_EXECUTOR_CORES,
        firstNonBlank(
            flat.get(PLACEHOLDER_SPARK_EXECUTOR_CORES),
            flat.get(SPARK_EXECUTOR_CORES_KEY),
            spark.get(SPARK_EXECUTOR_CORES_KEY),
            DEFAULT_SPARK_EXECUTOR_CORES));
    placeholders.put(
        PLACEHOLDER_SPARK_EXECUTOR_MEMORY,
        firstNonBlank(
            flat.get(PLACEHOLDER_SPARK_EXECUTOR_MEMORY),
            flat.get(SPARK_EXECUTOR_MEMORY_KEY),
            spark.get(SPARK_EXECUTOR_MEMORY_KEY),
            DEFAULT_SPARK_EXECUTOR_MEMORY));
    placeholders.put(
        PLACEHOLDER_SPARK_DRIVER_MEMORY,
        firstNonBlank(
            flat.get(PLACEHOLDER_SPARK_DRIVER_MEMORY),
            flat.get(SPARK_DRIVER_MEMORY_KEY),
            spark.get(SPARK_DRIVER_MEMORY_KEY),
            DEFAULT_SPARK_DRIVER_MEMORY));
    placeholders.put(
        PLACEHOLDER_CATALOG_TYPE,
        firstNonBlank(flat.get(PLACEHOLDER_CATALOG_TYPE), spark.get(catalogPrefix + ".type")));
    placeholders.put(
        PLACEHOLDER_CATALOG_URI,
        firstNonBlank(flat.get(PLACEHOLDER_CATALOG_URI), spark.get(catalogPrefix + ".uri"), ""));
    placeholders.put(
        PLACEHOLDER_WAREHOUSE_LOCATION,
        firstNonBlank(
            flat.get(PLACEHOLDER_WAREHOUSE_LOCATION), spark.get(catalogPrefix + ".warehouse"), ""));
    return placeholders;
  }

  /**
   * Applies top-level resource / catalog keys from {@code jobOptions} onto nested {@code sparkConf}
   * using {@link #resolveTemplateSparkPlaceholders} (flat wins). Only writes keys that {@code
   * jobOptions} set; does not inject template defaults into {@code sparkConf}.
   *
   * @param sparkConf mutable nested spark conf
   * @param jobOptions policy / submitter map that may carry placeholders or spark.* resource keys
   * @param catalogName Spark catalog alias
   */
  public static void overlayFlatResourceAndCatalogKeys(
      Map<String, String> sparkConf, Map<String, String> jobOptions, String catalogName) {
    Preconditions.checkNotNull(sparkConf, "sparkConf");
    Preconditions.checkArgument(StringUtils.isNotBlank(catalogName), "catalogName is blank");
    if (jobOptions == null || jobOptions.isEmpty()) {
      return;
    }
    Map<String, String> resolved =
        resolveTemplateSparkPlaceholders(catalogName, sparkConf, jobOptions);
    String catalogPrefix = SPARK_SQL_CATALOG_PREFIX + catalogName;
    if (hasResourceOverride(
        jobOptions, PLACEHOLDER_SPARK_EXECUTOR_INSTANCES, SPARK_EXECUTOR_INSTANCES_KEY)) {
      sparkConf.put(
          SPARK_EXECUTOR_INSTANCES_KEY, resolved.get(PLACEHOLDER_SPARK_EXECUTOR_INSTANCES));
    }
    if (hasResourceOverride(
        jobOptions, PLACEHOLDER_SPARK_EXECUTOR_CORES, SPARK_EXECUTOR_CORES_KEY)) {
      sparkConf.put(SPARK_EXECUTOR_CORES_KEY, resolved.get(PLACEHOLDER_SPARK_EXECUTOR_CORES));
    }
    if (hasResourceOverride(
        jobOptions, PLACEHOLDER_SPARK_EXECUTOR_MEMORY, SPARK_EXECUTOR_MEMORY_KEY)) {
      sparkConf.put(SPARK_EXECUTOR_MEMORY_KEY, resolved.get(PLACEHOLDER_SPARK_EXECUTOR_MEMORY));
    }
    if (hasResourceOverride(jobOptions, PLACEHOLDER_SPARK_DRIVER_MEMORY, SPARK_DRIVER_MEMORY_KEY)) {
      sparkConf.put(SPARK_DRIVER_MEMORY_KEY, resolved.get(PLACEHOLDER_SPARK_DRIVER_MEMORY));
    }
    if (hasResourceOverride(jobOptions, PLACEHOLDER_SPARK_MASTER, SPARK_MASTER_KEY)) {
      sparkConf.put(SPARK_MASTER_KEY, resolved.get(PLACEHOLDER_SPARK_MASTER));
    }
    if (StringUtils.isNotBlank(jobOptions.get(PLACEHOLDER_CATALOG_TYPE))) {
      sparkConf.put(catalogPrefix + ".type", resolved.get(PLACEHOLDER_CATALOG_TYPE));
    }
    if (StringUtils.isNotBlank(jobOptions.get(PLACEHOLDER_CATALOG_URI))) {
      sparkConf.put(catalogPrefix + ".uri", resolved.get(PLACEHOLDER_CATALOG_URI));
    }
    if (StringUtils.isNotBlank(jobOptions.get(PLACEHOLDER_WAREHOUSE_LOCATION))) {
      sparkConf.put(catalogPrefix + ".warehouse", resolved.get(PLACEHOLDER_WAREHOUSE_LOCATION));
    }
  }

  private static boolean hasResourceOverride(
      Map<String, String> jobOptions, String placeholderKey, String sparkKey) {
    return StringUtils.isNotBlank(jobOptions.get(placeholderKey))
        || StringUtils.isNotBlank(jobOptions.get(sparkKey));
  }

  /**
   * @return true when {@code key} is a known flat resource/catalog placeholder or mapped spark.*
   */
  public static boolean isFlatResourceOrCatalogKey(String key) {
    if (key == null) {
      return false;
    }
    if (PLACEHOLDER_SPARK_MASTER.equals(key)
        || PLACEHOLDER_SPARK_EXECUTOR_INSTANCES.equals(key)
        || PLACEHOLDER_SPARK_EXECUTOR_CORES.equals(key)
        || PLACEHOLDER_SPARK_EXECUTOR_MEMORY.equals(key)
        || PLACEHOLDER_SPARK_DRIVER_MEMORY.equals(key)
        || PLACEHOLDER_CATALOG_TYPE.equals(key)
        || PLACEHOLDER_CATALOG_URI.equals(key)
        || PLACEHOLDER_WAREHOUSE_LOCATION.equals(key)) {
      return true;
    }
    return SPARK_KEY_TO_PLACEHOLDER.containsKey(key);
  }

  /**
   * @param candidates values in priority order
   * @return first non-blank trimmed candidate, or empty string
   */
  public static String firstNonBlank(String... candidates) {
    if (candidates == null) {
      return "";
    }
    for (String candidate : candidates) {
      if (StringUtils.isNotBlank(candidate)) {
        return candidate.trim();
      }
    }
    return "";
  }

  /** Build default Spark template configs for Iceberg jobs. */
  public static Map<String, String> buildTemplateSparkConfigs() {
    Map<String, String> configs = new HashMap<>();
    // An empty value leaves the Spark configuration unset: spark-submit then runs the job with
    // its default master, local[*], and the k8s job executor lets the operator decide it.
    configs.put("spark.master", "{{spark_master:-}}");
    configs.put("spark.executor.instances", "{{spark_executor_instances:-1}}");
    configs.put("spark.executor.cores", "{{spark_executor_cores:-1}}");
    configs.put("spark.executor.memory", "{{spark_executor_memory:-1g}}");
    configs.put("spark.driver.memory", "{{spark_driver_memory:-1g}}");
    // The catalog connection has no default that fits every catalog, so it stays required.
    configs.put(
        SPARK_SQL_CATALOG_PREFIX + "{{catalog_name}}", "org.apache.iceberg.spark.SparkCatalog");
    configs.put(SPARK_SQL_CATALOG_PREFIX + "{{catalog_name}}.type", "{{catalog_type}}");
    configs.put(SPARK_SQL_CATALOG_PREFIX + "{{catalog_name}}.uri", "{{catalog_uri}}");
    configs.put(SPARK_SQL_CATALOG_PREFIX + "{{catalog_name}}.warehouse", "{{warehouse_location}}");
    // Bare years()/months()/days() in rewrite where clauses resolve via the current catalog's
    // FunctionCatalog. Keep the session catalog on the target Iceberg catalog, not spark_catalog.
    configs.put(SPARK_SQL_DEFAULT_CATALOG_KEY, "{{catalog_name}}");
    configs.put(SPARK_SQL_EXTENSIONS_KEY, ICEBERG_SPARK_EXTENSIONS);
    return Collections.unmodifiableMap(configs);
  }

  /**
   * Fills missing Iceberg SparkCatalog impl and extensions when nested catalog connection keys are
   * present, and sets {@code spark.sql.defaultCatalog} from the job's {@code catalogName}.
   *
   * <p>TMS {@code spark_conf} may only carry {@code .type}/{@code .uri}/{@code .warehouse}. Without
   * {@code spark.sql.defaultCatalog}, {@code CALL catalog.system.rewrite_data_files(...)} still
   * finds the procedure, but bare {@code years(ts)} in {@code where} resolves against {@code
   * spark_catalog} and fails. Callers must not put {@code spark.sql.defaultCatalog} in {@code
   * spark_conf}; the job owns that key via {@code catalog_name}. Template configs may still set
   * {@code {{catalog_name}}} separately from the {@code spark_conf} JSON overlay.
   *
   * @param sparkConfigs mutable Spark configs from {@code spark_conf} applied to the session
   *     builder
   * @param catalogName Iceberg catalog for this job ({@code catalog_name} / {@code --catalog})
   */
  public static void ensureIcebergCatalogConfigs(
      Map<String, String> sparkConfigs, String catalogName) {
    Preconditions.checkNotNull(sparkConfigs, "sparkConfigs");
    Preconditions.checkArgument(StringUtils.isNotBlank(catalogName), "catalogName is blank");
    Preconditions.checkArgument(
        !sparkConfigs.containsKey(SPARK_SQL_DEFAULT_CATALOG_KEY),
        "Do not set spark.sql.defaultCatalog in spark_conf; the job sets it from catalog_name (%s)",
        catalogName);

    Set<String> catalogs = new HashSet<>();
    for (String key : sparkConfigs.keySet()) {
      if (!key.startsWith(SPARK_SQL_CATALOG_PREFIX)) {
        continue;
      }
      String rest = key.substring(SPARK_SQL_CATALOG_PREFIX.length());
      int dot = rest.indexOf('.');
      if (dot <= 0) {
        continue;
      }
      String prop = rest.substring(dot + 1);
      if ("type".equals(prop) || "uri".equals(prop) || "warehouse".equals(prop)) {
        catalogs.add(rest.substring(0, dot));
      }
    }
    for (String catalog : catalogs) {
      sparkConfigs.putIfAbsent(
          SPARK_SQL_CATALOG_PREFIX + catalog, "org.apache.iceberg.spark.SparkCatalog");
    }
    sparkConfigs.put(SPARK_SQL_DEFAULT_CATALOG_KEY, catalogName);
    sparkConfigs.putIfAbsent(SPARK_SQL_EXTENSIONS_KEY, ICEBERG_SPARK_EXTENSIONS);
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
