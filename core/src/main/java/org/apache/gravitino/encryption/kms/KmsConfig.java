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
package org.apache.gravitino.encryption.kms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.gravitino.Config;

final class KmsConfig {

  static final String KMS_CONFIG_PREFIX = "gravitino.kms.";
  static final String KMS_SOURCES = KMS_CONFIG_PREFIX + "sources";

  private static final String SOURCES = "sources";
  private static final String SOURCE_PREFIX = "source.";
  private static final String API = "api";
  private static final Pattern SOURCE_NAME_PATTERN = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]*");

  private final Map<String, SourceConfig> sources;

  KmsConfig(Config config) {
    if (config == null) {
      throw new KmsConfigurationException("Gravitino configuration cannot be null");
    }

    Map<String, String> values = config.getConfigsWithPrefix(KMS_CONFIG_PREFIX);
    List<String> configuredSources = parseSources(values.get(SOURCES));
    this.sources = parseSourceConfigs(values, configuredSources);
  }

  Map<String, SourceConfig> sources() {
    return sources;
  }

  private static List<String> parseSources(String value) {
    if (value == null || value.trim().isEmpty()) {
      return Collections.emptyList();
    }

    List<String> sources = new ArrayList<>();
    Set<String> uniqueSources = new LinkedHashSet<>();
    for (String item : value.split(",", -1)) {
      String source = item.trim();
      if (!SOURCE_NAME_PATTERN.matcher(source).matches()) {
        throw new KmsConfigurationException(
            "Invalid KMS source name '%s' in %s", source, KMS_SOURCES);
      }
      if (!uniqueSources.add(source)) {
        throw new KmsConfigurationException("Duplicate KMS source '%s' in %s", source, KMS_SOURCES);
      }
      sources.add(source);
    }
    return Collections.unmodifiableList(sources);
  }

  private static Map<String, SourceConfig> parseSourceConfigs(
      Map<String, String> values, List<String> configuredSources) {
    Map<String, Map<String, String>> propertiesBySource = new LinkedHashMap<>();
    for (String source : configuredSources) {
      propertiesBySource.put(source, new LinkedHashMap<>());
    }

    for (Map.Entry<String, String> entry : values.entrySet()) {
      String key = entry.getKey();
      if (SOURCES.equals(key)) {
        continue;
      }
      if (!key.startsWith(SOURCE_PREFIX)) {
        throw invalidConfigurationKey(key);
      }

      String sourceAndProperty = key.substring(SOURCE_PREFIX.length());
      int separator = sourceAndProperty.indexOf('.');
      if (separator <= 0 || separator == sourceAndProperty.length() - 1) {
        throw invalidConfigurationKey(key);
      }

      String source = sourceAndProperty.substring(0, separator);
      if (!SOURCE_NAME_PATTERN.matcher(source).matches()) {
        throw invalidConfigurationKey(key);
      }

      Map<String, String> properties = propertiesBySource.get(source);
      if (properties == null) {
        throw new KmsConfigurationException(
            "KMS configuration references unlisted source '%s'", source);
      }

      String property = sourceAndProperty.substring(separator + 1);
      properties.put(property, entry.getValue());
    }

    Map<String, SourceConfig> sourceConfigs = new LinkedHashMap<>();

    for (String source : configuredSources) {
      String apiKey = SOURCE_PREFIX + source + "." + API;
      Map<String, String> properties = propertiesBySource.get(source);
      String apiValue = properties.remove(API);
      if (apiValue == null || apiValue.trim().isEmpty()) {
        throw new KmsConfigurationException(
            "KMS API property '%s%s' cannot be blank", KMS_CONFIG_PREFIX, apiKey);
      }
      String api;
      try {
        api = KmsApiIdentifiers.requireValid(apiValue);
      } catch (IllegalArgumentException e) {
        throw new KmsConfigurationException(
            e, "Invalid KMS API property '%s%s': %s", KMS_CONFIG_PREFIX, apiKey, e.getMessage());
      }

      sourceConfigs.put(source, new SourceConfig(api, properties));
    }

    return Collections.unmodifiableMap(sourceConfigs);
  }

  private static KmsConfigurationException invalidConfigurationKey(String key) {
    return new KmsConfigurationException(
        "Invalid KMS configuration key '%s%s'", KMS_CONFIG_PREFIX, key);
  }

  static final class SourceConfig {
    private final String api;
    private final Map<String, String> properties;

    private SourceConfig(String api, Map<String, String> properties) {
      this.api = api;
      this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    String api() {
      return api;
    }

    Map<String, String> properties() {
      return properties;
    }
  }
}
