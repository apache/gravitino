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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.Config;

final class KmsConfig {

  static final String KMS_CONFIG_PREFIX = "gravitino.kms.";
  static final String KMS_SOURCES = KMS_CONFIG_PREFIX + "sources";

  private static final String SOURCES = "sources";
  private static final String SOURCE_PREFIX = "source.";
  private static final String API = "api";
  private static final Pattern SOURCE_NAME_PATTERN = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]*");
  private static final Pattern SOURCE_PROPERTY_PATTERN =
      Pattern.compile("source\\.([A-Za-z0-9][A-Za-z0-9_-]*)\\.(.+)");

  private final Map<String, SourceConfig> sources;

  KmsConfig(Config config) {
    if (config == null) {
      throw new KmsConfigurationException("Gravitino configuration cannot be null");
    }

    Map<String, String> values = config.getConfigsWithPrefix(KMS_CONFIG_PREFIX);
    List<String> configuredSources = parseSources(values.get(SOURCES));
    validateKeys(values, configuredSources);
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

  private static void validateKeys(Map<String, String> values, List<String> configuredSources) {
    Set<String> sourceSet = new LinkedHashSet<>(configuredSources);
    for (String key : values.keySet()) {
      if (SOURCES.equals(key)) {
        continue;
      }

      Matcher matcher = SOURCE_PROPERTY_PATTERN.matcher(key);
      if (!matcher.matches()) {
        throw new KmsConfigurationException(
            "Invalid KMS configuration key '%s%s'", KMS_CONFIG_PREFIX, key);
      }
      if (!sourceSet.contains(matcher.group(1))) {
        throw new KmsConfigurationException(
            "KMS configuration references unlisted source '%s'", matcher.group(1));
      }
    }
  }

  private static Map<String, SourceConfig> parseSourceConfigs(
      Map<String, String> values, List<String> configuredSources) {
    Map<String, SourceConfig> sourceConfigs = new LinkedHashMap<>();

    for (String source : configuredSources) {
      String propertyPrefix = SOURCE_PREFIX + source + ".";
      String apiKey = propertyPrefix + API;
      String apiValue = values.get(apiKey);
      if (apiValue == null || apiValue.trim().isEmpty()) {
        throw new KmsConfigurationException(
            "KMS API property '%s%s' cannot be blank", KMS_CONFIG_PREFIX, apiKey);
      }
      KmsApi api;
      try {
        api = KmsApi.fromWireValue(apiValue);
      } catch (IllegalArgumentException e) {
        throw new KmsConfigurationException(
            e, "Invalid KMS API property '%s%s': %s", KMS_CONFIG_PREFIX, apiKey, e.getMessage());
      }

      Map<String, String> properties = new LinkedHashMap<>();
      values.forEach(
          (key, value) -> {
            if (key.startsWith(propertyPrefix) && !apiKey.equals(key)) {
              String sourceProperty = key.substring(propertyPrefix.length());
              properties.put(sourceProperty, value);
            }
          });
      sourceConfigs.put(source, new SourceConfig(api, properties));
    }

    return Collections.unmodifiableMap(sourceConfigs);
  }

  static final class SourceConfig {
    private final KmsApi api;
    private final Map<String, String> properties;

    private SourceConfig(KmsApi api, Map<String, String> properties) {
      this.api = api;
      this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    KmsApi api() {
      return api;
    }

    Map<String, String> properties() {
      return properties;
    }
  }
}
