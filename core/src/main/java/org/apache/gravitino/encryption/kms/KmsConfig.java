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
  static final String KMS_PROVIDERS = KMS_CONFIG_PREFIX + "providers";

  private static final String PROVIDERS = "providers";
  private static final String PROVIDER_PREFIX = "provider.";
  private static final String CLASS_NAME = "className";
  private static final Pattern PROVIDER_NAME_PATTERN = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]*");

  private final Map<String, ProviderConfig> providers;

  KmsConfig(Config config) {
    if (config == null) {
      throw new KmsConfigurationException("Gravitino configuration cannot be null");
    }

    Map<String, String> values = config.getConfigsWithPrefix(KMS_CONFIG_PREFIX);
    List<String> configuredProviders = parseProviders(values.get(PROVIDERS));
    this.providers = parseProviderConfigs(values, configuredProviders);
  }

  Map<String, ProviderConfig> providers() {
    return providers;
  }

  private static List<String> parseProviders(String value) {
    if (value == null || value.trim().isEmpty()) {
      return Collections.emptyList();
    }

    List<String> providers = new ArrayList<>();
    Set<String> uniqueProviders = new LinkedHashSet<>();
    for (String item : value.split(",", -1)) {
      String provider = item.trim();
      if (!PROVIDER_NAME_PATTERN.matcher(provider).matches()) {
        throw new KmsConfigurationException(
            "Invalid KMS provider name '%s' in %s", provider, KMS_PROVIDERS);
      }
      if (!uniqueProviders.add(provider)) {
        throw new KmsConfigurationException(
            "Duplicate KMS provider '%s' in %s", provider, KMS_PROVIDERS);
      }
      providers.add(provider);
    }
    return Collections.unmodifiableList(providers);
  }

  private static Map<String, ProviderConfig> parseProviderConfigs(
      Map<String, String> values, List<String> configuredProviders) {
    Map<String, Map<String, String>> propertiesByProvider = new LinkedHashMap<>();
    for (String provider : configuredProviders) {
      propertiesByProvider.put(provider, new LinkedHashMap<>());
    }

    for (Map.Entry<String, String> entry : values.entrySet()) {
      String key = entry.getKey();
      if (PROVIDERS.equals(key)) {
        continue;
      }
      if (!key.startsWith(PROVIDER_PREFIX)) {
        throw invalidConfigurationKey(key);
      }

      String providerAndProperty = key.substring(PROVIDER_PREFIX.length());
      int separator = providerAndProperty.indexOf('.');
      if (separator <= 0 || separator == providerAndProperty.length() - 1) {
        throw invalidConfigurationKey(key);
      }

      String provider = providerAndProperty.substring(0, separator);
      if (!PROVIDER_NAME_PATTERN.matcher(provider).matches()) {
        throw invalidConfigurationKey(key);
      }

      Map<String, String> properties = propertiesByProvider.get(provider);
      if (properties == null) {
        throw new KmsConfigurationException(
            "KMS configuration references unlisted provider '%s'", provider);
      }

      String property = providerAndProperty.substring(separator + 1);
      properties.put(property, entry.getValue());
    }

    Map<String, ProviderConfig> providerConfigs = new LinkedHashMap<>();

    for (String provider : configuredProviders) {
      String classNameKey = PROVIDER_PREFIX + provider + "." + CLASS_NAME;
      Map<String, String> properties = propertiesByProvider.get(provider);
      String className = properties.remove(CLASS_NAME);
      if (className == null || className.trim().isEmpty()) {
        throw new KmsConfigurationException(
            "KMS className property '%s%s' cannot be blank", KMS_CONFIG_PREFIX, classNameKey);
      }

      providerConfigs.put(provider, new ProviderConfig(className.trim(), properties));
    }

    return Collections.unmodifiableMap(providerConfigs);
  }

  private static KmsConfigurationException invalidConfigurationKey(String key) {
    return new KmsConfigurationException(
        "Invalid KMS configuration key '%s%s'", KMS_CONFIG_PREFIX, key);
  }

  static final class ProviderConfig {
    private final String className;
    private final Map<String, String> properties;

    private ProviderConfig(String className, Map<String, String> properties) {
      this.className = className;
      this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    String className() {
      return className;
    }

    Map<String, String> properties() {
      return properties;
    }
  }
}
