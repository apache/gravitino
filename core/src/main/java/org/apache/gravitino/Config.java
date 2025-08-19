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
package org.apache.gravitino;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Config class is responsible for managing configuration settings. */
public abstract class Config {

  private static final Logger LOG = LoggerFactory.getLogger(Config.class);

  private static final String CONFIG_PREPEND = "gravitino.";

  private final ConcurrentMap<String, String> configMap;

  private final Map<String, DeprecatedConfig> deprecatedConfigMap;
  // Constant Array to hold all deprecated configuration keys, when a configuration is deprecated,
  // we should add it here.
  private final DeprecatedConfig[] deprecatedConfigs = {
    new DeprecatedConfig(
        "gravitino.authenticator", "0.6.0", "Please use gravitino.authenticators instead.")
  };

  /**
   * Constructs a Config instance.
   *
   * @param loadDefaults Set to true if default configurations should be loaded.
   */
  protected Config(boolean loadDefaults) {
    deprecatedConfigMap = new HashMap<>();
    for (DeprecatedConfig config : deprecatedConfigs) {
      deprecatedConfigMap.put(config.key, config);
    }

    configMap = new ConcurrentHashMap<>();
    if (loadDefaults) {
      loadFromProperties(System.getProperties());
    }
  }

  /** Constructs a Config instance and loads default configurations. */
  protected Config() {
    this(true);
  }

  /**
   * Loads configurations from a properties file.
   *
   * @param name The name of the properties file.
   * @return The Config instance.
   * @throws Exception If there's an issue loading the properties.
   */
  public Config loadFromFile(String name) throws Exception {
    String confDir =
        Optional.ofNullable(System.getenv("GRAVITINO_CONF_DIR"))
            .orElse(
                Optional.ofNullable(System.getenv("GRAVITINO_HOME"))
                    .map(s -> s + File.separator + "conf")
                    .orElse(null));

    if (confDir == null) {
      throw new IllegalArgumentException("GRAVITINO_CONF_DIR or GRAVITINO_HOME not set");
    }

    File confFile = new File(confDir + File.separator + name);
    if (!confFile.exists()) {
      throw new IllegalArgumentException(
          "Config file " + confFile.getAbsolutePath() + " not found");
    }

    Properties properties = loadPropertiesFromFile(confFile);
    loadFromProperties(properties);

    return this;
  }

  /**
   * Gets the value of a configuration entry.
   *
   * @param entry The configuration entry to retrieve.
   * @param <T> The type of the configuration value.
   * @return The value of the configuration entry.
   * @throws NoSuchElementException If the configuration entry is not found.
   */
  public <T> T get(ConfigEntry<T> entry) throws NoSuchElementException {
    return entry.readFrom(configMap);
  }

  /**
   * Retrieves the raw string value associated with the specified configuration key.
   *
   * @param key The configuration key for which the raw string value is requested.
   * @return The raw string value associated with the given configuration key, or null if the key is
   *     not found.
   */
  public String getRawString(String key) {
    return configMap.get(key);
  }

  /**
   * Retrieves the raw string value associated with the specified configuration key, providing a
   * default value if the key is not found.
   *
   * @param key The configuration key for which the raw string value is requested.
   * @param defaultValue The default value to be returned if the key is not found.
   * @return The raw string value associated with the given configuration key, or the provided
   *     default value if the key is not found.
   */
  public String getRawString(String key, String defaultValue) {
    return configMap.getOrDefault(key, defaultValue);
  }

  /**
   * Retrieves a map containing configuration entries that have keys with the specified prefix.
   *
   * @param prefix The prefix that configuration keys should start with.
   * @return An unmodifiable map containing configuration entries with keys matching the prefix.
   */
  public Map<String, String> getConfigsWithPrefix(String prefix) {
    return MapUtils.getPrefixMap(configMap, prefix);
  }

  /**
   * Retrieves a map containing all configuration entries.
   *
   * @return An unmodifiable map containing all configuration entries.
   */
  public Map<String, String> getAllConfig() {
    return MapUtils.unmodifiableMap(configMap);
  }

  /**
   * Sets the value of a configuration entry.
   *
   * @param entry The configuration entry for which the value needs to be set.
   * @param value The new value to be assigned to the configuration entry.
   * @param <T> The type of the configuration value.
   */
  public <T> void set(ConfigEntry<T> entry, T value) {
    if (entry.isDeprecated()
        && deprecatedConfigMap.containsKey(entry.getKey())
        && LOG.isWarnEnabled()) {
      LOG.warn(
          "Config {} is deprecated since Gravitino {}. {}",
          entry.getKey(),
          deprecatedConfigMap.get(entry.getKey()).version,
          deprecatedConfigMap.get(entry.getKey()).deprecationMessage);
    }

    if (value == null && LOG.isWarnEnabled()) {
      LOG.warn("Config {} value to set is null, ignore setting to Config.", entry.getKey());
      return;
    }

    entry.writeTo(configMap, value);
  }

  /**
   * Loads configurations from a map.
   *
   * @param map The map containing configuration key-value pairs.
   * @param predicate The keys only match the predicate will be loaded to configMap
   */
  public void loadFromMap(Map<String, String> map, Predicate<String> predicate) {
    map.forEach(
        (k, v) -> {
          String trimmedK = k.trim();
          String trimmedV = v.trim();
          if (!trimmedK.isEmpty()) {
            if (predicate.test(trimmedK)) {
              configMap.put(trimmedK, trimmedV);
            }
          }
        });
  }

  /**
   * Loads configurations from properties.
   *
   * @param properties The properties object containing configuration key-value pairs.
   */
  public void loadFromProperties(Properties properties) {
    loadFromMap(
        Maps.fromProperties(properties),
        k -> {
          if (k.startsWith(CONFIG_PREPEND)) {
            if (deprecatedConfigMap.containsKey(k) && LOG.isWarnEnabled()) {
              LOG.warn(
                  "Config {} is deprecated since Gravitino {}. {}",
                  k,
                  deprecatedConfigMap.get(k).version,
                  deprecatedConfigMap.get(k).deprecationMessage);
            }
            return true;
          }
          return false;
        });
  }

  /**
   * Loads properties from a file.
   *
   * @param file The properties file to load from.
   * @return The loaded properties.
   * @throws IOException If there's an issue loading the properties.
   */
  public Properties loadPropertiesFromFile(File file) throws IOException {
    Properties properties = new Properties();
    try (InputStream in = Files.newInputStream(file.toPath())) {
      properties.load(in);
      return properties;

    } catch (Exception e) {
      LOG.error("Failed to load properties from {}", file.getAbsolutePath(), e);
      throw new IOException("Failed to load properties from " + file.getAbsolutePath(), e);
    }
  }

  /** The DeprecatedConfig class represents a configuration entry that has been deprecated. */
  private static class DeprecatedConfig {
    private final String key;
    private final String version;
    private final String deprecationMessage;

    /**
     * Constructs a DeprecatedConfig instance.
     *
     * @param key The key of the deprecated configuration.
     * @param version The version in which the configuration was deprecated.
     * @param deprecationMessage Message to indicate the deprecation warning.
     */
    private DeprecatedConfig(String key, String version, String deprecationMessage) {
      this.key = key;
      this.version = version;
      this.deprecationMessage = deprecationMessage;
    }
  }
}
