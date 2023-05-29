package com.datastrato.graviton;

import com.datastrato.graviton.config.ConfigEntry;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Config {

  private static final Logger LOG = LoggerFactory.getLogger(Config.class);

  private static final String CONFIG_PREPEND = "graviton.";

  private final ConcurrentMap<String, String> configMap;

  public Config(boolean loadDefaults) {
    configMap = new ConcurrentHashMap<>();
    if (loadDefaults) {
      loadFromProperties(System.getProperties());
    }
  }

  public Config() {
    this(true);
  }

  public Config loadFromFile(String name) throws IOException {
    String confDir =
        Optional.ofNullable(System.getenv("GRAVITON_CONF_DIR"))
            .orElse(
                Optional.ofNullable(System.getenv("GRAVITON_HOME"))
                    .map(s -> s + File.separator + "conf")
                    .orElse(null));

    if (confDir == null) {
      throw new IllegalArgumentException("GRAVITON_CONF_DIR or GRAVITON_HOME not set");
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

  public <T> T get(ConfigEntry<T> entry) throws NoSuchElementException {
    if (entry.isDeprecated()) {
      LOG.warn("Config {} is deprecated.", entry.getKey());
      if (!entry.getAlternatives().isEmpty()) {
        LOG.warn("Please use {} instead.", String.join(", ", entry.getAlternatives()));
      }
    }

    return entry.readFrom(configMap);
  }

  public <T> void set(ConfigEntry<T> entry, T value) {
    if (entry.isDeprecated()) {
      LOG.warn("Config {} is deprecated.", entry.getKey());
      if (!entry.getAlternatives().isEmpty()) {
        LOG.warn("Please use {} instead.", String.join(", ", entry.getAlternatives()));
      }
    }

    if (value == null) {
      LOG.warn("Config {} value to set is null, ignore setting to Config.", entry.getKey());
    }

    entry.writeTo(configMap, value);
  }

  private void loadFromMap(Map<String, String> map) {
    map.forEach(
        (k, v) -> {
          String trimmedK = k.trim();
          String trimmedV = v.trim();
          if (!trimmedK.isEmpty() && !trimmedV.isEmpty() && trimmedK.startsWith(CONFIG_PREPEND)) {
            configMap.put(trimmedK, trimmedV);
          }
        });
  }

  private void loadFromProperties(Properties properties) {
    loadFromMap(Maps.fromProperties(properties));
  }

  private Properties loadPropertiesFromFile(File file) throws IOException {
    Properties properties = new Properties();
    try (InputStream in = Files.newInputStream(file.toPath())) {
      properties.load(in);
      return properties;

    } catch (Exception e) {
      LOG.error("Failed to load properties from " + file.getAbsolutePath(), e);
      throw new IOException("Failed to load properties from " + file.getAbsolutePath(), e);
    }
  }
}
