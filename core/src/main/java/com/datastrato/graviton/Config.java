package com.datastrato.graviton;

import com.datastrato.unified_catalog.config.ConfigEntry;
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

public final class Config {

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
        Optional.ofNullable(System.getenv("UNIFIED_CATALOG_CONF_DIR"))
            .orElse(
                Optional.ofNullable(System.getenv("UNIFIED_CATALOG_HOME"))
                    .map(s -> s + File.separator + "conf")
                    .orElse(null));

    if (confDir == null) {
      throw new IllegalArgumentException(
          "UNIFIED_CATALOG_CONF_DIR or UNIFIED_CATALOG_HOME not set");
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
    // TODO. Add deprecation warning log later on. @Jerry

    return entry.readFrom(configMap);
  }

  public <T> void set(ConfigEntry<T> entry, T value) {
    // TODO. Add deprecation warning log later on. @Jerry

    entry.writeTo(configMap, value);
  }

  private void loadFromMap(Map<String, String> map) {
    map.forEach(
        (k, v) -> {
          if (k.trim().startsWith(CONFIG_PREPEND)) {
            configMap.put(k.trim(), v.trim());
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
      throw new IOException("Failed to load properties from " + file.getAbsolutePath(), e);
    }
  }
}
