/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server;

import com.datastrato.graviton.Configs;
import com.datastrato.graviton.config.ConfigEntry;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestServerConfig {
  @Test
  public void checkGravitonConfFile()
      throws NoSuchFieldException, IllegalAccessException, IOException {
    // Load all program config keys from `ServerConfig` and `Configs` into a map
    Map<String, String> configKeyMap = new HashMap<>();
    configKeyMap.putAll(getConfigEntryFromClass(ServerConfig.class));
    configKeyMap.putAll(getConfigEntryFromClass(Configs.class));

    // Load all config keys from `graviton.conf.template` into a map
    Properties properties = new Properties();
    String confFile =
        System.getenv("GRAVITON_HOME")
            + File.separator
            + "conf"
            + File.separator
            + "graviton.conf.template";
    InputStream in = Files.newInputStream(new File(confFile).toPath());
    properties.load(in);

    // Check if all config keys from `graviton.conf.template` are defined in `ServerConfig` and
    // `Configs`
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      String propKey = (String) entry.getKey();
      Assertions.assertTrue(
          configKeyMap.containsKey(propKey),
          "Config key " + propKey + " is not defined in ConfigEntry");
    }
  }

  // Get all ConfigEntry member variables from a config class
  private Map<String, String> getConfigEntryFromClass(Class<?> configClazz)
      throws NoSuchFieldException, IllegalAccessException {
    Map<String, String> configKeyMap = new HashMap<>();
    // Get all fields
    Field[] fields = configClazz.getDeclaredFields();
    for (Field field : fields) {
      String fieldName = field.getName();
      Class<?> fieldType = field.getType();

      if (!(fieldType == ConfigEntry.class)) {
        continue;
      }
      Field memberConfigEntry = configClazz.getDeclaredField(fieldName);
      memberConfigEntry.setAccessible(true);

      // Get all ConfigEntry member variables
      ConfigEntry<?> configEntry = (ConfigEntry<?>) memberConfigEntry.get(null);
      String configEntryKey = configEntry.getKey();
      configKeyMap.put(configEntryKey, fieldName);
    }
    return configKeyMap;
  }
}
