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
package org.apache.gravitino.server;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestServerConfig {

  @Test
  public void checkGravitinoConfFile()
      throws NoSuchFieldException, IllegalAccessException, IOException {
    // Load all program config keys from `ServerConfig` and `Configs` into a map
    Map<String, String> configKeyMap = new HashMap<>();
    configKeyMap.putAll(getConfigEntryFromClass(ServerConfig.class));
    configKeyMap.putAll(getConfigEntryFromClass(Configs.class));
    Map<String, String> jettyConfigMap =
        getConfigEntryFromClass(JettyServerConfig.class).entrySet().stream()
            .collect(
                Collectors.toMap(
                    kv -> JettyServerConfig.GRAVITINO_SERVER_CONFIG_PREFIX + kv.getKey(),
                    Map.Entry::getValue));
    configKeyMap.putAll(jettyConfigMap);

    // Load all config keys from `gravitino.conf.template` into a map
    Properties properties = new Properties();
    String confFile =
        System.getenv("GRAVITINO_HOME")
            + File.separator
            + "conf"
            + File.separator
            + "gravitino.conf.template";
    InputStream in = Files.newInputStream(new File(confFile).toPath());
    properties.load(in);

    // Check if all config keys from `gravitino.conf.template` are defined in `ServerConfig` and
    // `Configs`
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      String propKey = (String) entry.getKey();
      if (propKey.startsWith(AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX)
          || propKey.startsWith("gravitino.iceberg-rest.")
          || propKey.startsWith("gravitino.lance-rest.")) {
        continue;
      }
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
