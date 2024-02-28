/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

public class ITUtils {
  public static final String TEST_MODE = "testMode";
  public static final String EMBEDDED_TEST_MODE = "embedded";

  public static String joinPath(String... dirs) {
    return String.join(File.separator, dirs);
  }

  public static String[] splitPath(String path) {
    return path.split(File.separator);
  }

  public static void rewriteConfigFile(
      String configTempFileName, String configFileName, Map<String, String> configMap)
      throws IOException {
    Properties props = new Properties();
    try (InputStream inputStream = Files.newInputStream(Paths.get(configTempFileName));
        OutputStream outputStream = Files.newOutputStream(Paths.get(configFileName))) {
      props.load(inputStream);
      props.putAll(configMap);
      for (String key : props.stringPropertyNames()) {
        String value = props.getProperty(key);
        // Use customized write functions to avoid escaping `:` into `\:`.
        outputStream.write((key + " = " + value + "\n").getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  private ITUtils() {}
}
