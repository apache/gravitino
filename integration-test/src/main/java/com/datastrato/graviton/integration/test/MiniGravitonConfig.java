/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.test;

import com.datastrato.graviton.Config;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;

public class MiniGravitonConfig extends Config {
  @Override
  public Config loadFromFile(String confFileName) throws Exception {
    File confFile = new File(confFileName);
    if (!confFile.exists()) {
      throw new IllegalArgumentException(
          "Config file " + confFile.getAbsolutePath() + " not found");
    }

    Properties properties = new Properties();
    try (InputStream in = Files.newInputStream(confFile.toPath())) {
      properties.load(in);
    } catch (Exception e) {
      throw new IOException("Failed to load properties from " + confFile.getAbsolutePath(), e);
    }

    super.loadFromProperties(properties);

    return this;
  }
}
