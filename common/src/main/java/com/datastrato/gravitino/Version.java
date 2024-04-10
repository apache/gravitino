/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import java.io.IOException;
import java.util.Properties;

/** Retrieve the version and build information from the building process */
public class Version {
  public static final String version;
  public static final String compileDate;
  public static final String gitCommit;

  static {
    Properties projectProperties = new Properties();
    try {
      projectProperties.load(
          Version.class.getClassLoader().getResourceAsStream("project.properties"));
      version = projectProperties.getProperty("project.version");
      compileDate = projectProperties.getProperty("compile.date");
      gitCommit = projectProperties.getProperty("git.commit.id");
    } catch (IOException e) {
      throw new GravitinoRuntimeException(e, "Failed to get Gravitino version");
    }
  }
}
