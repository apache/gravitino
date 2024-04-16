/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.dto.VersionDTO;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import java.io.IOException;
import java.util.Properties;

/** Retrieve the version and build information from the building process */
public class VersionUtil {

  private static boolean isInitialized = false;

  /** build version */
  public static String version;
  /** build time */
  public static String compileDate;
  /** build commit id */
  public static String gitCommit;

  private static void init() {
    Properties projectProperties = new Properties();
    try {
      projectProperties.load(
          VersionUtil.class.getClassLoader().getResourceAsStream("project.properties"));
      version = projectProperties.getProperty("project.version");
      compileDate = projectProperties.getProperty("compile.date");
      gitCommit = projectProperties.getProperty("git.commit.id");
      isInitialized = true;
    } catch (IOException e) {
      throw new GravitinoRuntimeException(e, "Failed to get Gravitino version");
    }
  }

  /** @return the current version DTO */
  public static VersionDTO createCurrentVersionDTO() {
    if (!isInitialized) {
      init();
    }
    return new VersionDTO(version, compileDate, gitCommit);
  }
}
