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

  private static volatile VersionUtil INSTANCE;

  private VersionInfo versionInfo;
  private VersionDTO versionDTO;

  private VersionUtil() {
    Properties projectProperties = new Properties();
    try {
      VersionInfo currentVersionInfo = new VersionInfo();
      projectProperties.load(
          VersionUtil.class.getClassLoader().getResourceAsStream("project.properties"));
      currentVersionInfo.version = projectProperties.getProperty("project.version");
      currentVersionInfo.compileDate = projectProperties.getProperty("compile.date");
      currentVersionInfo.gitCommit = projectProperties.getProperty("git.commit.id");

      versionInfo = currentVersionInfo;
      versionDTO =
          new VersionDTO(
              currentVersionInfo.version,
              currentVersionInfo.compileDate,
              currentVersionInfo.gitCommit);
    } catch (IOException e) {
      throw new GravitinoRuntimeException(e, "Failed to get Gravitino version");
    }
  }

  /** @return the instance of VersionUtil */
  public static VersionUtil getInstance() {
    if (INSTANCE != null) {
      return INSTANCE;
    }

    INSTANCE = new VersionUtil();
    return INSTANCE;
  }

  /** @return the current versionInfo */
  public static VersionInfo getCurrentVersion() {
    return getInstance().versionInfo;
  }

  /** @return the current version DTO */
  public static VersionDTO getCurrentVersionDTO() {
    return getInstance().versionDTO;
  }

  /** Store version information */
  public static class VersionInfo {
    /** build version */
    public String version;
    /** build time */
    public String compileDate;
    /** build commit id */
    public String gitCommit;
  }
}
