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

  private static volatile VersionInfo versionInfo;

  private static void init() {
    Properties projectProperties = new Properties();
    try {
      VersionInfo currentVersionInfo = new VersionInfo();
      projectProperties.load(
          VersionUtil.class.getClassLoader().getResourceAsStream("project.properties"));
      currentVersionInfo.version = projectProperties.getProperty("project.version");
      currentVersionInfo.compileDate = projectProperties.getProperty("compile.date");
      currentVersionInfo.gitCommit = projectProperties.getProperty("git.commit.id");
      versionInfo = currentVersionInfo;
    } catch (IOException e) {
      throw new GravitinoRuntimeException(e, "Failed to get Gravitino version");
    }
  }
  /** @return the current versionInfo */
  public static VersionInfo getCurrentVersion() {
    if (versionInfo != null) {
      return versionInfo;
    }
    init();
    return versionInfo;
  }

  /** @return the current version DTO */
  public static VersionDTO getCurrentVersionDTO() {
    VersionInfo versionInfo = getCurrentVersion();
    return new VersionDTO(versionInfo.version, versionInfo.compileDate, versionInfo.gitCommit);
  }

  public static class VersionInfo {
    /** build version */
    public String version;
    /** build time */
    public String compileDate;
    /** build commit id */
    public String gitCommit;
  }
}
