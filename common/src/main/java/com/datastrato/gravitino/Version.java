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
public class Version {

  private static final Version INSTANCE = new Version();

  private VersionInfo versionInfo;
  private VersionDTO versionDTO;

  private Version() {
    Properties projectProperties = new Properties();
    try {
      VersionInfo currentVersionInfo = new VersionInfo();
      projectProperties.load(
          Version.class.getClassLoader().getResourceAsStream("gravitino-build-info.properties"));
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

  /** @return the current versionInfo */
  public static VersionInfo getCurrentVersion() {
    return INSTANCE.versionInfo;
  }

  /** @return the current version DTO */
  public static VersionDTO getCurrentVersionDTO() {
    return INSTANCE.versionDTO;
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
