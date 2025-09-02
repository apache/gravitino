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
package org.apache.gravitino;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.VersionDTO;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;

/** Retrieve the version and build information from the building process */
public class Version {

  /** The HTTP header to send the client version */
  public static final String CLIENT_VERSION_HEADER = "X-Client-Version";

  private static final int VERSION_PART_NUMBER = 3;
  private static final Pattern PATTERN =
      Pattern.compile("^(\\d+)\\.(\\d+)\\.(\\d+)(?:-.*|\\.([a-zA-Z].*))?$");

  private static final Version INSTANCE = new Version();

  private final VersionInfo versionInfo;
  private final VersionDTO versionDTO;

  private Version() {
    Properties projectProperties = new Properties();
    try (InputStream inputStream =
        Version.class.getClassLoader().getResourceAsStream("gravitino-build-info.properties")) {
      if (inputStream == null) {
        throw new GravitinoRuntimeException(
            new IOException("Resource gravitino-build-info.properties not found"),
            "Failed to get Gravitino version: Build info properties file is missing.");
      }

      VersionInfo currentVersionInfo = new VersionInfo();
      projectProperties.load(inputStream);
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

  /**
   * Parse the version number from a version string
   *
   * @param versionString the version string to parse
   * @return an array of integers representing the version number
   */
  public static int[] parseVersionNumber(String versionString) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(versionString), "Version string is null or empty");

    Matcher matcher = PATTERN.matcher(versionString);
    if (matcher.matches()) {
      int[] versionNumbers = new int[VERSION_PART_NUMBER];
      for (int i = 0; i < VERSION_PART_NUMBER; i++) {
        versionNumbers[i] = Integer.parseInt(matcher.group(i + 1));
      }
      return versionNumbers;
    }
    throw new GravitinoRuntimeException("Invalid version string " + versionString);
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
