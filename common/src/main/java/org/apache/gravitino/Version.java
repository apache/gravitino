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

import java.io.IOException;
import java.util.Properties;
import org.apache.gravitino.dto.VersionDTO;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;

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
