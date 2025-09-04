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
package org.apache.gravitino.client;

import com.google.common.annotations.VisibleForTesting;
import org.apache.gravitino.Version;
import org.apache.gravitino.dto.VersionDTO;

/** Apache Gravitino version information. */
public class GravitinoVersion extends VersionDTO {

  @VisibleForTesting
  GravitinoVersion(String version, String compileDate, String gitCommit) {
    super(version, compileDate, gitCommit);
  }

  GravitinoVersion(VersionDTO versionDTO) {
    super(versionDTO.version(), versionDTO.compileDate(), versionDTO.gitCommit());
  }

  @VisibleForTesting
  /** @return parse the version number for a version string */
  int[] getVersionNumber() {
    return Version.parseVersionNumber(version());
  }

  /**
   * Check if the current version is compatible with the server version.
   *
   * @param serverVersion the server version to check compatibility with
   * @return true if the client current major version is less than or equal to the server's version
   */
  public boolean compatibleWithServerVersion(GravitinoVersion serverVersion) {
    int[] left = getVersionNumber();
    int[] right = serverVersion.getVersionNumber();
    return left[0] < right[0] || (left[0] == right[0] && left[1] <= right[1]);
  }
}
