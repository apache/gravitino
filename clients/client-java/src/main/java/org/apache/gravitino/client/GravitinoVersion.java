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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.dto.VersionDTO;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;

/** Apache Gravitino version information. */
public class GravitinoVersion extends VersionDTO implements Comparable {

  private static final int VERSION_PART_NUMBER = 3;

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
    Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)([-\\w]+)?");
    Matcher matcher = pattern.matcher(version());
    if (matcher.matches()) {
      int[] versionNumbers = new int[VERSION_PART_NUMBER];
      for (int i = 0; i < VERSION_PART_NUMBER; i++) {
        versionNumbers[i] = Integer.parseInt(matcher.group(i + 1));
      }
      return versionNumbers;
    }
    throw new GravitinoRuntimeException("Invalid version string " + version());
  }

  @Override
  public int compareTo(Object o) {
    if (!(o instanceof GravitinoVersion)) {
      return 1;
    }
    GravitinoVersion other = (GravitinoVersion) o;

    int[] left = getVersionNumber();
    int[] right = other.getVersionNumber();
    for (int i = 0; i < VERSION_PART_NUMBER; i++) {
      int v = left[i] - right[i];
      if (v != 0) {
        return v;
      }
    }
    return 0;
  }
}
