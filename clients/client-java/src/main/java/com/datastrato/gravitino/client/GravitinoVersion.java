/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.dto.VersionDTO;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.google.common.annotations.VisibleForTesting;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Gravitino version information. */
public class GravitinoVersion extends VersionDTO implements Comparable {

  private static final int VERSION_PART_NUMBER = 3;

  @VisibleForTesting
  GravitinoVersion(String version, String compoileDate, String gitCommit) {
    super(version, compoileDate, gitCommit);
  }

  GravitinoVersion(VersionDTO versionDTO) {
    super(versionDTO.version(), versionDTO.compileDate(), versionDTO.gitCommit());
  }

  @VisibleForTesting
  /** @return parse the version number for a version string */
  int[] getVersionNumber() {
    Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)(-\\w+){0,1}");
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
