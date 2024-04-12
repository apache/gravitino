/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.dto.VersionDTO;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Gravitino version information. */
public class GravitinoVersion extends VersionDTO {

  static final int VERSION_PART_NUMBER = 3;

  GravitinoVersion(VersionDTO versionDTO) {
    super(versionDTO.version(), versionDTO.compileDate(), versionDTO.gitCommit());
  }

  /**
   * Check if the version is greater than the other version.
   *
   * @param other The other version to compare.
   * @return true if the version is greater than the other version.
   */
  public boolean greaterThan(GravitinoVersion other) {
    int left[] = getVersionNumber();
    int right[] = other.getVersionNumber();
    for (int i = 0; i < VERSION_PART_NUMBER; i++) {
      int v = left[i] - right[i];
      if (v != 0) {
        return v > 0;
      }
    }
    return false;
  }

  private int[] getVersionNumber() {
    Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)");
    Matcher matcher = pattern.matcher(version());
    if (matcher.find()) {
      int[] versionNumbers = new int[VERSION_PART_NUMBER];
      for (int i = 0; i < VERSION_PART_NUMBER; i++) {
        versionNumbers[i] = Integer.parseInt(matcher.group(i + 1));
      }
      return versionNumbers;
    }
    throw new GravitinoRuntimeException("Invalid version string " + version());
  }
}
