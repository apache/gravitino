/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
package com.datastrato.graviton.server.web;

import java.util.Comparator;
import java.util.TreeSet;

public enum ApiVersion {
  V_1(1);

  private static final TreeSet<ApiVersion> VERSIONS =
      new TreeSet<>(Comparator.comparingInt(o -> o.version));

  static {
    VERSIONS.add(V_1);
  }

  private final int version;

  ApiVersion(int version) {
    this.version = version;
  }

  public int version() {
    return version;
  }

  public static ApiVersion latestVersion() {
    return VERSIONS.last();
  }

  public static boolean isSupportedVersion(int version) {
    for (ApiVersion v : ApiVersion.values()) {
      if (v.version == version) {
        return true;
      }
    }

    return false;
  }
}
