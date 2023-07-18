/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
package com.datastrato.graviton.meta;

public enum SchemaVersion {
  V_0_1(0, 1);

  public final int majorVersion;

  public final int minorVersion;

  public static SchemaVersion forValues(int majorVersion, int minorVersion) {
    for (SchemaVersion schemaVersion : SchemaVersion.values()) {
      if (schemaVersion.majorVersion == majorVersion
          && schemaVersion.minorVersion == minorVersion) {
        return schemaVersion;
      }
    }

    throw new IllegalArgumentException(
        String.format(
            "No schema version found for major version %d and minor version %d",
            majorVersion, minorVersion));
  }

  SchemaVersion(int majorVersion, int minorVersion) {
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
  }

  public int getMajorVersion() {
    return majorVersion;
  }

  public int getMinorVersion() {
    return minorVersion;
  }
}
