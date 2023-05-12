package com.datastrato.unified_catalog.schema;
public enum SchemaVersion {
  V_0_1(0, 1);

  public final int majorVersion;
  public final int minorVersion;

  private SchemaVersion(int majorVersion, int minorVersion) {
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