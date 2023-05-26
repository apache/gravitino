package com.datastrato.graviton.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum SchemaVersion {
  V_0_1(0, 1);

  @JsonProperty("major_version")
  public final int majorVersion;

  @JsonProperty("minor_version")
  public final int minorVersion;

  @JsonCreator
  public static SchemaVersion forValues(
      @JsonProperty("major_version") int majorVersion,
      @JsonProperty("minor_version") int minorVersion) {
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
