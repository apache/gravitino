/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

/** The storage layer version of the entity store. */
public enum StorageVersion {
  V1_0("v1.0"),
  V1_1("v1.1"),

  // V2_0 has not been released yet, it's just for testing.
  V2_0("v2.0");

  private final String version;

  StorageVersion(String version) {
    this.version = version;
  }

  public String getVersion() {
    return version;
  }

  // Returns true if the storage version is compatible with the other storage version.
  // For example, v1.1 is not compatible with v1.2, but v1.1 is compatible with v2.1.
  public boolean compatibleWith(StorageVersion other) {
    String thatVersion = other.version;
    return this.version.split("\\.")[0].equals(thatVersion.split("\\.")[0]);
  }

  public static StorageVersion fromString(String version) {
    for (StorageVersion v : StorageVersion.values()) {
      if (v.version.equals(version)) {
        return v;
      }
    }
    throw new IllegalArgumentException("Unknown storage version: " + version);
  }
}
