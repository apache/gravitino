/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

import lombok.Getter;

/** The storage layer version of the entity store. */
@Getter
public enum StorageLayoutVersion {
  V1("v1");

  private final String version;

  StorageLayoutVersion(String version) {
    this.version = version;
  }

  // Return true if the storage version is compatible with the other storage version.
  // For example, v1 is not compatible with v2.
  public boolean compatibleWith(StorageLayoutVersion other) {
    String thatVersion = other.version;
    return this.version.split("\\.")[0].equals(thatVersion.split("\\.")[0]);
  }

  public static StorageLayoutVersion fromString(String version) {
    for (StorageLayoutVersion v : StorageLayoutVersion.values()) {
      if (v.version.equals(version)) {
        return v;
      }
    }
    throw new IllegalArgumentException("Unknown storage version: " + version);
  }
}
