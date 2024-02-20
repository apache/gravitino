/*
 * Copyright 2023 Datastrato Pvt Ltd.
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

  public static StorageLayoutVersion fromString(String version) {
    for (StorageLayoutVersion v : StorageLayoutVersion.values()) {
      if (v.version.equals(version)) {
        return v;
      }
    }
    throw new StorageLayoutException(
        "Unknown storage version, maybe the data is broken, please check the storage directory.");
  }
}
