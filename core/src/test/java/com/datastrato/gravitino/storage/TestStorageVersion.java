/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestStorageVersion {

  @Test
  void testFromString() {
    StorageVersion version = StorageVersion.fromString("v1");
    Assertions.assertEquals(StorageVersion.V1, version);

    version = StorageVersion.fromString("v2");
    Assertions.assertEquals(StorageVersion.V2, version);

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> {
          StorageVersion.fromString("v3.0");
        });
  }

  @Test
  void testCompatibleWith() {
    Assertions.assertTrue(StorageVersion.V1.compatibleWith(StorageVersion.V1));
    Assertions.assertFalse(StorageVersion.V2.compatibleWith(StorageVersion.V1));
  }
}
