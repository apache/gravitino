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
    StorageVersion version = StorageVersion.fromString("v1.1");
    Assertions.assertEquals(StorageVersion.V1_1, version);

    version = StorageVersion.fromString("v2.0");
    Assertions.assertEquals(StorageVersion.V2_0, version);

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> {
          StorageVersion.fromString("v3.0");
        });
  }

  @Test
  void testCompatibleWith() {
    Assertions.assertTrue(StorageVersion.V1_1.compatibleWith(StorageVersion.V1_0));
    Assertions.assertFalse(StorageVersion.V1_1.compatibleWith(StorageVersion.V2_0));
  }
}
