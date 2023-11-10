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
    StorageLayoutVersion version = StorageLayoutVersion.fromString("v1");
    Assertions.assertEquals(StorageLayoutVersion.V1, version);

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> StorageLayoutVersion.fromString("v3.0"));
  }

  @Test
  void testCompatibleWith() {
    Assertions.assertTrue(StorageLayoutVersion.V1.compatibleWith(StorageLayoutVersion.V1));
  }
}
