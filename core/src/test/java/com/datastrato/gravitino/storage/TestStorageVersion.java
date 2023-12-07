/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
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
        StorageLayoutException.class, () -> StorageLayoutVersion.fromString("v200000.0"));
  }
}
