/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.store.kv;

import lombok.Builder;
import lombok.Data;

/** Helper class for rocksdb operations */
public class RockDBHelper {
  @Builder
  @Data
  static class ScanRange {
    private byte[] start;
    private byte[] end;
    private boolean startInclusive;
    private boolean endInclusive;

    // limit
    private int limit;
  }
}
