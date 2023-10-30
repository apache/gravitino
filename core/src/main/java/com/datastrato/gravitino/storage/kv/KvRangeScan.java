/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.utils.Bytes;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;

/** Represents a range scan configuration for the key-value store. */
@Builder
@Data
public class KvRangeScan {
  private byte[] start;
  private byte[] end;
  private boolean startInclusive;
  private boolean endInclusive;

  private int limit;

  /**
   * Constructs a KvRangeScan instance with the specified parameters.
   *
   * @param start The start key of the range.
   * @param end The end key of the range.
   * @param startInclusive True if the start key is inclusive, false otherwise.
   * @param endInclusive True if the end key is inclusive, false otherwise.
   * @param limit The maximum number of results to retrieve.
   */
  public KvRangeScan(
      byte[] start, byte[] end, boolean startInclusive, boolean endInclusive, int limit) {
    Preconditions.checkArgument(start != null, "start cannot be null");
    Preconditions.checkArgument(end != null, "start cannot be null");
    Preconditions.checkArgument(
        Bytes.wrap(start).compareTo(end) <= 0, "start must be less than or equal to end");

    if (limit == 0) {
      limit = Integer.MAX_VALUE;
    }
    Preconditions.checkArgument(limit > 0, "limit must be greater than 0");

    this.start = start;
    this.end = end;
    this.startInclusive = startInclusive;
    this.endInclusive = endInclusive;
    this.limit = limit;
  }
}
