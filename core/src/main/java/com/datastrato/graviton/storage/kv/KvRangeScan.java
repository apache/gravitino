/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import java.util.function.Predicate;
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
  private Predicate<byte[]> predicate;

  /**
   * Constructs a KvRangeScan instance with the specified parameters.
   *
   * @param start The start key of the range.
   * @param end The end key of the range.
   * @param startInclusive True if the start key is inclusive, false otherwise.
   * @param endInclusive True if the end key is inclusive, false otherwise.
   * @param limit The maximum number of results to retrieve.
   * @param predicate The predicate to filter the results.
   */
  @Builder
  public KvRangeScan(
      byte[] start,
      byte[] end,
      boolean startInclusive,
      boolean endInclusive,
      int limit,
      Predicate<byte[]> predicate) {
    this.start = start;
    this.end = end;
    this.startInclusive = startInclusive;
    this.endInclusive = endInclusive;
    this.limit = limit <= 0 ? Integer.MAX_VALUE : limit;
    this.predicate = predicate == null ? k -> true : predicate;
  }
}
