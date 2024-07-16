/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.storage.kv;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;
import org.apache.gravitino.storage.BiPredicate;
import org.apache.gravitino.utils.Bytes;

/** Represents a range scan/delete configuration for the key-value store. */
@Builder
@Data
public class KvRange {
  private byte[] start;
  private byte[] end;
  private boolean startInclusive;
  private boolean endInclusive;

  private int limit;
  private BiPredicate<byte[], byte[]> predicate;

  /**
   * Constructs a KvRangeScan instance with the specified parameters.
   *
   * @param start The start key of the range.
   * @param end The end key of the range.
   * @param startInclusive True if the start key is inclusive, false otherwise.
   * @param endInclusive True if the end key is inclusive, false otherwise.
   * @param limit The maximum number of results to retrieve.
   * @param predicate The predicate to use to filter key-value pairs.
   */
  public KvRange(
      byte[] start,
      byte[] end,
      boolean startInclusive,
      boolean endInclusive,
      int limit,
      BiPredicate<byte[], byte[]> predicate) {
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
    this.predicate = predicate == null ? (k, v) -> true : predicate;
  }
}
