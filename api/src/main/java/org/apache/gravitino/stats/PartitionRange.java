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
package org.apache.gravitino.stats;

import java.util.Optional;

/** PartitionRange represents a range of partitions defined by lower and upper partition names. */
public class PartitionRange {
  Optional<String> lowerPartitionName = Optional.empty();
  Optional<String> upperPartitionName = Optional.empty();

  private PartitionRange() {}

  /**
   * Creates a PartitionRange which only has upper bound partition name.
   *
   * @param upperPartitionName the upper partition name, exclusive.
   * @return a PartitionRange with the upper partition name.
   */
  public static PartitionRange lessThan(String upperPartitionName) {
    PartitionRange partitionRange = new PartitionRange();
    partitionRange.upperPartitionName = Optional.of(upperPartitionName);
    return partitionRange;
  }

  /**
   * Creates a PartitionRange which only has lower bound partition name.
   *
   * @param lowerPartitionName the lower partition name, inclusive.
   * @return a PartitionRange with the lower partition name.
   */
  public static PartitionRange greaterThanOrEqual(String lowerPartitionName) {
    PartitionRange partitionRange = new PartitionRange();
    partitionRange.lowerPartitionName = Optional.of(lowerPartitionName);
    return partitionRange;
  }

  /**
   * Creates a PartitionRange which has both lower and upper partition names.
   *
   * @param lowerPartitionName the lower partition name, inclusive.
   * @param upperPartitionName the upper partition name, exclusive.
   * @return a PartitionRange with both lower and upper partition names.
   */
  public static PartitionRange between(String lowerPartitionName, String upperPartitionName) {
    PartitionRange partitionRange = new PartitionRange();
    partitionRange.upperPartitionName = Optional.of(upperPartitionName);
    partitionRange.lowerPartitionName = Optional.of(lowerPartitionName);
    return partitionRange;
  }

  /**
   * Returns the lower partition name if it exists.
   *
   * @return an Optional containing the lower partition name if it exists, otherwise an empty
   *     Optional.
   */
  public Optional<String> lowerPartitionName() {
    return lowerPartitionName;
  }

  /**
   * Returns the upper partition name if it exists.
   *
   * @return an Optional containing the upper partition name if it exists, otherwise an empty
   *     Optional.
   */
  public Optional<String> upperPartitionName() {
    return upperPartitionName;
  }

  /**
   * Returns a comparator for comparing partitions within this range.
   *
   * @return a PartitionComparator that can be used to compare partitions.
   */
  public PartitionComparator comparator() {
    return PartitionComparator.defaultComparator();
  }
}
