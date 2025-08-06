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
  private PartitionComparator comparator = PartitionNameComparator.instance();

  private PartitionRange() {}

  /**
   * Creates a PartitionRange which only has upper bound partition name.
   *
   * @param upperPartitionName the upper partition name, exclusive.
   * @return a PartitionRange with the upper partition name.
   */
  public static PartitionRange lessThan(String upperPartitionName) {
    return lessThan(upperPartitionName, PartitionComparator.Type.NAME);
  }

  /**
   * Creates a PartitionRange which only has upper bound partition name with a specific comparator
   * type.
   *
   * @param upperPartitionName the upper partition name, exclusive.
   * @param type the type of partition comparator to use for this range.
   * @return a PartitionRange with the upper partition name and the specified comparator type.
   */
  public static PartitionRange lessThan(String upperPartitionName, PartitionComparator.Type type) {
    PartitionRange partitionRange = new PartitionRange();
    partitionRange.upperPartitionName = Optional.of(upperPartitionName);
    partitionRange.comparator = PartitionComparator.of(type);
    return partitionRange;
  }

  /**
   * Creates a PartitionRange which only has lower bound partition name.
   *
   * @param lowerPartitionName the lower partition name, inclusive.
   * @return a PartitionRange with the lower partition name.
   */
  public static PartitionRange greaterOrEqual(String lowerPartitionName) {
    return greaterOrEqual(lowerPartitionName, PartitionComparator.Type.NAME);
  }

  /**
   * Creates a PartitionRange which only has lower bound partition name with a specific comparator
   * type.
   *
   * @param lowerPartitionName the lower partition name, inclusive.
   * @param type the type of partition comparator to use for this range.
   * @return a PartitionRange with the lower partition name and the specified comparator type.
   */
  public static PartitionRange greaterOrEqual(
      String lowerPartitionName, PartitionComparator.Type type) {
    PartitionRange partitionRange = new PartitionRange();
    partitionRange.lowerPartitionName = Optional.of(lowerPartitionName);
    partitionRange.comparator = PartitionComparator.of(type);
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
    return between(lowerPartitionName, upperPartitionName, PartitionComparator.Type.NAME);
  }

  /**
   * Creates a PartitionRange which has both lower and upper partition names with a specific
   * comparator type.
   *
   * @param lowerPartitionName the lower partition name, inclusive.
   * @param upperPartitionName the upper partition name, exclusive.
   * @param type the type of partition comparator to use for this range.
   * @return a PartitionRange with both lower and upper partition names and the specified comparator
   *     type.
   */
  public static PartitionRange between(
      String lowerPartitionName, String upperPartitionName, PartitionComparator.Type type) {
    PartitionRange partitionRange = new PartitionRange();
    partitionRange.upperPartitionName = Optional.of(upperPartitionName);
    partitionRange.lowerPartitionName = Optional.of(lowerPartitionName);
    partitionRange.comparator = PartitionComparator.of(type);
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
    return comparator;
  }
}
