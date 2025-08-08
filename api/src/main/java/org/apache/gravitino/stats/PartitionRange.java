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

import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;

/** PartitionRange represents a range of partitions defined by lower and upper partition names. */
public class PartitionRange {
  private static final SortOrder DEFAULT_COMPARATOR =
      SortOrders.of(NamedReference.MetadataField.PARTITION_NAME_FIELD, SortDirection.ASCENDING);
  private Optional<String> lowerPartitionName = Optional.empty();
  private Optional<BoundType> lowerBoundType = Optional.empty();
  private Optional<String> upperPartitionName = Optional.empty();
  private Optional<BoundType> upperBoundType = Optional.empty();

  private SortOrder comparator;

  private PartitionRange() {}

  /**
   * Creates a PartitionRange which only has upper bound partition name.
   *
   * @param upperPartitionName the upper partition name.
   * @param upperBoundType the type of the upper bound (open or closed).
   * @return a PartitionRange with the upper partition name.
   */
  public static PartitionRange upTo(String upperPartitionName, BoundType upperBoundType) {
    return upTo(upperPartitionName, upperBoundType, DEFAULT_COMPARATOR);
  }

  /**
   * Creates a PartitionRange which only has upper bound partition name with a specific comparator
   * type.
   *
   * @param upperPartitionName the upper partition name.
   * @param upperBoundType the type of the upper bound (open or closed).
   * @param comparator the comparator to use for this range.
   * @return a PartitionRange with the upper partition name and the specified comparator type.
   */
  public static PartitionRange upTo(
      String upperPartitionName, BoundType upperBoundType, SortOrder comparator) {
    Preconditions.checkArgument(upperPartitionName != null, "Upper partition name cannot be null");
    Preconditions.checkArgument(upperBoundType != null, "Upper bound type cannot be null");
    Preconditions.checkArgument(
        !upperPartitionName.isEmpty(), "Upper partition name cannot be empty");
    PartitionRange partitionRange = new PartitionRange();
    partitionRange.upperPartitionName = Optional.of(upperPartitionName);
    partitionRange.upperBoundType = Optional.of(upperBoundType);
    partitionRange.comparator = comparator;
    return partitionRange;
  }

  /**
   * Creates a PartitionRange which only has lower bound partition name.
   *
   * @param lowerPartitionName the lower partition name.
   * @param lowerBoundType the type of the lower bound (open or closed).
   * @return a PartitionRange with the lower partition name.
   */
  public static PartitionRange downTo(String lowerPartitionName, BoundType lowerBoundType) {
    return downTo(lowerPartitionName, lowerBoundType, DEFAULT_COMPARATOR);
  }

  /**
   * Creates a PartitionRange which only has lower bound partition name with a specific comparator
   * type.
   *
   * @param lowerPartitionName the lower partition name.
   * @param lowerBoundType the type of the lower bound (open or closed).
   * @param comparator the comparator to use for this range.
   * @return a PartitionRange with the lower partition name and the specified comparator type.
   */
  public static PartitionRange downTo(
      String lowerPartitionName, BoundType lowerBoundType, SortOrder comparator) {
    Preconditions.checkArgument(lowerPartitionName != null, "Lower partition name cannot be null");
    Preconditions.checkArgument(lowerBoundType != null, "Lower bound type cannot be null");
    Preconditions.checkArgument(comparator != null, "Comparator cannot be null");
    PartitionRange partitionRange = new PartitionRange();
    partitionRange.lowerPartitionName = Optional.of(lowerPartitionName);
    partitionRange.lowerBoundType = Optional.of(lowerBoundType);
    partitionRange.comparator = comparator;
    return partitionRange;
  }

  /**
   * Creates a PartitionRange which has both lower and upper partition names.
   *
   * @param lowerPartitionName the lower partition name.
   * @param lowerBoundType the type of the lower bound (open or closed).
   * @param upperPartitionName the upper partition name.
   * @param upperBoundType the type of the upper bound (open or closed).
   * @return a PartitionRange with both lower and upper partition names.
   */
  public static PartitionRange between(
      String lowerPartitionName,
      BoundType lowerBoundType,
      String upperPartitionName,
      BoundType upperBoundType) {
    return between(
        lowerPartitionName, lowerBoundType, upperPartitionName, upperBoundType, DEFAULT_COMPARATOR);
  }

  /**
   * Creates a PartitionRange which has both lower and upper partition names with a specific
   * comparator type.
   *
   * @param lowerPartitionName the lower partition name.
   * @param lowerBoundType the type of the lower bound (open or closed).
   * @param upperPartitionName the upper partition name.
   * @param upperBoundType the type of the upper bound (open or closed).
   * @param comparator the comparator to use for this range.
   * @return a PartitionRange with both lower and upper partition names and the specified comparator
   *     type.
   */
  public static PartitionRange between(
      String lowerPartitionName,
      BoundType lowerBoundType,
      String upperPartitionName,
      BoundType upperBoundType,
      SortOrder comparator) {
    Preconditions.checkArgument(lowerPartitionName != null, "Lower partition name cannot be null");
    Preconditions.checkArgument(upperPartitionName != null, "Upper partition name cannot be null");
    Preconditions.checkArgument(lowerBoundType != null, "Lower bound type cannot be null");
    Preconditions.checkArgument(upperBoundType != null, "Upper bound type cannot be null");
    Preconditions.checkArgument(comparator != null, "Comparator cannot be null");
    PartitionRange partitionRange = new PartitionRange();
    partitionRange.upperPartitionName = Optional.of(upperPartitionName);
    partitionRange.lowerPartitionName = Optional.of(lowerPartitionName);
    partitionRange.upperBoundType = Optional.of(upperBoundType);
    partitionRange.lowerBoundType = Optional.of(lowerBoundType);
    partitionRange.comparator = comparator;
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
   * Returns the type of the lower bound if it exists.
   *
   * @return an Optional containing the BoundType of the lower bound if it exists, otherwise an
   *     empty Optional.
   */
  public Optional<BoundType> lowerBoundType() {
    return lowerBoundType;
  }

  /**
   * Returns the type of the upper bound if it exists.
   *
   * @return an Optional containing the BoundType of the upper bound if it exists, otherwise an
   *     empty Optional.
   */
  public Optional<BoundType> upperBoundType() {
    return upperBoundType;
  }

  /**
   * Returns a comparator for comparing partitions within this range.
   *
   * @return a PartitionComparator that can be used to compare partitions.
   */
  public SortOrder comparator() {
    return comparator;
  }

  /** Enum representing the type of bounds for a partition range. */
  public enum BoundType {
    /** Indicates that the bound is exclusive */
    OPEN,
    /** Indicates that the bound is inclusive */
    CLOSED
  }
}
