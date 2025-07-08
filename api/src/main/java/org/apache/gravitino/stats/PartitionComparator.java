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

import org.apache.gravitino.rel.partitions.Partition;

/** PartitionComparator is an interface that defines a method for comparing two partitions. */
public interface PartitionComparator {

  /**
   * Returns a default comparator for comparing two partitions.
   *
   * @return a default comparator that compares partitions by their names
   */
  static PartitionComparator defaultComparator() {
    return (firstPartition, secondPartition) -> {
      if (firstPartition == null && secondPartition == null) {
        return 0;
      } else if (firstPartition == null) {
        return -1;
      } else if (secondPartition == null) {
        return 1;
      }
      return firstPartition.name().compareTo(secondPartition.name());
    };
  }

  /**
   * Compares two partitions.
   *
   * @param firstPartition the first partition to compare
   * @param secondPartition the second partition to compare
   * @return a negative integer, zero, or a positive integer as the first argument is less than,
   *     equal to, or greater than the second.
   */
  int compareTo(Partition firstPartition, Partition secondPartition);
}
