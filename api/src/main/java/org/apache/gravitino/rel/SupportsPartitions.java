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
package org.apache.gravitino.rel;

import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.RangePartition;

/** Interface for tables that support partitions. */
@Evolving
public interface SupportsPartitions {

  /**
   * List all partition names of the table
   *
   * @return The list of partition names
   */
  String[] listPartitionNames();

  /**
   * List all partitions
   *
   * @return The list of partitions
   */
  Partition[] listPartitions();

  /**
   * Get a partition by partition name, you may get one of the following types of partitions:
   *
   * <ul>
   *   <li>{@link IdentityPartition}
   *   <li>{@link ListPartition}
   *   <li>{@link RangePartition}
   * </ul>
   *
   * It depends on the {@link Table#partitioning()}. A Java type conversion is required before
   * getting the specific partition, for example:
   *
   * <pre>
   *   RangePartition rangePartition = (RangePartition) table.supportPartitions().getPartition("p20200321");
   *   Literal&lt;?&gt; upper = rangePartition.upper();
   *   Literal&lt;?&gt; lower = rangePartition.lower();
   *   ...
   * </pre>
   *
   * @param partitionName the name of the partition
   * @return the partition
   * @throws NoSuchPartitionException if the partition does not exist
   */
  Partition getPartition(String partitionName) throws NoSuchPartitionException;

  /**
   * Check if a partition exists.
   *
   * @param partitionName The name of the partition.
   * @return True if the partition exists, false otherwise.
   */
  default boolean partitionExists(String partitionName) {
    try {
      getPartition(partitionName);
      return true;
    } catch (NoSuchPartitionException e) {
      return false;
    }
  }

  /**
   * Add a partition with specified name and properties to the table.
   *
   * @param partition The partition to add.
   * @return The created partition.
   * @throws PartitionAlreadyExistsException If the partition already exists.
   */
  Partition addPartition(Partition partition) throws PartitionAlreadyExistsException;

  /**
   * Drop a partition with specified name.
   *
   * @param partitionName the name of the partition
   * @return true if the partition is deleted successfully, false if the partition does not exist.
   */
  boolean dropPartition(String partitionName);

  /**
   * If the table supports purging, drop a partition with specified name and completely remove
   * partition data by skipping a trash. If the table is an external table or does not support
   * purging partitions, {@link UnsupportedOperationException} is thrown.
   *
   * @param partitionName The name of the partition.
   * @return true if the partition is purged, false if the partition does not exist.
   * @throws UnsupportedOperationException If partition purging is not supported.
   */
  default boolean purgePartition(String partitionName) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Partition purging is not supported");
  }
}
