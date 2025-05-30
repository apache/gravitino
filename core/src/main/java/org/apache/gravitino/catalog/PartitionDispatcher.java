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
package org.apache.gravitino.catalog;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.partitions.Partition;

/**
 * {@code PartitionDispatcher} interface is a wrapper around the {@link SupportsPartitions}
 * interface, adding {@link NameIdentifier} of table to the method parameters for find out the
 * catalog class loader.
 */
public interface PartitionDispatcher {

  /**
   * List the names of all partitions in the table.
   *
   * @param tableIdent The identifier of the table.
   * @return The names of all partitions in the table.
   */
  String[] listPartitionNames(NameIdentifier tableIdent);

  /**
   * List all partitions in the table.
   *
   * @param tableIdent The identifier of the table.
   * @return The list of partitions.
   */
  Partition[] listPartitions(NameIdentifier tableIdent);

  /**
   * Get a partition by name from the table.
   *
   * @param tableIdent The identifier of the table.
   * @param partitionName The name of the partition.
   * @return The partition.
   * @throws NoSuchPartitionException if no partition with the specified name exists in the table
   */
  Partition getPartition(NameIdentifier tableIdent, String partitionName)
      throws NoSuchPartitionException;

  /**
   * Check if a partition exists in the table.
   *
   * @param tableIdent The identifier of the table.
   * @param partitionName The name of the partition.
   * @return True if the partition exists, false otherwise.
   */
  default boolean partitionExists(NameIdentifier tableIdent, String partitionName) {
    try {
      getPartition(tableIdent, partitionName);
      return true;
    } catch (NoSuchPartitionException e) {
      return false;
    }
  }

  /**
   * Add a partition to the table.
   *
   * @param tableIdent The identifier of the table.
   * @param partition The partition to add.
   * @return The added partition.
   * @throws PartitionAlreadyExistsException If the partition already exists.
   */
  Partition addPartition(NameIdentifier tableIdent, Partition partition)
      throws PartitionAlreadyExistsException;

  /**
   * Drop a partition from the table by name.
   *
   * @param tableIdent The identifier of the table.
   * @param partitionName The name of the partition.
   * @return True if the partition was dropped, false if the partition does not exist.
   */
  boolean dropPartition(NameIdentifier tableIdent, String partitionName);

  /**
   * Purge a partition from the table by name.
   *
   * @param tableIdent The identifier of the table.
   * @param partitionName The name of the partition.
   * @return True if the partition is purged, false if the partition does not exist.
   * @throws UnsupportedOperationException If partition purging is not supported.
   */
  default boolean purgePartition(NameIdentifier tableIdent, String partitionName)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Partition purging is not supported");
  }
}
