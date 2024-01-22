/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel;

import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.rel.partitions.Partition;

public interface SupportsPartitions extends Table {

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
   * Get a partition by partition name
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
   * Alter a partition with specified identifier.
   *
   * @param partitionName The identifier of the partition.
   * @param changes The changes to apply.
   * @return The altered partition.
   * @throws NoSuchPartitionException If the partition does not exist.
   */
  default Partition alterPartition(String partitionName, PartitionChange... changes)
      throws NoSuchPartitionException {
    throw new UnsupportedOperationException("Partition altering is not supported");
  }

  /**
   * Drop a partition with specified name.
   *
   * @param partitionName The identifier of the partition.
   * @return true if a partition was deleted, false if the partition did not exist.
   */
  boolean dropPartition(String partitionName);

  /**
   * If the table supports purging, drop a partition with specified name and completely remove
   * partition data by skipping a trash.
   *
   * @param partitionName The name of the partition.
   * @return true if a partition was deleted, false if the partition did not exist.
   * @throws NoSuchPartitionException If the partition does not exist.
   * @throws UnsupportedOperationException If partition purging is not supported.
   */
  default boolean purgePartition(String partitionName)
      throws NoSuchPartitionException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Partition purging is not supported");
  }
}
