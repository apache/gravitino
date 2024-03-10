/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.rel.partitions.Partition;
import java.util.List;

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
   *   <li>{@link com.datastrato.gravitino.rel.partitions.IdentityPartition}
   *   <li>{@link com.datastrato.gravitino.rel.partitions.ListPartition}
   *   <li>{@link com.datastrato.gravitino.rel.partitions.RangePartition}
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
   * @param ifExists If true, will not throw NoSuchPartitionException if the partition not exists
   * @return true if a partition was deleted.
   */
  boolean dropPartition(String partitionName, boolean ifExists) throws NoSuchPartitionException;

  /**
   * Drop partitions with specified names.
   *
   * @param partitionNames the names of the partition
   * @param ifExists If true, will not throw NoSuchPartitionException if the partition not exists
   * @return true if all partitions was deleted.
   */
  boolean dropPartitions(List<String> partitionNames, boolean ifExists)
      throws NoSuchPartitionException, UnsupportedOperationException;

  /**
   * If the table supports purging, drop a partition with specified name and completely remove
   * partition data by skipping a trash.
   *
   * @param partitionName The name of the partition.
   * @param ifExists If true, will not throw NoSuchPartitionException if the partition not exists
   * @return true if a partition was deleted, false if the partition did not exist.
   * @throws NoSuchPartitionException If the partition does not exist.
   * @throws UnsupportedOperationException If partition purging is not supported.
   */
  default boolean purgePartition(String partitionName, boolean ifExists)
      throws NoSuchPartitionException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Partition purging is not supported");
  }

  /**
   * If the table supports purging, drop partitions with specified names and completely remove
   * partition data by skipping a trash.
   *
   * @param partitionNames The name of the partition.
   * @param ifExists If true, will not throw NoSuchPartitionException if the partition not exists
   * @return true if a partition was deleted, false if the partition did not exist.
   * @throws NoSuchPartitionException If the partition does not exist.
   * @throws UnsupportedOperationException If partition purging is not supported.
   */
  default boolean purgePartitions(List<String> partitionNames, boolean ifExists)
      throws NoSuchPartitionException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Partitions purging is not supported");
  }
}
