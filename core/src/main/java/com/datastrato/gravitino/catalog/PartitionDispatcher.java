/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.rel.SupportsPartitions;
import com.datastrato.gravitino.rel.partitions.Partition;

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
   * @throws NoSuchPartitionException
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
   * @return True if the partition was purged, false if the partition does not exist.
   * @throws UnsupportedOperationException If partition purging is not supported.
   */
  default boolean purgePartition(NameIdentifier tableIdent, String partitionName)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Partition purging is not supported");
  }
}
