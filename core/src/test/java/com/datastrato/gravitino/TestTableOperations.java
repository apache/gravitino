/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.connector.TableOperations;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.rel.SupportsPartitions;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;

public class TestTableOperations implements TableOperations, SupportsPartitions {

  private static final Map<String, Partition> partitions = Maps.newHashMap();

  @Override
  public String[] listPartitionNames() {
    return partitions.keySet().toArray(new String[0]);
  }

  @Override
  public Partition[] listPartitions() {
    return partitions.values().toArray(new Partition[0]);
  }

  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    if (!partitions.containsKey(partitionName)) {
      throw new NoSuchPartitionException("Partition not found: %s", partitionName);
    }
    return partitions.get(partitionName);
  }

  @Override
  public Partition addPartition(Partition partition) throws PartitionAlreadyExistsException {
    if (partitions.containsKey(partition.name())) {
      throw new PartitionAlreadyExistsException("Partition already exists: %s", partition.name());
    }
    partitions.put(partition.name(), partition);
    return partition;
  }

  @Override
  public boolean dropPartition(String partitionName) {
    if (!partitions.containsKey(partitionName)) {
      return false;
    }
    partitions.remove(partitionName);
    return true;
  }

  @Override
  public void close() throws IOException {
    partitions.clear();
  }
}
