/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import com.datastrato.gravitino.catalog.TableOperations;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.rel.SupportsPartitions;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.thrift.TException;

public class HiveTableOperations implements TableOperations, SupportsPartitions {

  private final HiveTable table;

  public HiveTableOperations(HiveTable table) {
    Preconditions.checkArgument(table != null, "table must not be null");
    this.table = table;
  }

  @Override
  public String[] listPartitionNames() {
    try {
      return table
          .clientPool()
          .run(
              c ->
                  c.listPartitionNames(table.schemaName(), table.name(), (short) -1)
                      .toArray(new String[0]));
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(
          "Failed to list partition names of table " + table.name() + "from Hive Metastore", e);
    }
  }

  @Override
  public Partition[] listPartitions() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition addPartition(Partition partition) throws PartitionAlreadyExistsException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropPartition(String partitionName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    if (table.clientPool() != null) {
      table.clientPool().close();
      table.setClientPool(null);
    }
  }
}
