/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.rel.SupportsPartitions;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.storage.IdGenerator;

public class PartitionOperationDispatcher extends OperationDispatcher
    implements PartitionDispatcher {

  /**
   * Creates a new PartitionOperationDispatcher.
   *
   * @param catalogManager The CatalogManager instance to be used for partition operations.
   * @param store The EntityStore instance to be used for partition operations.
   * @param idGenerator The IdGenerator instance to be used for partition operations.
   */
  public PartitionOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
  }

  @Override
  public String[] listPartitionNames(NameIdentifier tableIdent) {
    return doWithTable(
        tableIdent, SupportsPartitions::listPartitionNames, NoSuchTableException.class);
  }

  @Override
  public Partition[] listPartitions(NameIdentifier tableIdent) {
    return doWithTable(tableIdent, SupportsPartitions::listPartitions, NoSuchTableException.class);
  }

  @Override
  public Partition getPartition(NameIdentifier tableIdent, String partitionName)
      throws NoSuchPartitionException {
    return doWithTable(
        tableIdent, p -> p.getPartition(partitionName), NoSuchPartitionException.class);
  }

  @Override
  public Partition addPartition(NameIdentifier tableIdent, Partition partition)
      throws PartitionAlreadyExistsException {
    return doWithTable(
        tableIdent, p -> p.addPartition(partition), PartitionAlreadyExistsException.class);
  }

  @Override
  public boolean dropPartition(NameIdentifier tableIdent, String partitionName) {
    return doWithTable(
        tableIdent, p -> p.dropPartition(partitionName), NoSuchPartitionException.class);
  }

  @Override
  public boolean purgePartition(NameIdentifier tableIdent, String partitionName)
      throws UnsupportedOperationException {
    return doWithTable(
        tableIdent, p -> p.purgePartition(partitionName), NoSuchPartitionException.class);
  }
}
