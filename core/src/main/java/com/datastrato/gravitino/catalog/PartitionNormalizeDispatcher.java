/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;
import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCaseSensitiveOnName;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.rel.partitions.Partition;
import java.util.Arrays;

public class PartitionNormalizeDispatcher implements PartitionDispatcher {

  private final PartitionOperationDispatcher dispatcher;

  public PartitionNormalizeDispatcher(PartitionOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public String[] listPartitionNames(NameIdentifier tableIdent) {
    String[] partitionNames =
        dispatcher.listPartitionNames(
            applyCaseSensitive(tableIdent, Capability.Scope.TABLE, dispatcher));
    Capability capabilities = dispatcher.getCatalogCapability(tableIdent);
    return Arrays.stream(partitionNames)
        .map(
            partitionName ->
                applyCaseSensitiveOnName(Capability.Scope.PARTITION, partitionName, capabilities))
        .toArray(String[]::new);
  }

  @Override
  public Partition[] listPartitions(NameIdentifier tableIdent) {
    Partition[] partitions =
        dispatcher.listPartitions(
            CapabilityHelpers.applyCaseSensitive(tableIdent, Capability.Scope.TABLE, dispatcher));
    return applyCaseSensitive(partitions, dispatcher.getCatalogCapability(tableIdent));
  }

  @Override
  public Partition getPartition(NameIdentifier tableIdent, String partitionName)
      throws NoSuchPartitionException {
    return dispatcher.getPartition(
        CapabilityHelpers.applyCaseSensitive(tableIdent, Capability.Scope.TABLE, dispatcher),
        applyCaseSensitiveOnName(
            Capability.Scope.PARTITION,
            partitionName,
            dispatcher.getCatalogCapability(tableIdent)));
  }

  @Override
  public Partition addPartition(NameIdentifier tableIdent, Partition partition)
      throws PartitionAlreadyExistsException {
    return dispatcher.addPartition(
        CapabilityHelpers.applyCaseSensitive(tableIdent, Capability.Scope.TABLE, dispatcher),
        applyCaseSensitive(partition, dispatcher.getCatalogCapability(tableIdent)));
  }

  @Override
  public boolean dropPartition(NameIdentifier tableIdent, String partitionName) {
    return dispatcher.dropPartition(
        CapabilityHelpers.applyCaseSensitive(tableIdent, Capability.Scope.TABLE, dispatcher),
        applyCaseSensitiveOnName(
            Capability.Scope.PARTITION,
            partitionName,
            dispatcher.getCatalogCapability(tableIdent)));
  }

  @Override
  public boolean purgePartition(NameIdentifier tableIdent, String partitionName)
      throws UnsupportedOperationException {
    return dispatcher.purgePartition(
        CapabilityHelpers.applyCaseSensitive(tableIdent, Capability.Scope.TABLE, dispatcher),
        applyCaseSensitiveOnName(
            Capability.Scope.PARTITION,
            partitionName,
            dispatcher.getCatalogCapability(tableIdent)));
  }
}
