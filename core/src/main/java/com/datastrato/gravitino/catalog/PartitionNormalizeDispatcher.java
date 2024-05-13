/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCaseSensitiveOnName;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.rel.partitions.IdentityPartition;
import com.datastrato.gravitino.rel.partitions.ListPartition;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.partitions.Partitions;
import com.datastrato.gravitino.rel.partitions.RangePartition;
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
            CapabilityHelpers.applyCaseSensitive(tableIdent, Capability.Scope.TABLE, dispatcher));
    return applyCaseSensitive(tableIdent, partitionNames);
  }

  @Override
  public Partition[] listPartitions(NameIdentifier tableIdent) {
    Partition[] partitions =
        dispatcher.listPartitions(
            CapabilityHelpers.applyCaseSensitive(tableIdent, Capability.Scope.TABLE, dispatcher));
    return applyCaseSensitive(tableIdent, partitions);
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
        applyCaseSensitive(tableIdent, partition));
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

  private String[] applyCaseSensitive(NameIdentifier tableIdent, String[] partitionNames) {
    Capability capabilities = dispatcher.getCatalogCapability(tableIdent);
    return Arrays.stream(partitionNames)
        .map(
            partitionName ->
                applyCaseSensitiveOnName(Capability.Scope.PARTITION, partitionName, capabilities))
        .toArray(String[]::new);
  }

  private Partition[] applyCaseSensitive(NameIdentifier tableIdent, Partition[] partitions) {
    boolean caseSensitive =
        dispatcher
            .getCatalogCapability(tableIdent)
            .caseSensitiveOnName(Capability.Scope.PARTITION)
            .supported();
    return Arrays.stream(partitions)
        .map(partition -> applyCaseSensitive(partition, caseSensitive))
        .toArray(Partition[]::new);
  }

  private Partition applyCaseSensitive(NameIdentifier tableIdent, Partition partition) {
    boolean caseSensitive =
        dispatcher
            .getCatalogCapability(tableIdent)
            .caseSensitiveOnName(Capability.Scope.PARTITION)
            .supported();
    return applyCaseSensitive(partition, caseSensitive);
  }

  private Partition applyCaseSensitive(Partition partition, boolean caseSensitive) {
    String newName = caseSensitive ? partition.name() : partition.name().toLowerCase();
    if (partition instanceof IdentityPartition) {
      IdentityPartition identityPartition = (IdentityPartition) partition;
      return Partitions.identity(
          newName,
          identityPartition.fieldNames(),
          identityPartition.values(),
          identityPartition.properties());

    } else if (partition instanceof ListPartition) {
      ListPartition listPartition = (ListPartition) partition;
      return Partitions.list(newName, listPartition.lists(), listPartition.properties());

    } else if (partition instanceof RangePartition) {
      RangePartition rangePartition = (RangePartition) partition;
      return Partitions.range(
          newName, rangePartition.upper(), rangePartition.lower(), rangePartition.properties());

    } else {
      throw new IllegalArgumentException("Unknown partition type: " + partition.getClass());
    }
  }
}
