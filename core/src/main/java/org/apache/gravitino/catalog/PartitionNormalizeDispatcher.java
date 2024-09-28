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

import static org.apache.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;
import static org.apache.gravitino.catalog.CapabilityHelpers.applyCaseSensitiveOnName;
import static org.apache.gravitino.catalog.CapabilityHelpers.getCapability;

import java.util.Arrays;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.partitions.Partition;

public class PartitionNormalizeDispatcher implements PartitionDispatcher {
  private final CatalogManager catalogManager;
  private final PartitionDispatcher dispatcher;

  public PartitionNormalizeDispatcher(
      PartitionDispatcher dispatcher, CatalogManager catalogManager) {
    this.dispatcher = dispatcher;
    this.catalogManager = catalogManager;
  }

  @Override
  public String[] listPartitionNames(NameIdentifier tableIdent) {
    Capability capabilities = getCapability(tableIdent, catalogManager);
    String[] partitionNames =
        dispatcher.listPartitionNames(
            applyCaseSensitive(tableIdent, Capability.Scope.TABLE, capabilities));
    return Arrays.stream(partitionNames)
        .map(
            partitionName ->
                applyCaseSensitiveOnName(Capability.Scope.PARTITION, partitionName, capabilities))
        .toArray(String[]::new);
  }

  @Override
  public Partition[] listPartitions(NameIdentifier tableIdent) {
    Capability capabilities = getCapability(tableIdent, catalogManager);
    Partition[] partitions =
        dispatcher.listPartitions(
            CapabilityHelpers.applyCaseSensitive(tableIdent, Capability.Scope.TABLE, capabilities));
    return applyCaseSensitive(partitions, capabilities);
  }

  @Override
  public Partition getPartition(NameIdentifier tableIdent, String partitionName)
      throws NoSuchPartitionException {
    Capability capabilities = getCapability(tableIdent, catalogManager);
    return dispatcher.getPartition(
        CapabilityHelpers.applyCaseSensitive(tableIdent, Capability.Scope.TABLE, capabilities),
        applyCaseSensitiveOnName(Capability.Scope.PARTITION, partitionName, capabilities));
  }

  @Override
  public Partition addPartition(NameIdentifier tableIdent, Partition partition)
      throws PartitionAlreadyExistsException {
    Capability capabilities = getCapability(tableIdent, catalogManager);
    return dispatcher.addPartition(
        CapabilityHelpers.applyCaseSensitive(tableIdent, Capability.Scope.TABLE, capabilities),
        applyCaseSensitive(partition, capabilities));
  }

  @Override
  public boolean dropPartition(NameIdentifier tableIdent, String partitionName) {
    Capability capabilities = getCapability(tableIdent, catalogManager);
    return dispatcher.dropPartition(
        CapabilityHelpers.applyCaseSensitive(tableIdent, Capability.Scope.TABLE, capabilities),
        applyCaseSensitiveOnName(Capability.Scope.PARTITION, partitionName, capabilities));
  }

  @Override
  public boolean purgePartition(NameIdentifier tableIdent, String partitionName)
      throws UnsupportedOperationException {
    Capability capabilities = getCapability(tableIdent, catalogManager);
    return dispatcher.purgePartition(
        CapabilityHelpers.applyCaseSensitive(tableIdent, Capability.Scope.TABLE, capabilities),
        applyCaseSensitiveOnName(Capability.Scope.PARTITION, partitionName, capabilities));
  }
}
