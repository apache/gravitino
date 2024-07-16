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

import java.util.Arrays;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.partitions.Partition;

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
