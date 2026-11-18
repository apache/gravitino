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
import static org.apache.gravitino.catalog.CapabilityHelpers.withCapability;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.partitions.Partition;

/**
 * Note on list operations: names returned by list methods (e.g. {@link
 * #listPartitionNames(NameIdentifier)}, {@link #listPartitions(NameIdentifier)}) are assumed to
 * already be in their canonical, legal form and are not re-normalized here.
 */
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
    return dispatcher.listPartitionNames(normalizeTableIdent(tableIdent));
  }

  @Override
  public Partition[] listPartitions(NameIdentifier tableIdent) {
    return dispatcher.listPartitions(normalizeTableIdent(tableIdent));
  }

  @Override
  public Partition getPartition(NameIdentifier tableIdent, String partitionName)
      throws NoSuchPartitionException {
    Pair<NameIdentifier, String> normalized = normalizePartitionName(tableIdent, partitionName);
    return dispatcher.getPartition(normalized.getLeft(), normalized.getRight());
  }

  @Override
  public Partition addPartition(NameIdentifier tableIdent, Partition partition)
      throws PartitionAlreadyExistsException {
    Pair<NameIdentifier, Partition> normalized =
        withCapability(
            tableIdent,
            catalogManager,
            capabilities ->
                Pair.of(
                    applyCaseSensitive(tableIdent, Capability.Scope.TABLE, capabilities),
                    applyCaseSensitive(partition, capabilities)));
    return dispatcher.addPartition(normalized.getLeft(), normalized.getRight());
  }

  @Override
  public boolean dropPartition(NameIdentifier tableIdent, String partitionName) {
    Pair<NameIdentifier, String> normalized = normalizePartitionName(tableIdent, partitionName);
    return dispatcher.dropPartition(normalized.getLeft(), normalized.getRight());
  }

  @Override
  public boolean purgePartition(NameIdentifier tableIdent, String partitionName)
      throws UnsupportedOperationException {
    Pair<NameIdentifier, String> normalized = normalizePartitionName(tableIdent, partitionName);
    return dispatcher.purgePartition(normalized.getLeft(), normalized.getRight());
  }

  private NameIdentifier normalizeTableIdent(NameIdentifier tableIdent) {
    return withCapability(
        tableIdent,
        catalogManager,
        capabilities -> applyCaseSensitive(tableIdent, Capability.Scope.TABLE, capabilities));
  }

  private Pair<NameIdentifier, String> normalizePartitionName(
      NameIdentifier tableIdent, String partitionName) {
    return withCapability(
        tableIdent,
        catalogManager,
        capabilities ->
            Pair.of(
                applyCaseSensitive(tableIdent, Capability.Scope.TABLE, capabilities),
                applyCaseSensitiveOnName(Capability.Scope.PARTITION, partitionName, capabilities)));
  }
}
