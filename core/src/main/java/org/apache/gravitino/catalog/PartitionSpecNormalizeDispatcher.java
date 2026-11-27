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
import static org.apache.gravitino.catalog.CapabilityHelpers.withCapability;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.rel.expressions.transforms.Transform;

/** Normalizes table identifiers before partition-spec reads. */
public class PartitionSpecNormalizeDispatcher implements PartitionSpecDispatcher {

  private final CatalogManager catalogManager;
  private final PartitionSpecDispatcher dispatcher;

  /**
   * Creates a normalizing decorator.
   *
   * @param dispatcher The underlying partition-spec dispatcher.
   * @param catalogManager Used to resolve catalog name capabilities.
   */
  public PartitionSpecNormalizeDispatcher(
      PartitionSpecDispatcher dispatcher, CatalogManager catalogManager) {
    this.dispatcher = dispatcher;
    this.catalogManager = catalogManager;
  }

  @Override
  public int defaultPartitionSpecId(NameIdentifier tableIdent) {
    return dispatcher.defaultPartitionSpecId(normalizeTableIdent(tableIdent));
  }

  @Override
  public Map<Integer, Transform[]> partitionSpecs(NameIdentifier tableIdent) {
    return dispatcher.partitionSpecs(normalizeTableIdent(tableIdent));
  }

  private NameIdentifier normalizeTableIdent(NameIdentifier tableIdent) {
    return withCapability(
        tableIdent,
        catalogManager,
        capabilities -> applyCaseSensitive(tableIdent, Capability.Scope.TABLE, capabilities));
  }
}
