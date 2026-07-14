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

import static org.apache.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static org.apache.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;
import static org.apache.gravitino.catalog.CapabilityHelpers.withCapability;

import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

public class TableNormalizeDispatcher implements TableDispatcher {
  private final CatalogManager catalogManager;
  private final TableDispatcher dispatcher;

  public TableNormalizeDispatcher(TableDispatcher dispatcher, CatalogManager catalogManager) {
    this.catalogManager = catalogManager;
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    Namespace caseSensitiveNs = normalizeCaseSensitive(namespace);
    NameIdentifier[] identifiers = dispatcher.listTables(caseSensitiveNs);
    return normalizeCaseSensitive(identifiers);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.loadTable(normalizeCaseSensitive(ident));
  }

  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    NameIdentifier normalizedIdent =
        withCapability(
            ident, catalogManager, cap -> applyCapabilities(ident, Capability.Scope.TABLE, cap));
    Column[] normalizedColumns =
        withCapability(ident, catalogManager, cap -> applyCapabilities(columns, cap));
    Transform[] normalizedPartitions =
        withCapability(ident, catalogManager, cap -> applyCapabilities(partitions, cap));
    Distribution normalizedDistribution =
        withCapability(ident, catalogManager, cap -> applyCapabilities(distribution, cap));
    SortOrder[] normalizedSortOrders =
        withCapability(ident, catalogManager, cap -> applyCapabilities(sortOrders, cap));
    Index[] normalizedIndexes =
        withCapability(ident, catalogManager, cap -> applyCapabilities(indexes, cap));
    return dispatcher.createTable(
        normalizedIdent,
        normalizedColumns,
        comment,
        properties,
        normalizedPartitions,
        normalizedDistribution,
        normalizedSortOrders,
        normalizedIndexes);
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    Pair<NameIdentifier, TableChange[]> normalized =
        withCapability(
            ident,
            catalogManager,
            cap ->
                Pair.of(
                    applyCaseSensitive(ident, Capability.Scope.TABLE, cap),
                    applyCapabilities(cap, changes)));
    return dispatcher.alterTable(normalized.getLeft(), normalized.getRight());
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    return dispatcher.dropTable(normalizeNameIdentifier(ident));
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    return dispatcher.purgeTable(normalizeNameIdentifier(ident));
  }

  @Override
  public boolean tableExists(NameIdentifier ident) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.tableExists(normalizeCaseSensitive(ident));
  }

  private Namespace normalizeCaseSensitive(Namespace namespace) {
    return withCapability(
        NameIdentifier.of(namespace.levels()),
        catalogManager,
        cap -> applyCaseSensitive(namespace, Capability.Scope.TABLE, cap));
  }

  private NameIdentifier normalizeCaseSensitive(NameIdentifier tableIdent) {
    return withCapability(
        tableIdent,
        catalogManager,
        cap -> applyCaseSensitive(tableIdent, Capability.Scope.TABLE, cap));
  }

  private NameIdentifier[] normalizeCaseSensitive(NameIdentifier[] tableIdents) {
    if (ArrayUtils.isEmpty(tableIdents)) {
      return tableIdents;
    }

    return withCapability(
        tableIdents[0],
        catalogManager,
        cap -> applyCaseSensitive(tableIdents, Capability.Scope.TABLE, cap));
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier tableIdent) {
    return withCapability(
        tableIdent,
        catalogManager,
        cap -> applyCapabilities(tableIdent, Capability.Scope.TABLE, cap));
  }
}
