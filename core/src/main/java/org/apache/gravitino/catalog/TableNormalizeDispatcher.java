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
import static org.apache.gravitino.catalog.CapabilityHelpers.resolvePhysicalTableName;
import static org.apache.gravitino.catalog.CapabilityHelpers.withCapability;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

/**
 * Note on list operations: names returned by list methods (e.g. {@link #listTables(Namespace)}) are
 * assumed to already be in their canonical, legal form and are not re-normalized here.
 */
public class TableNormalizeDispatcher implements TableDispatcher {
  private final CatalogManager catalogManager;
  private final TableDispatcher dispatcher;

  public TableNormalizeDispatcher(TableDispatcher dispatcher, CatalogManager catalogManager) {
    this.catalogManager = catalogManager;
    this.dispatcher = dispatcher;
  }

  /** {@inheritDoc} */
  @Override
  public Table importTable(NameIdentifier ident, AuditInfo auditInfo) throws NoSuchTableException {
    return dispatcher.importTable(
        resolvePhysicalTableName(normalizeCaseSensitive(ident), catalogManager), auditInfo);
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    Namespace caseSensitiveNs = normalizeCaseSensitive(namespace);
    return dispatcher.listTables(caseSensitiveNs);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.loadTable(
        resolvePhysicalTableName(normalizeCaseSensitive(ident), catalogManager));
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
    NormalizedTableDefinition normalized =
        withCapability(
            ident,
            catalogManager,
            capability ->
                new NormalizedTableDefinition(
                    applyCapabilities(ident, Capability.Scope.TABLE, capability),
                    applyCapabilities(columns, capability),
                    applyCapabilities(partitions, capability),
                    applyCapabilities(distribution, capability),
                    applyCapabilities(sortOrders, capability),
                    applyCapabilities(indexes, capability)));
    return dispatcher.createTable(
        normalized.ident,
        normalized.columns,
        comment,
        properties,
        normalized.partitions,
        normalized.distribution,
        normalized.sortOrders,
        normalized.indexes);
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities to the name here.
    Pair<NameIdentifier, TableChange[]> normalized =
        withCapability(
            ident,
            catalogManager,
            capability ->
                Pair.of(
                    applyCaseSensitive(ident, Capability.Scope.TABLE, capability),
                    applyCapabilities(capability, changes)));
    return dispatcher.alterTable(
        resolvePhysicalTableName(normalized.getLeft(), catalogManager), normalized.getRight());
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    return dispatcher.dropTable(
        resolvePhysicalTableName(normalizeNameIdentifier(ident), catalogManager));
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    return dispatcher.purgeTable(
        resolvePhysicalTableName(normalizeNameIdentifier(ident), catalogManager));
  }

  @Override
  public boolean tableExists(NameIdentifier ident) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.tableExists(
        resolvePhysicalTableName(normalizeCaseSensitive(ident), catalogManager));
  }

  private Namespace normalizeCaseSensitive(Namespace namespace) {
    return withCapability(
        NameIdentifier.of(namespace.levels()),
        catalogManager,
        capabilities -> applyCaseSensitive(namespace, Capability.Scope.TABLE, capabilities));
  }

  private NameIdentifier normalizeCaseSensitive(NameIdentifier tableIdent) {
    return withCapability(
        tableIdent,
        catalogManager,
        capability -> applyCaseSensitive(tableIdent, Capability.Scope.TABLE, capability));
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier tableIdent) {
    Namespace namespace = tableIdent.namespace();
    return withCapability(
        tableIdent,
        catalogManager,
        capability -> {
          validateExistingName(
              Capability.Scope.SCHEMA, namespace.level(namespace.length() - 1), capability);
          validateExistingName(Capability.Scope.TABLE, tableIdent.name(), capability);
          return applyCaseSensitive(tableIdent, Capability.Scope.TABLE, capability);
        });
  }

  private static void validateExistingName(
      Capability.Scope scope, String name, Capability capability) {
    CapabilityResult result = capability.specificationOnExistingName(scope, name);
    Preconditions.checkArgument(
        result.supported(), result.unsupportedMessage() + " Illegal name: " + name);
  }

  /** The table definition after the catalog capability rules have been applied. */
  private static final class NormalizedTableDefinition {
    private final NameIdentifier ident;
    private final Column[] columns;
    private final Transform[] partitions;
    private final Distribution distribution;
    private final SortOrder[] sortOrders;
    private final Index[] indexes;

    private NormalizedTableDefinition(
        NameIdentifier ident,
        Column[] columns,
        Transform[] partitions,
        Distribution distribution,
        SortOrder[] sortOrders,
        Index[] indexes) {
      this.ident = ident;
      this.columns = columns;
      this.partitions = partitions;
      this.distribution = distribution;
      this.sortOrders = sortOrders;
      this.indexes = indexes;
    }
  }
}
