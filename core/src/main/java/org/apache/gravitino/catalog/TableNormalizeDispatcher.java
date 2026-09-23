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
import static org.apache.gravitino.catalog.CapabilityHelpers.getCapability;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
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
import org.apache.gravitino.utils.NameIdentifierUtil;

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
    return dispatcher.loadTable(resolvePhysicalName(normalizeCaseSensitive(ident)));
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
    Capability capability = getCapability(ident, catalogManager);
    return dispatcher.createTable(
        applyCapabilities(ident, Capability.Scope.TABLE, capability),
        applyCapabilities(columns, capability),
        comment,
        properties,
        applyCapabilities(partitions, capability),
        applyCapabilities(distribution, capability),
        applyCapabilities(sortOrders, capability),
        applyCapabilities(indexes, capability));
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    Capability capability = getCapability(ident, catalogManager);
    return dispatcher.alterTable(
        // The constraints of the name spec may be more strict than underlying catalog,
        // and for compatibility reasons, we only apply case-sensitive capabilities here.
        resolvePhysicalName(normalizeCaseSensitive(ident)), applyCapabilities(capability, changes));
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    return dispatcher.dropTable(resolvePhysicalName(normalizeNameIdentifier(ident)));
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    return dispatcher.purgeTable(resolvePhysicalName(normalizeNameIdentifier(ident)));
  }

  @Override
  public boolean tableExists(NameIdentifier ident) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.tableExists(resolvePhysicalName(normalizeCaseSensitive(ident)));
  }

  private Namespace normalizeCaseSensitive(Namespace namespace) {
    Capability capabilities = getCapability(NameIdentifier.of(namespace.levels()), catalogManager);
    return applyCaseSensitive(namespace, Capability.Scope.TABLE, capabilities);
  }

  private NameIdentifier normalizeCaseSensitive(NameIdentifier tableIdent) {
    Capability capability = getCapability(tableIdent, catalogManager);
    return applyCaseSensitive(tableIdent, Capability.Scope.TABLE, capability);
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier tableIdent) {
    Capability capability = getCapability(tableIdent, catalogManager);
    return applyCapabilities(tableIdent, Capability.Scope.TABLE, capability);
  }

  /**
   * Maps a normalized table identifier to the identifier under which the table is physically stored
   * by the catalog's backend, via {@link org.apache.gravitino.rel.TableCatalog#resolveTableName}.
   *
   * <p>For catalogs that do not override that hook (the default), this returns {@code ident}
   * unchanged without touching the backend. Resolving here — before the identifier is handed to the
   * downstream dispatcher — ensures the same physical identifier drives both the underlying catalog
   * operation and the Gravitino entity store key, so the two never diverge.
   */
  private NameIdentifier resolvePhysicalName(NameIdentifier ident) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(ident);
    try {
      return catalogManager.doWithCatalogWrapper(
          catalogIdent,
          wrapper -> wrapper.doWithTableOps(tableOps -> tableOps.resolveTableName(ident)));
    } catch (RuntimeException e) {
      // Preserve typed exceptions the caller expects (e.g. NoSuchCatalogException, and any
      // GravitinoRuntimeException the connector already produced via its exception mapper such as a
      // NoSuchTableException). Only genuinely unexpected checked exceptions from
      // doWithCatalogWrapper are wrapped below, matching CapabilityHelpers.getCapability in this
      // package.
      throw e;
    } catch (Exception e) {
      throw new GravitinoRuntimeException(
          e, "Failed to resolve physical table name for: %s", ident);
    }
  }
}
