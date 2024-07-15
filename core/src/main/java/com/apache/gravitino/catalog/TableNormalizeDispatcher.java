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
package com.apache.gravitino.catalog;

import static com.apache.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static com.apache.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;

import com.apache.gravitino.NameIdentifier;
import com.apache.gravitino.Namespace;
import com.apache.gravitino.connector.capability.Capability;
import com.apache.gravitino.exceptions.NoSuchSchemaException;
import com.apache.gravitino.exceptions.NoSuchTableException;
import com.apache.gravitino.exceptions.TableAlreadyExistsException;
import com.apache.gravitino.rel.Column;
import com.apache.gravitino.rel.Table;
import com.apache.gravitino.rel.TableChange;
import com.apache.gravitino.rel.expressions.distributions.Distribution;
import com.apache.gravitino.rel.expressions.sorts.SortOrder;
import com.apache.gravitino.rel.expressions.transforms.Transform;
import com.apache.gravitino.rel.indexes.Index;
import java.util.Map;

public class TableNormalizeDispatcher implements TableDispatcher {

  private final TableOperationDispatcher dispatcher;

  public TableNormalizeDispatcher(TableOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    Namespace caseSensitiveNs = applyCaseSensitive(namespace, Capability.Scope.TABLE, dispatcher);
    NameIdentifier[] identifiers = dispatcher.listTables(caseSensitiveNs);
    return applyCaseSensitive(identifiers, Capability.Scope.TABLE, dispatcher);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.loadTable(applyCaseSensitive(ident, Capability.Scope.TABLE, dispatcher));
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
    Capability capability = dispatcher.getCatalogCapability(ident);
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
    Capability capability = dispatcher.getCatalogCapability(ident);
    return dispatcher.alterTable(
        // The constraints of the name spec may be more strict than underlying catalog,
        // and for compatibility reasons, we only apply case-sensitive capabilities here.
        applyCaseSensitive(ident, Capability.Scope.TABLE, dispatcher),
        applyCapabilities(capability, changes));
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
    return dispatcher.tableExists(applyCaseSensitive(ident, Capability.Scope.TABLE, dispatcher));
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier ident) {
    Capability capability = dispatcher.getCatalogCapability(ident);
    return applyCapabilities(ident, Capability.Scope.TABLE, capability);
  }
}
