/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
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
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.dropTable(applyCaseSensitive(ident, Capability.Scope.TABLE, dispatcher));
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

  @Override
  public boolean importTable(NameIdentifier identifier) {
    return dispatcher.importTable(
        applyCaseSensitive(identifier, Capability.Scope.TABLE, dispatcher));
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier ident) {
    Capability capability = dispatcher.getCatalogCapability(ident);
    return applyCapabilities(ident, Capability.Scope.TABLE, capability);
  }
}
