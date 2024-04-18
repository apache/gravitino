/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

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

import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCapabilities;

public class TableStandardizedDispatcher implements TableDispatcher {

  private final TableOperationDispatcher dispatcher;

  public TableStandardizedDispatcher(TableOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    Capability capability = dispatcher.getCatalogCapability(namespace);
    Namespace standardizedNamespace = applyCapabilities(namespace, Capability.Scope.TABLE, capability);
    NameIdentifier[] identifiers = dispatcher.listTables(standardizedNamespace);
    return applyCapabilities(identifiers, Capability.Scope.TABLE, capability);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    return dispatcher.loadTable(standardizeNameIdentifier(ident));
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
  public Table alterTable(NameIdentifier ident, TableChange... changes) throws NoSuchTableException, IllegalArgumentException {
    Capability capability = dispatcher.getCatalogCapability(ident);
    return dispatcher.alterTable(
        applyCapabilities(ident, Capability.Scope.TABLE, capability),
        applyCapabilities(capability, changes));
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    return dispatcher.dropTable(standardizeNameIdentifier(ident));
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    return dispatcher.purgeTable(standardizeNameIdentifier(ident));
  }

  @Override
  public boolean tableExists(NameIdentifier ident) {
    return dispatcher.tableExists(standardizeNameIdentifier(ident));
  }

  private NameIdentifier standardizeNameIdentifier(NameIdentifier ident) {
    Capability capability = dispatcher.getCatalogCapability(ident);
    return applyCapabilities(ident, Capability.Scope.TABLE, capability);
  }
}
