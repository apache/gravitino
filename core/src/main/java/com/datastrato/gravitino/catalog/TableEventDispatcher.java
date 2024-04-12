/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.listener.event.CreateTableEvent;
import com.datastrato.gravitino.listener.event.CreateTableFailureEvent;
import com.datastrato.gravitino.listener.event.DropTableEvent;
import com.datastrato.gravitino.listener.event.DropTableFailureEvent;
import com.datastrato.gravitino.listener.impl.EventBus;
import com.datastrato.gravitino.listener.info.TableInfo;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.util.Map;

/**
 * {@code TableEventDispatcher} is a decorator for {@link TableDispatcher} that not only delegates
 * table operations to the underlying catalog dispatcher but also dispatches corresponding events to
 * an {@link EventBus} after each operation is completed. This allows for event-driven workflows or
 * monitoring of table operations.
 */
public class TableEventDispatcher implements TableDispatcher {
  private final EventBus eventBus;
  private final TableDispatcher dispatcher;

  /**
   * Constructs a TableEventDispatcher with a specified EventBus and TableCatalog.
   *
   * @param eventBus The EventBus to which events will be dispatched.
   * @param dispatcher The underlying {@link TableOperationDispatcher} that will perform the actual
   *     table operations.
   */
  public TableEventDispatcher(EventBus eventBus, TableDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    return dispatcher.listTables(namespace);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    return dispatcher.loadTable(ident);
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
    try {
      Table table =
          dispatcher.createTable(
              ident, columns, comment, properties, partitions, distribution, sortOrders, indexes);
      eventBus.dispatchEvent(
          new CreateTableEvent(PrincipalUtils.getCurrentUserName(), ident, new TableInfo(table)));
      return table;
    } catch (Exception e) {
      TableInfo createTableRequest =
          new TableInfo(
              ident.name(),
              columns,
              comment,
              properties,
              partitions,
              distribution,
              sortOrders,
              indexes,
              null);
      eventBus.dispatchEvent(
          new CreateTableFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, e, createTableRequest));
      throw e;
    }
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    return dispatcher.alterTable(ident, changes);
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    try {
      boolean isExists = dispatcher.dropTable(ident);
      eventBus.dispatchEvent(
          new DropTableEvent(PrincipalUtils.getCurrentUserName(), ident, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropTableFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) {
    return dispatcher.purgeTable(ident);
  }

  @Override
  public boolean tableExists(NameIdentifier ident) {
    return dispatcher.tableExists(ident);
  }
}
