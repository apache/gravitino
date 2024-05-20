/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.TableDispatcher;
import com.datastrato.gravitino.catalog.TableOperationDispatcher;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.listener.api.event.AlterTableEvent;
import com.datastrato.gravitino.listener.api.event.AlterTableFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateTableEvent;
import com.datastrato.gravitino.listener.api.event.CreateTableFailureEvent;
import com.datastrato.gravitino.listener.api.event.DropTableEvent;
import com.datastrato.gravitino.listener.api.event.DropTableFailureEvent;
import com.datastrato.gravitino.listener.api.event.ListTableEvent;
import com.datastrato.gravitino.listener.api.event.ListTableFailureEvent;
import com.datastrato.gravitino.listener.api.event.LoadTableEvent;
import com.datastrato.gravitino.listener.api.event.LoadTableFailureEvent;
import com.datastrato.gravitino.listener.api.event.PurgeTableEvent;
import com.datastrato.gravitino.listener.api.event.PurgeTableFailureEvent;
import com.datastrato.gravitino.listener.api.info.TableInfo;
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
 * an {@link com.datastrato.gravitino.listener.EventBus} after each operation is completed. This
 * allows for event-driven workflows or monitoring of table operations.
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
    try {
      NameIdentifier[] nameIdentifiers = dispatcher.listTables(namespace);
      eventBus.dispatchEvent(new ListTableEvent(PrincipalUtils.getCurrentUserName(), namespace));
      return nameIdentifiers;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListTableFailureEvent(PrincipalUtils.getCurrentUserName(), namespace, e));
      throw e;
    }
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    try {
      Table table = dispatcher.loadTable(ident);
      eventBus.dispatchEvent(
          new LoadTableEvent(PrincipalUtils.getCurrentUserName(), ident, new TableInfo(table)));
      return table;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new LoadTableFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
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
    try {
      Table table = dispatcher.alterTable(ident, changes);
      eventBus.dispatchEvent(
          new AlterTableEvent(
              PrincipalUtils.getCurrentUserName(), ident, changes, new TableInfo(table)));
      return table;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterTableFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e, changes));
      throw e;
    }
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
    try {
      boolean isExists = dispatcher.purgeTable(ident);
      eventBus.dispatchEvent(
          new PurgeTableEvent(PrincipalUtils.getCurrentUserName(), ident, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new PurgeTableFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public boolean tableExists(NameIdentifier ident) {
    return dispatcher.tableExists(ident);
  }
}
