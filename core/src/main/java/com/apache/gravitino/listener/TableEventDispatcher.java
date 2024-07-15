/*
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

package com.apache.gravitino.listener;

import com.apache.gravitino.NameIdentifier;
import com.apache.gravitino.Namespace;
import com.apache.gravitino.catalog.TableDispatcher;
import com.apache.gravitino.catalog.TableOperationDispatcher;
import com.apache.gravitino.exceptions.NoSuchSchemaException;
import com.apache.gravitino.exceptions.NoSuchTableException;
import com.apache.gravitino.exceptions.TableAlreadyExistsException;
import com.apache.gravitino.listener.api.event.AlterTableEvent;
import com.apache.gravitino.listener.api.event.AlterTableFailureEvent;
import com.apache.gravitino.listener.api.event.CreateTableEvent;
import com.apache.gravitino.listener.api.event.CreateTableFailureEvent;
import com.apache.gravitino.listener.api.event.DropTableEvent;
import com.apache.gravitino.listener.api.event.DropTableFailureEvent;
import com.apache.gravitino.listener.api.event.ListTableEvent;
import com.apache.gravitino.listener.api.event.ListTableFailureEvent;
import com.apache.gravitino.listener.api.event.LoadTableEvent;
import com.apache.gravitino.listener.api.event.LoadTableFailureEvent;
import com.apache.gravitino.listener.api.event.PurgeTableEvent;
import com.apache.gravitino.listener.api.event.PurgeTableFailureEvent;
import com.apache.gravitino.listener.api.info.TableInfo;
import com.apache.gravitino.rel.Column;
import com.apache.gravitino.rel.Table;
import com.apache.gravitino.rel.TableChange;
import com.apache.gravitino.rel.expressions.distributions.Distribution;
import com.apache.gravitino.rel.expressions.sorts.SortOrder;
import com.apache.gravitino.rel.expressions.transforms.Transform;
import com.apache.gravitino.rel.indexes.Index;
import com.apache.gravitino.utils.PrincipalUtils;
import java.util.Map;

/**
 * {@code TableEventDispatcher} is a decorator for {@link TableDispatcher} that not only delegates
 * table operations to the underlying catalog dispatcher but also dispatches corresponding events to
 * an {@link com.apache.gravitino.listener.EventBus} after each operation is completed. This allows
 * for event-driven workflows or monitoring of table operations.
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
