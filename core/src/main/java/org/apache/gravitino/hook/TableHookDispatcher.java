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
package org.apache.gravitino.hook;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
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
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code TableHookDispatcher} is a decorator for {@link TableDispatcher} that not only delegates
 * table operations to the underlying table dispatcher but also executes some hook operations before
 * or after the underlying operations.
 */
public class TableHookDispatcher implements TableDispatcher {
  private final TableDispatcher dispatcher;

  public TableHookDispatcher(TableDispatcher dispatcher) {
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
    // Check whether the current user exists or not
    AuthorizationUtils.checkCurrentUser(
        ident.namespace().level(0), PrincipalUtils.getCurrentUserName());

    Table table =
        dispatcher.createTable(
            ident, columns, comment, properties, partitions, distribution, sortOrders, indexes);

    // Set the creator as the owner of the table.
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      ownerManager.setOwner(
          ident.namespace().level(0),
          NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.TABLE),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
    return table;
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    TableChange.RenameTable lastRenameChange = null;
    List<String> locations = null;
    for (TableChange change : changes) {
      if (change instanceof TableChange.RenameTable) {
        lastRenameChange = (TableChange.RenameTable) change;
      }
    }
    if (lastRenameChange != null) {
      locations = AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.TABLE);
    }
    Table alteredTable = dispatcher.alterTable(ident, changes);

    if (lastRenameChange != null) {
      AuthorizationUtils.authorizationPluginRenamePrivileges(
          ident, Entity.EntityType.TABLE, lastRenameChange.getNewName(), locations);
    }

    return alteredTable;
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    List<String> locations =
        AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.TABLE);
    boolean dropped = dispatcher.dropTable(ident);
    AuthorizationUtils.authorizationPluginRemovePrivileges(
        ident, Entity.EntityType.TABLE, locations);
    return dropped;
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    List<String> locations =
        AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.TABLE);
    boolean purged = dispatcher.purgeTable(ident);
    AuthorizationUtils.authorizationPluginRemovePrivileges(
        ident, Entity.EntityType.TABLE, locations);
    return purged;
  }

  @Override
  public boolean tableExists(NameIdentifier ident) {
    return dispatcher.tableExists(ident);
  }
}
