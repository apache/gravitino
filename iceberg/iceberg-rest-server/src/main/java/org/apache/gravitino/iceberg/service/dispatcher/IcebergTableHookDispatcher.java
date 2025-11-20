/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.service.dispatcher;

import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

public class IcebergTableHookDispatcher implements IcebergTableOperationDispatcher {

  private final IcebergTableOperationDispatcher dispatcher;
  private String metalake;

  public IcebergTableHookDispatcher(IcebergTableOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
    this.metalake = IcebergRESTServerContext.getInstance().metalakeName();
  }

  @Override
  public LoadTableResponse createTable(
      IcebergRequestContext context, Namespace namespace, CreateTableRequest createTableRequest) {
    AuthorizationUtils.checkCurrentUser(metalake, context.userName());

    LoadTableResponse response = dispatcher.createTable(context, namespace, createTableRequest);
    importTable(context.catalogName(), namespace, createTableRequest.name());
    setTableOwner(context.catalogName(), namespace, createTableRequest.name(), context.userName());

    return response;
  }

  @Override
  public LoadTableResponse updateTable(
      IcebergRequestContext context,
      TableIdentifier tableIdentifier,
      UpdateTableRequest updateTableRequest) {
    return dispatcher.updateTable(context, tableIdentifier, updateTableRequest);
  }

  @Override
  public void dropTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier, boolean purgeRequested) {
    dispatcher.dropTable(context, tableIdentifier, purgeRequested);
  }

  @Override
  public LoadTableResponse loadTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier) {
    return dispatcher.loadTable(context, tableIdentifier);
  }

  @Override
  public ListTablesResponse listTable(IcebergRequestContext context, Namespace namespace) {
    return dispatcher.listTable(context, namespace);
  }

  @Override
  public boolean tableExists(IcebergRequestContext context, TableIdentifier tableIdentifier) {
    return dispatcher.tableExists(context, tableIdentifier);
  }

  @Override
  public void renameTable(IcebergRequestContext context, RenameTableRequest renameTableRequest) {
    dispatcher.renameTable(context, renameTableRequest);
  }

  @Override
  public LoadCredentialsResponse getTableCredentials(
      IcebergRequestContext context, TableIdentifier tableIdentifier) {
    return dispatcher.getTableCredentials(context, tableIdentifier);
  }

  private void importTable(String catalogName, Namespace namespace, String tableName) {
    TableDispatcher tableDispatcher = GravitinoEnv.getInstance().tableDispatcher();
    if (tableDispatcher != null) {
      tableDispatcher.loadTable(
          IcebergIdentifierUtils.toGravitinoTableIdentifier(
              metalake, catalogName, TableIdentifier.of(namespace, tableName)));
    }
  }

  private void setTableOwner(
      String catalogName, Namespace namespace, String tableName, String user) {
    // Set the creator as the owner of the table.
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      ownerManager.setOwner(
          metalake,
          NameIdentifierUtil.toMetadataObject(
              IcebergIdentifierUtils.toGravitinoTableIdentifier(
                  metalake, catalogName, TableIdentifier.of(namespace, tableName)),
              Entity.EntityType.TABLE),
          user,
          Owner.Type.USER);
    }
  }
}
