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

import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

public class IcebergTableOperationExecutor implements IcebergTableOperationDispatcher {

  private IcebergCatalogWrapperManager icebergCatalogWrapperManager;

  public IcebergTableOperationExecutor(IcebergCatalogWrapperManager icebergCatalogWrapperManager) {
    this.icebergCatalogWrapperManager = icebergCatalogWrapperManager;
  }

  @Override
  public LoadTableResponse createTable(
      IcebergRequestContext context, Namespace namespace, CreateTableRequest createTableRequest) {
    try {
      // Execute the catalog operation within the authenticated user's security context
      String userName = context.userName();
      UserPrincipal principal =
          new UserPrincipal(userName != null ? userName : AuthConstants.ANONYMOUS_USER);

      return PrincipalUtils.doAs(
          principal,
          () -> {
            return icebergCatalogWrapperManager
                .getCatalogWrapper(context.catalogName())
                .createTable(namespace, createTableRequest, context.requestCredentialVending());
          });
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException("Failed to create table", e);
    }
  }

  @Override
  public LoadTableResponse updateTable(
      IcebergRequestContext context,
      TableIdentifier tableIdentifier,
      UpdateTableRequest updateTableRequest) {
    try {
      // Execute the catalog operation within the authenticated user's security context
      String userName = context.userName();
      UserPrincipal principal =
          new UserPrincipal(userName != null ? userName : AuthConstants.ANONYMOUS_USER);

      return PrincipalUtils.doAs(
          principal,
          () -> {
            return icebergCatalogWrapperManager
                .getCatalogWrapper(context.catalogName())
                .updateTable(tableIdentifier, updateTableRequest);
          });
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException("Failed to update table", e);
    }
  }

  @Override
  public void dropTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier, boolean purgeRequested) {
    if (purgeRequested) {
      icebergCatalogWrapperManager
          .getCatalogWrapper(context.catalogName())
          .purgeTable(tableIdentifier);
    } else {
      icebergCatalogWrapperManager
          .getCatalogWrapper(context.catalogName())
          .dropTable(tableIdentifier);
    }
  }

  @Override
  public LoadTableResponse loadTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .loadTable(tableIdentifier, context.requestCredentialVending());
  }

  @Override
  public ListTablesResponse listTable(IcebergRequestContext context, Namespace namespace) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .listTable(namespace);
  }

  @Override
  public boolean tableExists(IcebergRequestContext context, TableIdentifier tableIdentifier) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .tableExists(tableIdentifier);
  }

  @Override
  public void renameTable(IcebergRequestContext context, RenameTableRequest renameTableRequest) {
    icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .renameTable(renameTableRequest);
  }
}
