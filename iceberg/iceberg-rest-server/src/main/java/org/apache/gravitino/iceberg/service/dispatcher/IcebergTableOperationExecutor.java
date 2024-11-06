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

import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.IcebergRequestContext;
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
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.getCatalogName())
        .createTable(namespace, createTableRequest);
  }

  @Override
  public LoadTableResponse updateTable(
      IcebergRequestContext context,
      TableIdentifier tableIdentifier,
      UpdateTableRequest updateTableRequest) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.getCatalogName())
        .updateTable(tableIdentifier, updateTableRequest);
  }

  @Override
  public void dropTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier, boolean purgeRequested) {
    if (purgeRequested) {
      icebergCatalogWrapperManager
          .getCatalogWrapper(context.getCatalogName())
          .purgeTable(tableIdentifier);
    } else {
      icebergCatalogWrapperManager
          .getCatalogWrapper(context.getCatalogName())
          .dropTable(tableIdentifier);
    }
  }

  @Override
  public LoadTableResponse loadTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.getCatalogName())
        .loadTable(tableIdentifier);
  }

  @Override
  public ListTablesResponse listTable(IcebergRequestContext context, Namespace namespace) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.getCatalogName())
        .listTable(namespace);
  }

  @Override
  public boolean tableExists(IcebergRequestContext context, TableIdentifier tableIdentifier) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.getCatalogName())
        .tableExists(tableIdentifier);
  }

  @Override
  public void renameTable(IcebergRequestContext context, RenameTableRequest renameTableRequest) {
    icebergCatalogWrapperManager
        .getCatalogWrapper(context.getCatalogName())
        .renameTable(renameTableRequest);
  }
}
