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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.IcebergRequestContext;
import org.apache.gravitino.iceberg.service.IcebergRestUtils;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.IcebergCreateTableEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergDropTableEvent;
import org.apache.gravitino.listener.api.event.IcebergDropTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergDropTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergListTableEvent;
import org.apache.gravitino.listener.api.event.IcebergListTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergListTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadTableEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameTableEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergTableExistsEvent;
import org.apache.gravitino.listener.api.event.IcebergTableExistsFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergTableExistsPreEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTableEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTablePreEvent;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

/**
 * {@code IcebergTableEventDispatcher} is a decorator for {@link IcebergTableOperationExecutor} that
 * not only delegates table operations to the underlying dispatcher but also dispatches
 * corresponding events to an {@link org.apache.gravitino.listener.EventBus}.
 */
public class IcebergTableEventDispatcher implements IcebergTableOperationDispatcher {

  private IcebergTableOperationDispatcher icebergTableOperationDispatcher;
  private EventBus eventBus;
  private String metalakeName;

  public IcebergTableEventDispatcher(
      IcebergTableOperationDispatcher icebergTableOperationDispatcher,
      EventBus eventBus,
      String metalakeName) {
    this.icebergTableOperationDispatcher = icebergTableOperationDispatcher;
    this.eventBus = eventBus;
    this.metalakeName = metalakeName;
  }

  @Override
  public LoadTableResponse createTable(
      IcebergRequestContext context, Namespace namespace, CreateTableRequest createTableRequest) {
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, createTableRequest.name());
    NameIdentifier nameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.getCatalogName(), tableIdentifier);
    eventBus.dispatchEvent(
        new IcebergCreateTablePreEvent(
            PrincipalUtils.getCurrentUserName(), nameIdentifier, createTableRequest));
    LoadTableResponse loadTableResponse;
    try {
      loadTableResponse =
          icebergTableOperationDispatcher.createTable(context, namespace, createTableRequest);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergCreateTableFailureEvent(
              PrincipalUtils.getCurrentUserName(), nameIdentifier, createTableRequest, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergCreateTableEvent(
            PrincipalUtils.getCurrentUserName(),
            nameIdentifier,
            createTableRequest,
            loadTableResponse));
    return loadTableResponse;
  }

  @Override
  public LoadTableResponse updateTable(
      IcebergRequestContext context,
      TableIdentifier tableIdentifier,
      UpdateTableRequest updateTableRequest) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.getCatalogName(), tableIdentifier);
    eventBus.dispatchEvent(
        new IcebergUpdateTablePreEvent(
            PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier, updateTableRequest));
    LoadTableResponse loadTableResponse;
    try {
      loadTableResponse =
          icebergTableOperationDispatcher.updateTable(context, tableIdentifier, updateTableRequest);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergUpdateTableFailureEvent(
              PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier, updateTableRequest, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergUpdateTableEvent(
            PrincipalUtils.getCurrentUserName(),
            gravitinoNameIdentifier,
            updateTableRequest,
            loadTableResponse));
    return loadTableResponse;
  }

  @Override
  public void dropTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier, boolean purgeRequested) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.getCatalogName(), tableIdentifier);
    eventBus.dispatchEvent(
        new IcebergDropTablePreEvent(
            PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier, purgeRequested));
    try {
      icebergTableOperationDispatcher.dropTable(context, tableIdentifier, purgeRequested);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergDropTableFailureEvent(
              PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier, purgeRequested, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergDropTableEvent(
            PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier, purgeRequested));
  }

  @Override
  public LoadTableResponse loadTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.getCatalogName(), tableIdentifier);
    eventBus.dispatchEvent(
        new IcebergLoadTablePreEvent(PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier));
    LoadTableResponse loadTableResponse;
    try {
      loadTableResponse = icebergTableOperationDispatcher.loadTable(context, tableIdentifier);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergLoadTableFailureEvent(
              PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergLoadTableEvent(
            PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier, loadTableResponse));
    return loadTableResponse;
  }

  @Override
  public ListTablesResponse listTable(IcebergRequestContext context, Namespace namespace) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.getCatalogName(), namespace);
    eventBus.dispatchEvent(
        new IcebergListTablePreEvent(PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier));
    ListTablesResponse listTablesResponse;
    try {
      listTablesResponse = icebergTableOperationDispatcher.listTable(context, namespace);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergListTableFailureEvent(
              PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergListTableEvent(PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier));
    return listTablesResponse;
  }

  @Override
  public boolean tableExists(IcebergRequestContext context, TableIdentifier tableIdentifier) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.getCatalogName(), tableIdentifier);
    eventBus.dispatchEvent(
        new IcebergTableExistsPreEvent(
            PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier));
    boolean isExists;
    try {
      isExists = icebergTableOperationDispatcher.tableExists(context, tableIdentifier);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergTableExistsFailureEvent(
              PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergTableExistsEvent(
            PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier, isExists));
    return isExists;
  }

  @Override
  public void renameTable(IcebergRequestContext context, RenameTableRequest renameTableRequest) {
    TableIdentifier sourceTable = renameTableRequest.source();
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.getCatalogName(), sourceTable);
    eventBus.dispatchEvent(
        new IcebergRenameTablePreEvent(
            PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier, renameTableRequest));
    try {
      icebergTableOperationDispatcher.renameTable(context, renameTableRequest);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergRenameTableFailureEvent(
              PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier, renameTableRequest, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergRenameTableEvent(
            PrincipalUtils.getCurrentUserName(), gravitinoNameIdentifier, renameTableRequest));
  }
}
