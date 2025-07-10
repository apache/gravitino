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

import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.IcebergRestUtils;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.BaseEvent;
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
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.listener.api.event.IcebergTableExistsEvent;
import org.apache.gravitino.listener.api.event.IcebergTableExistsFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergTableExistsPreEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTableEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTablePreEvent;
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
            metalakeName, context.catalogName(), tableIdentifier);
    Optional<BaseEvent> transformedEvent =
        eventBus.dispatchEvent(
            new IcebergCreateTablePreEvent(context, nameIdentifier, createTableRequest));
    IcebergCreateTablePreEvent transformedCreateEvent =
        (IcebergCreateTablePreEvent) transformedEvent.get();
    LoadTableResponse loadTableResponse;
    try {
      loadTableResponse =
          icebergTableOperationDispatcher.createTable(
              context, namespace, transformedCreateEvent.createTableRequest());
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergCreateTableFailureEvent(
              context, nameIdentifier, transformedCreateEvent.createTableRequest(), e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergCreateTableEvent(
            context,
            nameIdentifier,
            transformedCreateEvent.createTableRequest(),
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
            metalakeName, context.catalogName(), tableIdentifier);
    Optional<BaseEvent> transformedEvent =
        eventBus.dispatchEvent(
            new IcebergUpdateTablePreEvent(context, gravitinoNameIdentifier, updateTableRequest));
    IcebergUpdateTablePreEvent transformedUpdateEvent =
        (IcebergUpdateTablePreEvent) transformedEvent.get();
    LoadTableResponse loadTableResponse;
    try {
      loadTableResponse =
          icebergTableOperationDispatcher.updateTable(
              context, tableIdentifier, transformedUpdateEvent.updateTableRequest());
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergUpdateTableFailureEvent(
              context, gravitinoNameIdentifier, transformedUpdateEvent.updateTableRequest(), e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergUpdateTableEvent(
            context,
            gravitinoNameIdentifier,
            transformedUpdateEvent.updateTableRequest(),
            loadTableResponse));
    return loadTableResponse;
  }

  @Override
  public void dropTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier, boolean purgeRequested) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.catalogName(), tableIdentifier);
    eventBus.dispatchEvent(
        new IcebergDropTablePreEvent(context, gravitinoNameIdentifier, purgeRequested));
    try {
      icebergTableOperationDispatcher.dropTable(context, tableIdentifier, purgeRequested);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergDropTableFailureEvent(context, gravitinoNameIdentifier, purgeRequested, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergDropTableEvent(context, gravitinoNameIdentifier, purgeRequested));
  }

  @Override
  public LoadTableResponse loadTable(
      IcebergRequestContext context, TableIdentifier tableIdentifier) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.catalogName(), tableIdentifier);
    eventBus.dispatchEvent(new IcebergLoadTablePreEvent(context, gravitinoNameIdentifier));
    LoadTableResponse loadTableResponse;
    try {
      loadTableResponse = icebergTableOperationDispatcher.loadTable(context, tableIdentifier);
    } catch (Exception e) {
      eventBus.dispatchEvent(new IcebergLoadTableFailureEvent(context, gravitinoNameIdentifier, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergLoadTableEvent(context, gravitinoNameIdentifier, loadTableResponse));
    return loadTableResponse;
  }

  @Override
  public ListTablesResponse listTable(IcebergRequestContext context, Namespace namespace) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(metalakeName, context.catalogName(), namespace);
    eventBus.dispatchEvent(new IcebergListTablePreEvent(context, gravitinoNameIdentifier));
    ListTablesResponse listTablesResponse;
    try {
      listTablesResponse = icebergTableOperationDispatcher.listTable(context, namespace);
    } catch (Exception e) {
      eventBus.dispatchEvent(new IcebergListTableFailureEvent(context, gravitinoNameIdentifier, e));
      throw e;
    }
    eventBus.dispatchEvent(new IcebergListTableEvent(context, gravitinoNameIdentifier));
    return listTablesResponse;
  }

  @Override
  public boolean tableExists(IcebergRequestContext context, TableIdentifier tableIdentifier) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.catalogName(), tableIdentifier);
    eventBus.dispatchEvent(new IcebergTableExistsPreEvent(context, gravitinoNameIdentifier));
    boolean isExists;
    try {
      isExists = icebergTableOperationDispatcher.tableExists(context, tableIdentifier);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergTableExistsFailureEvent(context, gravitinoNameIdentifier, e));
      throw e;
    }
    eventBus.dispatchEvent(new IcebergTableExistsEvent(context, gravitinoNameIdentifier, isExists));
    return isExists;
  }

  @Override
  public void renameTable(IcebergRequestContext context, RenameTableRequest renameTableRequest) {
    TableIdentifier sourceTable = renameTableRequest.source();
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.catalogName(), sourceTable);
    eventBus.dispatchEvent(
        new IcebergRenameTablePreEvent(context, gravitinoNameIdentifier, renameTableRequest));
    try {
      icebergTableOperationDispatcher.renameTable(context, renameTableRequest);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergRenameTableFailureEvent(
              context, gravitinoNameIdentifier, renameTableRequest, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergRenameTableEvent(context, gravitinoNameIdentifier, renameTableRequest));
  }
}
