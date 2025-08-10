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
import org.apache.gravitino.listener.api.event.IcebergCreateViewEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateViewFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateViewPreEvent;
import org.apache.gravitino.listener.api.event.IcebergDropViewEvent;
import org.apache.gravitino.listener.api.event.IcebergDropViewFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergDropViewPreEvent;
import org.apache.gravitino.listener.api.event.IcebergListViewEvent;
import org.apache.gravitino.listener.api.event.IcebergListViewFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergListViewPreEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadViewEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadViewFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadViewPreEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameViewEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameViewFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameViewPreEvent;
import org.apache.gravitino.listener.api.event.IcebergReplaceViewEvent;
import org.apache.gravitino.listener.api.event.IcebergReplaceViewFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergReplaceViewPreEvent;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.listener.api.event.IcebergViewExistsEvent;
import org.apache.gravitino.listener.api.event.IcebergViewExistsFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergViewExistsPreEvent;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;

/**
 * {@code IcebergViewEventDispatcher} is a decorator for {@link IcebergViewOperationExecutor} that
 * not only delegates view operations to the underlying dispatcher but also dispatches corresponding
 * events to an {@link EventBus}.
 */
public class IcebergViewEventDispatcher implements IcebergViewOperationDispatcher {

  private IcebergViewOperationDispatcher icebergViewOperationDispatcher;
  private EventBus eventBus;
  private String metalakeName;

  public IcebergViewEventDispatcher(
      IcebergViewOperationDispatcher icebergViewOperationDispatcher,
      EventBus eventBus,
      String metalakeName) {
    this.icebergViewOperationDispatcher = icebergViewOperationDispatcher;
    this.eventBus = eventBus;
    this.metalakeName = metalakeName;
  }

  @Override
  public LoadViewResponse createView(
      IcebergRequestContext context, Namespace namespace, CreateViewRequest createViewRequest) {
    TableIdentifier viewIdentifier = TableIdentifier.of(namespace, createViewRequest.name());
    NameIdentifier nameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.catalogName(), viewIdentifier);
    Optional<BaseEvent> transformedEvent =
        eventBus.dispatchEvent(
            new IcebergCreateViewPreEvent(context, nameIdentifier, createViewRequest));
    IcebergCreateViewPreEvent transformedCreateEvent =
        (IcebergCreateViewPreEvent) transformedEvent.get();
    LoadViewResponse loadViewResponse;
    try {
      loadViewResponse =
          icebergViewOperationDispatcher.createView(
              context, namespace, transformedCreateEvent.createViewRequest());
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergCreateViewFailureEvent(
              context, nameIdentifier, transformedCreateEvent.createViewRequest(), e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergCreateViewEvent(
            context, nameIdentifier, transformedCreateEvent.createViewRequest(), loadViewResponse));
    return loadViewResponse;
  }

  @Override
  public LoadViewResponse replaceView(
      IcebergRequestContext context,
      TableIdentifier viewIdentifier,
      UpdateTableRequest replaceViewRequest) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.catalogName(), viewIdentifier);
    Optional<BaseEvent> transformedEvent =
        eventBus.dispatchEvent(
            new IcebergReplaceViewPreEvent(context, gravitinoNameIdentifier, replaceViewRequest));
    IcebergReplaceViewPreEvent transformedReplaceEvent =
        (IcebergReplaceViewPreEvent) transformedEvent.get();
    LoadViewResponse loadViewResponse;
    try {
      loadViewResponse =
          icebergViewOperationDispatcher.replaceView(
              context, viewIdentifier, transformedReplaceEvent.replaceViewRequest());
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergReplaceViewFailureEvent(
              context, gravitinoNameIdentifier, transformedReplaceEvent.replaceViewRequest(), e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergReplaceViewEvent(
            context,
            gravitinoNameIdentifier,
            transformedReplaceEvent.replaceViewRequest(),
            loadViewResponse));
    return loadViewResponse;
  }

  @Override
  public void dropView(IcebergRequestContext context, TableIdentifier viewIdentifier) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.catalogName(), viewIdentifier);
    eventBus.dispatchEvent(new IcebergDropViewPreEvent(context, gravitinoNameIdentifier));
    try {
      icebergViewOperationDispatcher.dropView(context, viewIdentifier);
    } catch (Exception e) {
      eventBus.dispatchEvent(new IcebergDropViewFailureEvent(context, gravitinoNameIdentifier, e));
      throw e;
    }
    eventBus.dispatchEvent(new IcebergDropViewEvent(context, gravitinoNameIdentifier));
  }

  @Override
  public LoadViewResponse loadView(IcebergRequestContext context, TableIdentifier viewIdentifier) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.catalogName(), viewIdentifier);
    eventBus.dispatchEvent(new IcebergLoadViewPreEvent(context, gravitinoNameIdentifier));
    LoadViewResponse loadViewResponse;
    try {
      loadViewResponse = icebergViewOperationDispatcher.loadView(context, viewIdentifier);
    } catch (Exception e) {
      eventBus.dispatchEvent(new IcebergLoadViewFailureEvent(context, gravitinoNameIdentifier, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergLoadViewEvent(context, gravitinoNameIdentifier, loadViewResponse));
    return loadViewResponse;
  }

  @Override
  public ListTablesResponse listView(IcebergRequestContext context, Namespace namespace) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(metalakeName, context.catalogName(), namespace);
    eventBus.dispatchEvent(new IcebergListViewPreEvent(context, gravitinoNameIdentifier));
    ListTablesResponse listViewsResponse;
    try {
      listViewsResponse = icebergViewOperationDispatcher.listView(context, namespace);
    } catch (Exception e) {
      eventBus.dispatchEvent(new IcebergListViewFailureEvent(context, gravitinoNameIdentifier, e));
      throw e;
    }
    eventBus.dispatchEvent(new IcebergListViewEvent(context, gravitinoNameIdentifier));
    return listViewsResponse;
  }

  @Override
  public boolean viewExists(IcebergRequestContext context, TableIdentifier viewIdentifier) {
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.catalogName(), viewIdentifier);
    eventBus.dispatchEvent(new IcebergViewExistsPreEvent(context, gravitinoNameIdentifier));
    boolean isExists;
    try {
      isExists = icebergViewOperationDispatcher.viewExists(context, viewIdentifier);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergViewExistsFailureEvent(context, gravitinoNameIdentifier, e));
      throw e;
    }
    eventBus.dispatchEvent(new IcebergViewExistsEvent(context, gravitinoNameIdentifier, isExists));
    return isExists;
  }

  @Override
  public void renameView(IcebergRequestContext context, RenameTableRequest renameViewRequest) {
    TableIdentifier sourceView = renameViewRequest.source();
    NameIdentifier gravitinoNameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.catalogName(), sourceView);
    eventBus.dispatchEvent(
        new IcebergRenameViewPreEvent(context, gravitinoNameIdentifier, renameViewRequest));
    try {
      icebergViewOperationDispatcher.renameView(context, renameViewRequest);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergRenameViewFailureEvent(
              context, gravitinoNameIdentifier, renameViewRequest, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergRenameViewEvent(context, gravitinoNameIdentifier, renameViewRequest));
  }
}
