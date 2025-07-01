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
import org.apache.gravitino.listener.api.event.IcebergCreateNamespaceEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateNamespaceFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateNamespacePreEvent;
import org.apache.gravitino.listener.api.event.IcebergDropNamespaceEvent;
import org.apache.gravitino.listener.api.event.IcebergDropNamespaceFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergDropNamespacePreEvent;
import org.apache.gravitino.listener.api.event.IcebergListNamespacesEvent;
import org.apache.gravitino.listener.api.event.IcebergListNamespacesFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergListNamespacesPreEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadNamespaceEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadNamespaceFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadNamespacePreEvent;
import org.apache.gravitino.listener.api.event.IcebergNamespaceExistsEvent;
import org.apache.gravitino.listener.api.event.IcebergNamespaceExistsFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergNamespaceExistsPreEvent;
import org.apache.gravitino.listener.api.event.IcebergRegisterTableEvent;
import org.apache.gravitino.listener.api.event.IcebergRegisterTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergRegisterTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.listener.api.event.IcebergUpdateNamespaceEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateNamespaceFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateNamespacePreEvent;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;

/**
 * {@code IcebergNamespaceEventDispatcher} is a decorator for {@link
 * IcebergNamespaceOperationExecutor} that not only delegates namespace operations to the underlying
 * dispatcher but also dispatches corresponding events to an {@link EventBus}.
 */
public class IcebergNamespaceEventDispatcher implements IcebergNamespaceOperationDispatcher {

  private IcebergNamespaceOperationDispatcher operationDispatcher;
  private EventBus eventBus;
  private String metalakeName;

  public IcebergNamespaceEventDispatcher(
      IcebergNamespaceOperationDispatcher operationDispatcher,
      EventBus eventBus,
      String metalakeName) {
    this.operationDispatcher = operationDispatcher;
    this.eventBus = eventBus;
    this.metalakeName = metalakeName;
  }

  @Override
  public CreateNamespaceResponse createNamespace(
      IcebergRequestContext context, CreateNamespaceRequest createRequest) {
    NameIdentifier nameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.catalogName(), createRequest.namespace());

    Optional<BaseEvent> transformedEvent =
        eventBus.dispatchEvent(
            new IcebergCreateNamespacePreEvent(context, nameIdentifier, createRequest));
    IcebergCreateNamespacePreEvent transformedCreateEvent =
        (IcebergCreateNamespacePreEvent) transformedEvent.get();

    CreateNamespaceResponse createResponse;
    try {
      createResponse =
          operationDispatcher.createNamespace(
              context, transformedCreateEvent.createNamespaceRequest());
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergCreateNamespaceFailureEvent(
              context, nameIdentifier, transformedCreateEvent.createNamespaceRequest(), e));
      throw e;
    }

    eventBus.dispatchEvent(
        new IcebergCreateNamespaceEvent(
            context,
            nameIdentifier,
            transformedCreateEvent.createNamespaceRequest(),
            createResponse));
    return createResponse;
  }

  @Override
  public UpdateNamespacePropertiesResponse updateNamespace(
      IcebergRequestContext context,
      Namespace namespace,
      UpdateNamespacePropertiesRequest updateRequest) {
    NameIdentifier nameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(metalakeName, context.catalogName(), namespace);

    Optional<BaseEvent> transformedEvent =
        eventBus.dispatchEvent(
            new IcebergUpdateNamespacePreEvent(context, nameIdentifier, updateRequest));
    IcebergUpdateNamespacePreEvent transformedUpdateEvent =
        (IcebergUpdateNamespacePreEvent) transformedEvent.get();

    UpdateNamespacePropertiesResponse updateResponse;
    try {
      updateResponse =
          operationDispatcher.updateNamespace(
              context, namespace, transformedUpdateEvent.updateNamespacePropertiesRequest());
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergUpdateNamespaceFailureEvent(
              context,
              nameIdentifier,
              transformedUpdateEvent.updateNamespacePropertiesRequest(),
              e));
      throw e;
    }

    eventBus.dispatchEvent(
        new IcebergUpdateNamespaceEvent(
            context,
            nameIdentifier,
            transformedUpdateEvent.updateNamespacePropertiesRequest(),
            updateResponse));
    return updateResponse;
  }

  @Override
  public void dropNamespace(IcebergRequestContext context, Namespace namespace) {
    NameIdentifier nameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(metalakeName, context.catalogName(), namespace);
    eventBus.dispatchEvent(new IcebergDropNamespacePreEvent(context, nameIdentifier));

    try {
      operationDispatcher.dropNamespace(context, namespace);
    } catch (Exception e) {
      eventBus.dispatchEvent(new IcebergDropNamespaceFailureEvent(context, nameIdentifier, e));
      throw e;
    }

    eventBus.dispatchEvent(new IcebergDropNamespaceEvent(context, nameIdentifier));
  }

  @Override
  public GetNamespaceResponse loadNamespace(IcebergRequestContext context, Namespace namespace) {
    NameIdentifier nameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(metalakeName, context.catalogName(), namespace);
    eventBus.dispatchEvent(new IcebergLoadNamespacePreEvent(context, nameIdentifier));

    GetNamespaceResponse getResponse;
    try {
      getResponse = operationDispatcher.loadNamespace(context, namespace);
    } catch (Exception e) {
      eventBus.dispatchEvent(new IcebergLoadNamespaceFailureEvent(context, nameIdentifier, e));
      throw e;
    }

    eventBus.dispatchEvent(new IcebergLoadNamespaceEvent(context, nameIdentifier, getResponse));
    return getResponse;
  }

  @Override
  public ListNamespacesResponse listNamespaces(
      IcebergRequestContext context, Namespace parentNamespace) {
    NameIdentifier nameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.catalogName(), parentNamespace);
    eventBus.dispatchEvent(new IcebergListNamespacesPreEvent(context, nameIdentifier));

    ListNamespacesResponse listResponse;
    try {
      listResponse = operationDispatcher.listNamespaces(context, parentNamespace);
    } catch (Exception e) {
      eventBus.dispatchEvent(new IcebergListNamespacesFailureEvent(context, nameIdentifier, e));
      throw e;
    }

    eventBus.dispatchEvent(new IcebergListNamespacesEvent(context, nameIdentifier));
    return listResponse;
  }

  @Override
  public boolean namespaceExists(IcebergRequestContext context, Namespace namespace) {
    NameIdentifier nameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(metalakeName, context.catalogName(), namespace);
    eventBus.dispatchEvent(new IcebergNamespaceExistsPreEvent(context, nameIdentifier));

    boolean isExists;
    try {
      isExists = operationDispatcher.namespaceExists(context, namespace);
    } catch (Exception e) {
      eventBus.dispatchEvent(new IcebergNamespaceExistsFailureEvent(context, nameIdentifier, e));
      throw e;
    }

    eventBus.dispatchEvent(new IcebergNamespaceExistsEvent(context, nameIdentifier, isExists));
    return isExists;
  }

  @Override
  public LoadTableResponse registerTable(
      IcebergRequestContext context,
      Namespace namespace,
      RegisterTableRequest registerTableRequest) {
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, registerTableRequest.name());
    NameIdentifier nameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(
            metalakeName, context.catalogName(), tableIdentifier);

    eventBus.dispatchEvent(
        new IcebergRegisterTablePreEvent(context, nameIdentifier, registerTableRequest));

    LoadTableResponse loadTableResponse;
    try {
      loadTableResponse =
          operationDispatcher.registerTable(context, namespace, registerTableRequest);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergRegisterTableFailureEvent(context, nameIdentifier, registerTableRequest, e));
      throw e;
    }

    eventBus.dispatchEvent(
        new IcebergRegisterTableEvent(
            context, nameIdentifier, registerTableRequest, loadTableResponse));
    return loadTableResponse;
  }
}
