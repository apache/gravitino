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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;

/**
 * {@code IcebergSchemaHookDispatcher} is a decorator for {@link
 * IcebergNamespaceOperationDispatcher} that not only delegates namespace operations to the
 * underlying dispatcher but also executes some hook operations before or after the underlying
 * operations.
 */
public class IcebergNamespaceHookDispatcher implements IcebergNamespaceOperationDispatcher {
  private final IcebergNamespaceOperationDispatcher dispatcher;
  private final String metalake;

  public IcebergNamespaceHookDispatcher(IcebergNamespaceOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
    this.metalake = IcebergRESTServerContext.getInstance().metalakeName();
  }

  @Override
  public CreateNamespaceResponse createNamespace(
      IcebergRequestContext context, CreateNamespaceRequest createRequest) {
    AuthorizationUtils.checkCurrentUser(metalake, context.userName());

    CreateNamespaceResponse response = dispatcher.createNamespace(context, createRequest);

    importSchema(context.catalogName(), createRequest.namespace());
    setSchemaOwner(context.catalogName(), createRequest.namespace(), context.userName());

    return response;
  }

  @Override
  public UpdateNamespacePropertiesResponse updateNamespace(
      IcebergRequestContext context,
      Namespace namespace,
      UpdateNamespacePropertiesRequest updateRequest) {
    return dispatcher.updateNamespace(context, namespace, updateRequest);
  }

  @Override
  public void dropNamespace(IcebergRequestContext context, Namespace namespace) {
    dispatcher.dropNamespace(context, namespace);
  }

  @Override
  public GetNamespaceResponse loadNamespace(IcebergRequestContext context, Namespace namespace) {
    return dispatcher.loadNamespace(context, namespace);
  }

  @Override
  public ListNamespacesResponse listNamespaces(
      IcebergRequestContext context, Namespace parentNamespace) {
    return dispatcher.listNamespaces(context, parentNamespace);
  }

  @Override
  public boolean namespaceExists(IcebergRequestContext context, Namespace namespace) {
    return dispatcher.namespaceExists(context, namespace);
  }

  @Override
  public LoadTableResponse registerTable(
      IcebergRequestContext context,
      Namespace namespace,
      RegisterTableRequest registerTableRequest) {
    return dispatcher.registerTable(context, namespace, registerTableRequest);
  }

  private void importSchema(String catalogName, Namespace namespace) {
    SchemaDispatcher schemaDispatcher = GravitinoEnv.getInstance().schemaDispatcher();
    if (schemaDispatcher != null) {
      schemaDispatcher.loadSchema(
          IcebergIdentifierUtils.toGravitinoSchemaIdentifier(metalake, catalogName, namespace));
    }
  }

  private void setSchemaOwner(String catalogName, Namespace namespace, String user) {
    NameIdentifier schemaIdentifier =
        IcebergIdentifierUtils.toGravitinoSchemaIdentifier(metalake, catalogName, namespace);
    // Set the creator as the owner of the namespace
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      ownerManager.setOwner(
          metalake,
          NameIdentifierUtil.toMetadataObject(schemaIdentifier, Entity.EntityType.SCHEMA),
          user,
          Owner.Type.USER);
    }
  }
}
