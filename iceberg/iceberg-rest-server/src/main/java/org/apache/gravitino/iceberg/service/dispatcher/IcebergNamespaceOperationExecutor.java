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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergNamespaceOperationExecutor implements IcebergNamespaceOperationDispatcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergNamespaceOperationExecutor.class);

  private IcebergCatalogWrapperManager icebergCatalogWrapperManager;

  public IcebergNamespaceOperationExecutor(
      IcebergCatalogWrapperManager icebergCatalogWrapperManager) {
    this.icebergCatalogWrapperManager = icebergCatalogWrapperManager;
  }

  @Override
  public CreateNamespaceResponse createNamespace(
      IcebergRequestContext context, CreateNamespaceRequest createNamespaceRequest) {
    String authenticatedUser = context.userName();
    if (!AuthConstants.ANONYMOUS_USER.equals(authenticatedUser)) {
      String existingOwner = createNamespaceRequest.properties().get(IcebergConstants.OWNER);

      // Override the owner as the authenticated user if different from authenticated user
      if (!authenticatedUser.equals(existingOwner)) {
        Map<String, String> properties = new HashMap<>(createNamespaceRequest.properties());
        properties.put(IcebergConstants.OWNER, authenticatedUser);
        LOG.debug(
            "Overriding namespace owner from '{}' to authenticated user: '{}'",
            existingOwner,
            authenticatedUser);

        // CreateNamespaceRequest is immutable, so we need to rebuild it with modified properties
        createNamespaceRequest =
            CreateNamespaceRequest.builder()
                .withNamespace(createNamespaceRequest.namespace())
                .setProperties(properties)
                .build();
      }
    }

    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .createNamespace(createNamespaceRequest);
  }

  @Override
  public UpdateNamespacePropertiesResponse updateNamespace(
      IcebergRequestContext context,
      Namespace namespace,
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .updateNamespaceProperties(namespace, updateNamespacePropertiesRequest);
  }

  @Override
  public void dropNamespace(IcebergRequestContext context, Namespace namespace) {
    icebergCatalogWrapperManager.getCatalogWrapper(context.catalogName()).dropNamespace(namespace);
  }

  @Override
  public GetNamespaceResponse loadNamespace(IcebergRequestContext context, Namespace namespace) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .loadNamespace(namespace);
  }

  @Override
  public ListNamespacesResponse listNamespaces(
      IcebergRequestContext context, Namespace parentNamespace) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .listNamespace(parentNamespace);
  }

  @Override
  public boolean namespaceExists(IcebergRequestContext context, Namespace namespace) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .namespaceExists(namespace);
  }

  @Override
  public LoadTableResponse registerTable(
      IcebergRequestContext context,
      Namespace namespace,
      RegisterTableRequest registerTableRequest) {
    return icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .registerTable(namespace, registerTableRequest);
  }
}
