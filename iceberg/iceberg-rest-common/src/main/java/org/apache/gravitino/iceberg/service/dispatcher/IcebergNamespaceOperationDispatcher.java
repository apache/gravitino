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

/**
 * The {@code IcebergNamespaceOperationDispatcher} interface defines the public API for managing
 * Iceberg namespaces.
 */
public interface IcebergNamespaceOperationDispatcher {

  /**
   * Creates a new Iceberg namespace.
   *
   * @param context Iceberg REST request context information.
   * @param createNamespaceRequest The request object containing the details for creating the
   *     namespace.
   * @return A {@link CreateNamespaceResponse} object containing the result of the operation.
   */
  CreateNamespaceResponse createNamespace(
      IcebergRequestContext context, CreateNamespaceRequest createNamespaceRequest);

  /**
   * Updates an Iceberg namespace.
   *
   * @param context Iceberg REST request context information.
   * @param namespace The Iceberg namespace.
   * @param updateNamespacePropertiesRequest The request object containing the details for updating
   *     the namespace.
   * @return A {@link UpdateNamespacePropertiesResponse} object containing the result of the
   *     operation.
   */
  UpdateNamespacePropertiesResponse updateNamespace(
      IcebergRequestContext context,
      Namespace namespace,
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest);

  /**
   * Drops an Iceberg namespace.
   *
   * @param context Iceberg REST request context information.
   * @param namespace The Iceberg namespace.
   */
  void dropNamespace(IcebergRequestContext context, Namespace namespace);

  /**
   * Loads an Iceberg namespace.
   *
   * @param context Iceberg REST request context information.
   * @param namespace The Iceberg namespace.
   * @return A {@link GetNamespaceResponse} object containing the result of the operation.
   */
  GetNamespaceResponse loadNamespace(IcebergRequestContext context, Namespace namespace);

  /**
   * Lists Iceberg namespaces.
   *
   * @param context Iceberg REST request context information.
   * @param parentNamespace The Iceberg namespace.
   * @return A {@link ListNamespacesResponse} object containing the list of namespaces.
   */
  ListNamespacesResponse listNamespaces(IcebergRequestContext context, Namespace parentNamespace);

  /**
   * Check whether an Iceberg namespace exists.
   *
   * @param context Iceberg REST request context information.
   * @param namespace The Iceberg namespace.
   * @return Whether namespace exists.
   */
  boolean namespaceExists(IcebergRequestContext context, Namespace namespace);

  /**
   * Register a Iceberg table.
   *
   * @param context Iceberg REST request context information.
   * @param namespace The Iceberg namespace.
   * @param registerTableRequest The request object containing the details for registering the
   *     table.
   * @return A {@link LoadTableResponse} object containing the result of the operation.
   */
  LoadTableResponse registerTable(
      IcebergRequestContext context,
      Namespace namespace,
      RegisterTableRequest registerTableRequest);
}
