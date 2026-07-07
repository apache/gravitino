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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;

/**
 * The {@code IcebergViewOperationDispatcher} interface defines the public API for managing Iceberg
 * views.
 */
public interface IcebergViewOperationDispatcher {

  /**
   * Creates a new Iceberg view.
   *
   * @param context Iceberg REST request context information.
   * @param namespace The namespace within which the view should be created.
   * @param createViewRequest The request object containing the details for creating the view.
   * @return A {@link LoadViewResponse} object containing the result of the operation.
   */
  LoadViewResponse createView(
      IcebergRequestContext context, Namespace namespace, CreateViewRequest createViewRequest);

  /**
   * Updates an Iceberg view.
   *
   * @param context Iceberg REST request context information.
   * @param viewIdentifier The Iceberg view identifier.
   * @param replaceViewRequest The request object containing the details for updating the view.
   * @return A {@link LoadViewResponse} object containing the result of the operation.
   */
  LoadViewResponse replaceView(
      IcebergRequestContext context,
      TableIdentifier viewIdentifier,
      UpdateTableRequest replaceViewRequest);

  /**
   * Drops an Iceberg view.
   *
   * @param context Iceberg REST request context information.
   * @param viewIdentifier The Iceberg view identifier.
   */
  void dropView(IcebergRequestContext context, TableIdentifier viewIdentifier);

  /**
   * Loads an Iceberg view.
   *
   * @param context Iceberg REST request context information.
   * @param viewIdentifier The Iceberg view identifier.
   * @return A {@link LoadViewResponse} object containing the result of the operation.
   */
  LoadViewResponse loadView(IcebergRequestContext context, TableIdentifier viewIdentifier);

  /**
   * Lists Iceberg views.
   *
   * @param context Iceberg REST request context information.
   * @param namespace The Iceberg namespace.
   * @return A {@link ListTablesResponse} object containing the list of view identifiers.
   */
  ListTablesResponse listView(IcebergRequestContext context, Namespace namespace);

  /**
   * Check whether an Iceberg view exists.
   *
   * @param context Iceberg REST request context information.
   * @param viewIdentifier The Iceberg view identifier.
   * @return Whether view exists.
   */
  boolean viewExists(IcebergRequestContext context, TableIdentifier viewIdentifier);

  /**
   * Rename an Iceberg view.
   *
   * @param context Iceberg REST request context information.
   * @param renameViewRequest Rename view request information.
   */
  void renameView(IcebergRequestContext context, RenameTableRequest renameViewRequest);
}
