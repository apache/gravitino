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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

/**
 * The {@code IcebergTableOperationDispatcher} interface defines the public API for managing Iceberg
 * tables.
 */
public interface IcebergTableOperationDispatcher {

  /**
   * Creates a new Iceberg table.
   *
   * @param catalogName The catalog name when creating the table.
   * @param namespace The namespace within which the table should be created.
   * @param createTableRequest The request object containing the details for creating the table.
   * @return A {@link LoadTableResponse} object containing the result of the operation.
   */
  LoadTableResponse createTable(
      String catalogName, Namespace namespace, CreateTableRequest createTableRequest);

  /**
   * Updates an Iceberg table.
   *
   * @param catalogName The catalog name when updating the table.
   * @param tableIdentifier The Iceberg table identifier.
   * @param updateTableRequest The request object containing the details for updating the table.
   * @return A {@link LoadTableResponse} object containing the result of the operation.
   */
  LoadTableResponse updateTable(
      String catalogName, TableIdentifier tableIdentifier, UpdateTableRequest updateTableRequest);

  /**
   * Drops an Iceberg table.
   *
   * @param catalogName The catalog name when dropping the table.
   * @param tableIdentifier The Iceberg table identifier.
   * @param purgeRequested Whether to purge the table.
   */
  void dropTable(String catalogName, TableIdentifier tableIdentifier, boolean purgeRequested);

  /**
   * Loads an Iceberg table.
   *
   * @param catalogName The catalog name when dropping the table.
   * @param tableIdentifier The Iceberg table identifier.
   * @return A {@link LoadTableResponse} object containing the result of the operation.
   */
  LoadTableResponse loadTable(String catalogName, TableIdentifier tableIdentifier);

  /**
   * Lists Iceberg tables.
   *
   * @param catalogName The catalog name when dropping the table.
   * @param namespace The Iceberg namespace.
   * @return A {@link ListTablesResponse} object containing the list of table identifiers.
   */
  ListTablesResponse listTable(String catalogName, Namespace namespace);

  /**
   * Check whether an Iceberg table exists.
   *
   * @param catalogName The catalog name when dropping the table.
   * @param tableIdentifier The Iceberg table identifier.
   * @return Whether table exists.
   */
  boolean tableExists(String catalogName, TableIdentifier tableIdentifier);

  /**
   * Rename an Iceberg table.
   *
   * @param catalogName The catalog name when dropping the table.
   * @param renameTableRequest Rename table request information.
   */
  void renameTable(String catalogName, RenameTableRequest renameTableRequest);
}
