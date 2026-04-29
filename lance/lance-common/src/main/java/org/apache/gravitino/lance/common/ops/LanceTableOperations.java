/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.common.ops;

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.lance.namespace.model.CreateEmptyTableResponse;
import org.lance.namespace.model.CreateTableResponse;
import org.lance.namespace.model.DeclareTableResponse;
import org.lance.namespace.model.DeregisterTableResponse;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropTableResponse;
import org.lance.namespace.model.RegisterTableResponse;

public interface LanceTableOperations {

  /**
   * Describe the details of a table.
   *
   * @param tableId table ids are in the format of "{namespace}{delimiter}{table_name}"
   * @param delimiter the delimiter used in the namespace
   * @param version the version of the table to describe, if null, describe the latest version
   * @param credentialPrivilege the privilege level for vended credentials, null means no credential
   *     vending
   * @return the table description
   */
  DescribeTableResponse describeTable(
      String tableId,
      String delimiter,
      Optional<Long> version,
      @Nullable CredentialPrivilege credentialPrivilege);

  /**
   * Create a new table.
   *
   * @param tableId table ids are in the format of "{namespace}{delimiter}{table_name}"
   * @param mode it can be CREATE, OVERWRITE, or EXIST_OK
   * @param delimiter the delimiter used in the namespace
   * @param tableLocation the location where the table data will be stored
   * @param tableProperties the properties of the table
   * @param arrowStreamBody the arrow stream bytes containing the schema and data
   * @return the response of the create table operation
   */
  CreateTableResponse createTable(
      String tableId,
      String mode,
      String delimiter,
      String tableLocation,
      Map<String, String> tableProperties,
      byte[] arrowStreamBody);

  /**
   * Declare a table without touching storage. This is the preferred API for creating metadata-only
   * table entries, replacing the deprecated {@link #createEmptyTable} method.
   *
   * @param tableId table ids are in the format of "{namespace}{delimiter}{table_name}"
   * @param delimiter the delimiter used in the namespace
   * @param tableLocation the location where the table data will be stored
   * @param tableProperties the properties of the table
   * @return the response of the declare table operation
   */
  DeclareTableResponse declareTable(
      String tableId, String delimiter, String tableLocation, Map<String, String> tableProperties);

  /**
   * Create a new table without schema.
   *
   * @param tableId table ids are in the format of "{namespace}{delimiter}{table_name}"
   * @param delimiter the delimiter used in the namespace
   * @param tableLocation the location where the table data will be stored
   * @param tableProperties the properties of the table
   * @return the response of the create table operation
   * @deprecated Use {@link #declareTable} instead.
   */
  @Deprecated
  @SuppressWarnings("deprecation")
  CreateEmptyTableResponse createEmptyTable(
      String tableId, String delimiter, String tableLocation, Map<String, String> tableProperties);

  /**
   * Register an existing table.
   *
   * @param tableId table ids are in the format of "{namespace}{delimiter}{table_name}"
   * @param mode it can be REGISTER or OVERWRITE.
   * @param delimiter the delimiter used in the namespace
   * @param tableProperties the properties of the table, it should contain the table location
   * @return the response of the register table operation
   */
  RegisterTableResponse registerTable(
      String tableId, String mode, String delimiter, Map<String, String> tableProperties);

  /**
   * Deregister a table. It will not delete the underlying lance data.
   *
   * @param tableId table ids are in the format of "{namespace}{delimiter}{table_name}"
   * @param delimiter the delimiter used in the namespace
   * @return the response of the deregister table operation
   */
  DeregisterTableResponse deregisterTable(String tableId, String delimiter);

  /**
   * Check if a table exists.
   *
   * @param tableId table ids are in the format of "{namespace}{delimiter}{table_name}"
   * @param delimiter the delimiter used in the namespace
   * @return true if the table exists, false otherwise
   */
  boolean tableExists(String tableId, String delimiter);

  /**
   * Drop a table. It will delete the underlying lance data.
   *
   * @param tableId table ids are in the format of "{namespace}{delimiter}{table_name}"
   * @param delimiter the delimiter used in the namespace
   * @return the response of the drop table operation
   */
  DropTableResponse dropTable(String tableId, String delimiter);

  /**
   * Alter a table.
   *
   * @param tableId table ids are in the format of "{namespace}{delimiter}{table_name}"
   * @param delimiter the delimiter used in the namespace
   * @param request the request containing alter table details, it can be add/drop/alter columns
   * @return the response of the alter table operation.
   */
  Object alterTable(String tableId, String delimiter, Object request);
}
