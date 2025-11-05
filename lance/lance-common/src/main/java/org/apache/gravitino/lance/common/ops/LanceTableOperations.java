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

import com.lancedb.lance.namespace.model.CreateEmptyTableResponse;
import com.lancedb.lance.namespace.model.CreateTableRequest;
import com.lancedb.lance.namespace.model.CreateTableResponse;
import com.lancedb.lance.namespace.model.DeregisterTableResponse;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.RegisterTableRequest;
import com.lancedb.lance.namespace.model.RegisterTableResponse;
import java.util.Map;
import java.util.Optional;

public interface LanceTableOperations {

  /**
   * Describe the details of a table.
   *
   * @param tableId table ids are in the format of "{namespace}{delimiter}{table_name}"
   * @param delimiter the delimiter used in the namespace
   * @param version the version of the table to describe, if null, describe the latest version
   * @return the table description
   */
  DescribeTableResponse describeTable(String tableId, String delimiter, Optional<Long> version);

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
      CreateTableRequest.ModeEnum mode,
      String delimiter,
      String tableLocation,
      Map<String, String> tableProperties,
      byte[] arrowStreamBody);

  /**
   * Create an new table without schema.
   *
   * @param tableId table ids are in the format of "{namespace}{delimiter}{table_name}"
   * @param delimiter the delimiter used in the namespace
   * @param tableLocation the location where the table data will be stored
   * @param tableProperties the properties of the table
   * @return the response of the create table operation
   */
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
      String tableId,
      RegisterTableRequest.ModeEnum mode,
      String delimiter,
      Map<String, String> tableProperties);

  /**
   * Deregister a table. It will not delete the underlying lance data.
   *
   * @param tableId table ids are in the format of "{namespace}{delimiter}{table_name}"
   * @param delimiter the delimiter used in the namespace
   * @return the response of the deregister table operation
   */
  DeregisterTableResponse deregisterTable(String tableId, String delimiter);
}
