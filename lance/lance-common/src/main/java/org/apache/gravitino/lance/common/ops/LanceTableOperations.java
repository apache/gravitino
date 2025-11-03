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

import com.lancedb.lance.namespace.model.CreateTableRequest;
import com.lancedb.lance.namespace.model.CreateTableResponse;
import com.lancedb.lance.namespace.model.DeregisterTableResponse;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.RegisterTableRequest;
import com.lancedb.lance.namespace.model.RegisterTableResponse;
import java.util.Map;

public interface LanceTableOperations {

  DescribeTableResponse describeTable(String tableId, String delimiter);

  CreateTableResponse createTable(
      String tableId,
      CreateTableRequest.ModeEnum mode,
      String delimiter,
      String tableLocation,
      Map<String, String> tableProperties,
      byte[] arrowStreamBody);

  RegisterTableResponse registerTable(
      String tableId,
      RegisterTableRequest.ModeEnum mode,
      String delimiter,
      Map<String, String> tableProperties);

  DeregisterTableResponse deregisterTable(String tableId, String delimiter);
}
