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

import com.lancedb.lance.namespace.LanceNamespaceException;
import com.lancedb.lance.namespace.model.CreateNamespaceRequest;
import com.lancedb.lance.namespace.model.CreateNamespaceResponse;
import com.lancedb.lance.namespace.model.DescribeNamespaceResponse;
import com.lancedb.lance.namespace.model.DropNamespaceRequest;
import com.lancedb.lance.namespace.model.DropNamespaceResponse;
import com.lancedb.lance.namespace.model.ListNamespacesResponse;
import com.lancedb.lance.namespace.model.ListTablesResponse;
import java.util.Map;

public interface LanceNamespaceOperations {

  ListNamespacesResponse listNamespaces(
      String namespaceId, String delimiter, String pageToken, Integer limit);

  DescribeNamespaceResponse describeNamespace(String namespaceId, String delimiter);

  CreateNamespaceResponse createNamespace(
      String namespaceId,
      String delimiter,
      CreateNamespaceRequest.ModeEnum mode,
      Map<String, String> properties);

  DropNamespaceResponse dropNamespace(
      String namespaceId,
      String delimiter,
      DropNamespaceRequest.ModeEnum mode,
      DropNamespaceRequest.BehaviorEnum behavior);

  void namespaceExists(String namespaceId, String delimiter) throws LanceNamespaceException;

  ListTablesResponse listTables(
      String namespaceId, String delimiter, String pageToken, Integer limit);
}
