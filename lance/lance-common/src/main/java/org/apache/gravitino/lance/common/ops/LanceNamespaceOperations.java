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
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DropNamespaceResponse;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesResponse;

public interface LanceNamespaceOperations {

  ListNamespacesResponse listNamespaces(
      String namespaceId, String delimiter, String pageToken, Integer limit);

  DescribeNamespaceResponse describeNamespace(String namespaceId, String delimiter);

  CreateNamespaceResponse createNamespace(
      String namespaceId, String delimiter, String mode, Map<String, String> properties);

  DropNamespaceResponse dropNamespace(
      String namespaceId, String delimiter, String mode, String behavior);

  void namespaceExists(String namespaceId, String delimiter) throws LanceNamespaceException;

  ListTablesResponse listTables(
      String namespaceId, String delimiter, String pageToken, Integer limit);
}
