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
package org.apache.gravitino.lance.common.ops.hive;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.lance.common.ops.LanceNamespaceOperations;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.hive2.Hive2Namespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropNamespaceResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;

/**
 * Adapts Gravitino's {@link LanceNamespaceOperations} interface onto the official {@link
 * Hive2Namespace} implementation. Each call parses the delimited identifier into the list form the
 * Lance Namespace model expects, delegates to {@link Hive2Namespace}, and returns its response
 * directly (the Gravitino interface and {@link Hive2Namespace} share the {@code
 * org.lance.namespace.model} response types).
 */
public class HiveLanceNamespaceOperations implements LanceNamespaceOperations {

  private final Hive2Namespace delegate;

  /**
   * Create namespace operations delegating to the given {@link Hive2Namespace}.
   *
   * @param delegate the underlying Lance Hive 2 namespace implementation
   */
  public HiveLanceNamespaceOperations(Hive2Namespace delegate) {
    this.delegate = delegate;
  }

  @Override
  public ListNamespacesResponse listNamespaces(
      String namespaceId, String delimiter, String pageToken, Integer limit) {
    ListNamespacesRequest request = new ListNamespacesRequest();
    request.setId(IdentifierUtil.parse(namespaceId, delimiter));
    request.setPageToken(pageToken);
    request.setLimit(limit);
    return delegate.listNamespaces(request);
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(String namespaceId, String delimiter) {
    DescribeNamespaceRequest request = new DescribeNamespaceRequest();
    request.setId(IdentifierUtil.parse(namespaceId, delimiter));
    return delegate.describeNamespace(request);
  }

  @Override
  public CreateNamespaceResponse createNamespace(
      String namespaceId, String delimiter, String mode, Map<String, String> properties) {
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.setId(IdentifierUtil.parse(namespaceId, delimiter));
    request.setMode(mode);
    request.setProperties(properties);
    return delegate.createNamespace(request);
  }

  @Override
  public DropNamespaceResponse dropNamespace(
      String namespaceId, String delimiter, String mode, String behavior) {
    DropNamespaceRequest request = new DropNamespaceRequest();
    request.setId(IdentifierUtil.parse(namespaceId, delimiter));
    request.setMode(mode);
    request.setBehavior(behavior);
    return delegate.dropNamespace(request);
  }

  @Override
  public void namespaceExists(String namespaceId, String delimiter) throws LanceNamespaceException {
    NamespaceExistsRequest request = new NamespaceExistsRequest();
    request.setId(IdentifierUtil.parse(namespaceId, delimiter));
    delegate.namespaceExists(request);
  }

  @Override
  public ListTablesResponse listTables(
      String namespaceId, String delimiter, String pageToken, Integer limit) {
    ListTablesRequest request = new ListTablesRequest();
    List<String> id = IdentifierUtil.parse(namespaceId, delimiter);
    request.setId(id);
    request.setPageToken(pageToken);
    request.setLimit(limit);
    return delegate.listTables(request);
  }
}
