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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link HiveLanceNamespaceOperations}, which adapts Gravitino's namespace-operation
 * interface onto a mocked {@link Hive2Namespace}.
 */
class TestHiveLanceNamespaceOperations {

  private static final String DELIMITER = "$";

  private Hive2Namespace delegate;
  private HiveLanceNamespaceOperations ops;

  @BeforeEach
  void setUp() {
    delegate = mock(Hive2Namespace.class);
    ops = new HiveLanceNamespaceOperations(delegate);
  }

  @Test
  void testListNamespacesTranslatesRootIdToEmptyList() {
    ListNamespacesResponse expected = new ListNamespacesResponse();
    when(delegate.listNamespaces(any(ListNamespacesRequest.class))).thenReturn(expected);

    ListNamespacesResponse actual = ops.listNamespaces("", DELIMITER, "tok", 10);

    assertSame(expected, actual);
    ArgumentCaptor<ListNamespacesRequest> captor =
        ArgumentCaptor.forClass(ListNamespacesRequest.class);
    verify(delegate).listNamespaces(captor.capture());
    ListNamespacesRequest sent = captor.getValue();
    assertEquals(List.of(), sent.getId());
    assertEquals("tok", sent.getPageToken());
    assertEquals(10, sent.getLimit());
  }

  @Test
  void testCreateNamespaceTranslatesRequest() {
    CreateNamespaceResponse expected = new CreateNamespaceResponse();
    when(delegate.createNamespace(any(CreateNamespaceRequest.class))).thenReturn(expected);

    CreateNamespaceResponse actual =
        ops.createNamespace("db1", DELIMITER, "create", ImmutableMap.of("k", "v"));

    assertSame(expected, actual);
    ArgumentCaptor<CreateNamespaceRequest> captor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(delegate).createNamespace(captor.capture());
    CreateNamespaceRequest sent = captor.getValue();
    assertEquals(List.of("db1"), sent.getId());
    assertEquals("create", sent.getMode());
    assertEquals("v", sent.getProperties().get("k"));
  }

  @Test
  void testDescribeNamespaceTranslatesRequest() {
    DescribeNamespaceResponse expected = new DescribeNamespaceResponse();
    when(delegate.describeNamespace(any(DescribeNamespaceRequest.class))).thenReturn(expected);

    DescribeNamespaceResponse actual = ops.describeNamespace("db1", DELIMITER);

    assertSame(expected, actual);
    ArgumentCaptor<DescribeNamespaceRequest> captor =
        ArgumentCaptor.forClass(DescribeNamespaceRequest.class);
    verify(delegate).describeNamespace(captor.capture());
    assertEquals(List.of("db1"), captor.getValue().getId());
  }

  @Test
  void testDropNamespaceTranslatesModeAndBehavior() {
    DropNamespaceResponse expected = new DropNamespaceResponse();
    when(delegate.dropNamespace(any(DropNamespaceRequest.class))).thenReturn(expected);

    DropNamespaceResponse actual = ops.dropNamespace("db1", DELIMITER, "fail", "Restrict");

    assertSame(expected, actual);
    ArgumentCaptor<DropNamespaceRequest> captor =
        ArgumentCaptor.forClass(DropNamespaceRequest.class);
    verify(delegate).dropNamespace(captor.capture());
    DropNamespaceRequest sent = captor.getValue();
    assertEquals(List.of("db1"), sent.getId());
    assertEquals("fail", sent.getMode());
    assertEquals("Restrict", sent.getBehavior());
  }

  @Test
  void testNamespaceExistsDelegates() {
    ops.namespaceExists("db1", DELIMITER);
    ArgumentCaptor<NamespaceExistsRequest> captor =
        ArgumentCaptor.forClass(NamespaceExistsRequest.class);
    verify(delegate).namespaceExists(captor.capture());
    assertEquals(List.of("db1"), captor.getValue().getId());
  }

  @Test
  void testListTablesTranslatesRequest() {
    ListTablesResponse expected = new ListTablesResponse();
    when(delegate.listTables(any(ListTablesRequest.class))).thenReturn(expected);

    ListTablesResponse actual = ops.listTables("db1", DELIMITER, null, null);

    assertSame(expected, actual);
    ArgumentCaptor<ListTablesRequest> captor = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(delegate).listTables(captor.capture());
    assertEquals(List.of("db1"), captor.getValue().getId());
  }

  @Test
  void testMultiLevelIdSplits() {
    when(delegate.describeNamespace(any(DescribeNamespaceRequest.class)))
        .thenReturn(new DescribeNamespaceResponse());
    ops.describeNamespace("a$b", DELIMITER);
    ArgumentCaptor<DescribeNamespaceRequest> captor =
        ArgumentCaptor.forClass(DescribeNamespaceRequest.class);
    verify(delegate).describeNamespace(captor.capture());
    assertEquals(List.of("a", "b"), captor.getValue().getId());
  }
}
