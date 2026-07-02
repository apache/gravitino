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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.hive2.Hive2Namespace;
import org.lance.namespace.model.AlterTableDropColumnsRequest;
import org.lance.namespace.model.AlterTableDropColumnsResponse;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeclareTableResponse;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DeregisterTableResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.DropTableResponse;
import org.lance.namespace.model.TableExistsRequest;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link HiveLanceTableOperations}, which adapts Gravitino's table-operation
 * interface onto a mocked {@link Hive2Namespace}.
 */
class TestHiveLanceTableOperations {

  private static final String DELIMITER = "$";

  private Hive2Namespace delegate;
  private HiveLanceTableOperations ops;

  @BeforeEach
  void setUp() {
    delegate = mock(Hive2Namespace.class);
    ops = new HiveLanceTableOperations(delegate);
  }

  @Test
  void testDescribeTableTranslatesRequestAndReturnsResponse() {
    DescribeTableResponse expected = new DescribeTableResponse();
    when(delegate.describeTable(any(DescribeTableRequest.class))).thenReturn(expected);

    DescribeTableResponse actual =
        ops.describeTable("db$t", DELIMITER, Optional.of(3L), true, false);

    assertSame(expected, actual);
    ArgumentCaptor<DescribeTableRequest> captor =
        ArgumentCaptor.forClass(DescribeTableRequest.class);
    verify(delegate).describeTable(captor.capture());
    DescribeTableRequest sent = captor.getValue();
    assertEquals(List.of("db", "t"), sent.getId());
    assertEquals(3L, sent.getVersion());
    assertEquals(Boolean.TRUE, sent.getCheckDeclared());
    assertEquals(Boolean.FALSE, sent.getLoadDetailedMetadata());
  }

  @Test
  void testDeclareTableTranslatesRequestAndReturnsResponse() {
    DeclareTableResponse expected = new DeclareTableResponse();
    when(delegate.declareTable(any(DeclareTableRequest.class))).thenReturn(expected);

    DeclareTableResponse actual =
        ops.declareTable("db$t", DELIMITER, "s3://bucket/db/t", ImmutableMap.of("k", "v"));

    assertSame(expected, actual);
    ArgumentCaptor<DeclareTableRequest> captor = ArgumentCaptor.forClass(DeclareTableRequest.class);
    verify(delegate).declareTable(captor.capture());
    DeclareTableRequest sent = captor.getValue();
    assertEquals(List.of("db", "t"), sent.getId());
    assertEquals("s3://bucket/db/t", sent.getLocation());
    assertEquals("v", sent.getProperties().get("k"));
  }

  @Test
  void testDropTableTranslatesRequestAndReturnsResponse() {
    DropTableResponse expected = new DropTableResponse();
    when(delegate.dropTable(any(DropTableRequest.class))).thenReturn(expected);

    DropTableResponse actual = ops.dropTable("db$t", DELIMITER);

    assertSame(expected, actual);
    ArgumentCaptor<DropTableRequest> captor = ArgumentCaptor.forClass(DropTableRequest.class);
    verify(delegate).dropTable(captor.capture());
    assertEquals(List.of("db", "t"), captor.getValue().getId());
  }

  @Test
  void testDeregisterTableTranslatesRequestAndReturnsResponse() {
    DeregisterTableResponse expected = new DeregisterTableResponse();
    when(delegate.deregisterTable(any(DeregisterTableRequest.class))).thenReturn(expected);

    DeregisterTableResponse actual = ops.deregisterTable("db$t", DELIMITER);

    assertSame(expected, actual);
    ArgumentCaptor<DeregisterTableRequest> captor =
        ArgumentCaptor.forClass(DeregisterTableRequest.class);
    verify(delegate).deregisterTable(captor.capture());
    assertEquals(List.of("db", "t"), captor.getValue().getId());
  }

  @Test
  void testTableExistsTrueWhenDelegateDoesNotThrow() {
    HiveLanceTableOperations operations = new HiveLanceTableOperations(delegate);
    assertTrue(operations.tableExists("db$t", DELIMITER));
    verify(delegate).tableExists(any(TableExistsRequest.class));
  }

  @Test
  void testTableExistsFalseWhenDelegateThrowsNotFound() {
    org.mockito.Mockito.doThrow(new TableNotFoundException("missing", null, "db.t"))
        .when(delegate)
        .tableExists(any(TableExistsRequest.class));
    assertFalse(ops.tableExists("db$t", DELIMITER));
  }

  @Test
  void testAlterTableDropColumnsDelegates() {
    AlterTableDropColumnsResponse expected = new AlterTableDropColumnsResponse();
    when(delegate.alterTableDropColumns(any(AlterTableDropColumnsRequest.class)))
        .thenReturn(expected);

    AlterTableDropColumnsRequest request = new AlterTableDropColumnsRequest();
    request.setColumns(List.of("c1"));
    Object actual = ops.alterTable("db$t", DELIMITER, request);

    assertSame(expected, actual);
    assertEquals(List.of("db", "t"), request.getId());
  }
}
