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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.NamespaceAlreadyExistsException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesResponse;
import org.mockito.invocation.InvocationOnMock;

/** Unit tests for {@link HiveLanceNamespaceOperations} backed by a mocked client pool. */
class TestHiveLanceNamespaceOperations {

  private static final String DELIMITER = "$";

  private HiveClientPool pool;
  private IMetaStoreClient client;
  private HiveLanceNamespaceOperations ops;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws Exception {
    pool = mock(HiveClientPool.class);
    client = mock(IMetaStoreClient.class);
    when(pool.run(any()))
        .thenAnswer(
            (InvocationOnMock inv) -> {
              ClientPoolImpl.Action<Object, IMetaStoreClient, Exception> action =
                  inv.getArgument(0);
              return action.run(client);
            });
    ops = new HiveLanceNamespaceOperations(pool, "s3://bucket/wh");
  }

  @Test
  void testListNamespacesAtRootReturnsSortedDatabases() throws Exception {
    when(client.getAllDatabases()).thenReturn(Arrays.asList("zeta", "alpha"));
    ListNamespacesResponse response = ops.listNamespaces("", DELIMITER, null, null);
    assertTrue(response.getNamespaces().contains("alpha"));
    assertTrue(response.getNamespaces().contains("zeta"));
    assertEquals(2, response.getNamespaces().size());
  }

  @Test
  void testCreateNamespaceCreatesDatabase() throws Exception {
    when(client.getDatabase("db1")).thenThrow(new NoSuchObjectException("no db"));
    CreateNamespaceResponse response =
        ops.createNamespace("db1", DELIMITER, "create", ImmutableMap.of());
    assertEquals(ImmutableMap.of(), response.getProperties());
    verify(client).createDatabase(any(Database.class));
  }

  @Test
  void testCreateNamespaceCreateModeThrowsWhenExists() throws Exception {
    when(client.getDatabase("db1")).thenReturn(new Database());
    assertThrows(
        NamespaceAlreadyExistsException.class,
        () -> ops.createNamespace("db1", DELIMITER, "create", ImmutableMap.of()));
  }

  @Test
  void testDropNamespaceDropsEmptyDatabase() throws Exception {
    Database database = new Database();
    database.setName("db1");
    when(client.getDatabase("db1")).thenReturn(database);
    when(client.getAllTables("db1")).thenReturn(Collections.emptyList());

    ops.dropNamespace("db1", DELIMITER, "fail", "Restrict");
    verify(client).dropDatabase("db1", false, true, false);
  }

  @Test
  void testDropNamespaceCascadeThrows() {
    assertThrows(
        InvalidInputException.class, () -> ops.dropNamespace("db1", DELIMITER, "fail", "Cascade"));
  }

  @Test
  void testNamespaceExistsThrowsWhenAbsent() throws Exception {
    when(client.getDatabase("db1")).thenThrow(new NoSuchObjectException("no db"));
    assertThrows(NamespaceNotFoundException.class, () -> ops.namespaceExists("db1", DELIMITER));
  }

  @Test
  void testListTablesReturnsOnlyLanceTables() throws Exception {
    Database database = new Database();
    database.setName("db1");
    when(client.getDatabase("db1")).thenReturn(database);
    when(client.getAllTables("db1")).thenReturn(Arrays.asList("lance_t", "other_t"));

    Table lanceTable = new Table();
    lanceTable.setParameters(ImmutableMap.of("table_type", "lance"));
    Table otherTable = new Table();
    otherTable.setParameters(ImmutableMap.of("table_type", "iceberg"));
    when(client.getTable("db1", "lance_t")).thenReturn(lanceTable);
    when(client.getTable("db1", "other_t")).thenReturn(otherTable);

    ListTablesResponse response = ops.listTables("db1", DELIMITER, null, null);
    assertEquals(Collections.singleton("lance_t"), response.getTables());
  }

  @Test
  void testListTablesThrowsWhenDatabaseMissing() throws Exception {
    when(client.getDatabase("db1")).thenThrow(new NoSuchObjectException("no db"));
    assertThrows(
        NamespaceNotFoundException.class, () -> ops.listTables("db1", DELIMITER, null, null));
  }

  @Test
  void testListNamespacesRejectsTwoLevels() {
    assertThrows(
        InvalidInputException.class, () -> ops.listNamespaces("db$x", DELIMITER, null, null));
  }

  @Test
  void testDescribeNamespaceReturnsProperties() throws Exception {
    Database database = new Database();
    database.setName("db1");
    database.setDescription("desc");
    database.setLocationUri("s3://bucket/wh/db1");
    when(client.getDatabase("db1")).thenReturn(database);

    assertEquals(
        "desc",
        ops.describeNamespace("db1", DELIMITER).getProperties().get("database.description"));
  }
}
