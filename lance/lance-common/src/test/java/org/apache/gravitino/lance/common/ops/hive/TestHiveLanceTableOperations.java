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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.TableAlreadyExistsException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.CreateTableResponse;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.RegisterTableResponse;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;

/** Unit tests for {@link HiveLanceTableOperations} backed by a mocked client pool. */
class TestHiveLanceTableOperations {

  private static final String DELIMITER = "$";

  private HiveClientPool pool;
  private IMetaStoreClient client;
  private HiveLanceTableOperations ops;

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
    ops = new HiveLanceTableOperations(pool, "s3://bucket/wh");
  }

  private static Table lanceTable(String location) {
    Table table = new Table();
    table.setDbName("db");
    table.setTableName("t");
    table.setParameters(ImmutableMap.of("table_type", "lance"));
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(location);
    table.setSd(sd);
    return table;
  }

  @Test
  void testCreateTableRegistersExternalLanceTable() throws Exception {
    when(client.getTable("db", "t")).thenThrow(new NoSuchObjectException("no table"));

    CreateTableResponse response =
        ops.createTable(
            "db$t", "create", DELIMITER, "s3://bucket/wh/db/t", ImmutableMap.of("k", "v"), null);
    assertEquals("s3://bucket/wh/db/t", response.getLocation());
    assertEquals("lance", response.getProperties().get("table_type"));
    assertEquals("storage", response.getProperties().get("managed_by"));
    assertEquals("v", response.getProperties().get("k"));

    ArgumentCaptor<Table> captor = ArgumentCaptor.forClass(Table.class);
    verify(client).createTable(captor.capture());
    Table created = captor.getValue();
    assertEquals("EXTERNAL_TABLE", created.getTableType());
    assertEquals("s3://bucket/wh/db/t", created.getSd().getLocation());
    assertEquals("org.lance.mapred.LanceInputFormat", created.getSd().getInputFormat());
    assertEquals("org.lance.mapred.LanceOutputFormat", created.getSd().getOutputFormat());
    assertEquals(
        "org.lance.mapred.LanceSerDe", created.getSd().getSerdeInfo().getSerializationLib());
    assertTrue(created.getSd().getCols().isEmpty());
  }

  @Test
  void testCreateTableCreateModeThrowsWhenExists() throws Exception {
    when(client.getTable("db", "t")).thenReturn(lanceTable("s3://bucket/wh/db/t"));
    assertThrows(
        TableAlreadyExistsException.class,
        () -> ops.createTable("db$t", "create", DELIMITER, "s3://bucket/wh/db/t", null, null));
  }

  @Test
  void testDescribeTableReturnsLocationAndManagedVersioningFalse() throws Exception {
    when(client.getTable("db", "t")).thenReturn(lanceTable("s3://bucket/wh/db/t"));
    DescribeTableResponse response =
        ops.describeTable("db$t", DELIMITER, Optional.empty(), false, false);
    assertEquals("s3://bucket/wh/db/t", response.getLocation());
    assertFalse(response.getManagedVersioning());
  }

  @Test
  void testDescribeTableThrowsWhenLoadDetailedMetadata() {
    assertThrows(
        InvalidInputException.class,
        () -> ops.describeTable("db$t", DELIMITER, Optional.empty(), false, true));
  }

  @Test
  void testDescribeTableThrowsWhenAbsent() throws Exception {
    when(client.getTable("db", "t")).thenThrow(new NoSuchObjectException("no table"));
    assertThrows(
        TableNotFoundException.class,
        () -> ops.describeTable("db$t", DELIMITER, Optional.empty(), false, false));
  }

  @Test
  void testRegisterTableUsesLocationFromProperties() throws Exception {
    when(client.getTable("db", "t")).thenThrow(new NoSuchObjectException("no table"));
    RegisterTableResponse response =
        ops.registerTable(
            "db$t", "create", DELIMITER, ImmutableMap.of("location", "s3://bucket/data/t"));
    assertEquals("s3://bucket/data/t", response.getLocation());
    verify(client).createTable(any(Table.class));
  }

  @Test
  void testRegisterTableThrowsWhenLocationMissing() {
    assertThrows(
        InvalidInputException.class,
        () -> ops.registerTable("db$t", "create", DELIMITER, ImmutableMap.of()));
  }

  @Test
  void testDropTableDeletesData() throws Exception {
    when(client.getTable("db", "t")).thenReturn(lanceTable("s3://bucket/wh/db/t"));
    ops.dropTable("db$t", DELIMITER);
    verify(client).dropTable("db", "t", true, true);
  }

  @Test
  void testDeregisterTableKeepsData() throws Exception {
    when(client.getTable("db", "t")).thenReturn(lanceTable("s3://bucket/wh/db/t"));
    ops.deregisterTable("db$t", DELIMITER);
    verify(client).dropTable("db", "t", false, true);
  }

  @Test
  void testTableExistsTrueForLanceTable() throws Exception {
    when(client.getTable("db", "t")).thenReturn(lanceTable("s3://bucket/wh/db/t"));
    assertTrue(ops.tableExists("db$t", DELIMITER));
  }

  @Test
  void testTableExistsFalseWhenAbsent() throws Exception {
    when(client.getTable("db", "t")).thenThrow(new NoSuchObjectException("no table"));
    assertFalse(ops.tableExists("db$t", DELIMITER));
  }

  @Test
  void testAlterTableThrowsInvalidInput() {
    assertThrows(
        InvalidInputException.class, () -> ops.alterTable("db$t", DELIMITER, new Object()));
  }

  @Test
  void testWrongArityThrows() {
    assertThrows(
        InvalidInputException.class,
        () -> ops.describeTable("db", DELIMITER, Optional.empty(), false, false));
    assertThrows(InvalidInputException.class, () -> ops.tableExists("db$t$x", DELIMITER));
  }

  @Test
  void testDeclareTableDerivesLocationFromDatabaseWhenAbsent() throws Exception {
    when(client.getTable("db", "t")).thenThrow(new NoSuchObjectException("no table"));
    org.apache.hadoop.hive.metastore.api.Database database =
        new org.apache.hadoop.hive.metastore.api.Database();
    database.setName("db");
    database.setLocationUri("s3://bucket/wh/db");
    when(client.getDatabase("db")).thenReturn(database);

    assertEquals(
        "s3://bucket/wh/db/t",
        ops.declareTable("db$t", DELIMITER, null, ImmutableMap.of()).getLocation());
    verify(client).createTable(any(Table.class));
  }

  @Test
  void testCreateTableOverwriteDropsExisting() throws Exception {
    when(client.getTable("db", "t")).thenReturn(lanceTable("s3://bucket/wh/db/t"));
    ops.createTable("db$t", "overwrite", DELIMITER, "s3://bucket/wh/db/t", null, null);
    verify(client).dropTable(eq("db"), eq("t"), eq(false), eq(true));
    verify(client).createTable(any(Table.class));
  }
}
