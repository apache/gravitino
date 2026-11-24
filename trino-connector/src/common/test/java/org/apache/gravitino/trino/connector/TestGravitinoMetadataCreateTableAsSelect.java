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
package org.apache.gravitino.trino.connector;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.TRANSACTION_ALREADY_ABORTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.IntegerType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.glue.GlueMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.hive.HiveMetadataAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

/** Tests which tables {@link GravitinoMetadata#beginCreateTable} drops when the statement fails. */
class TestGravitinoMetadataCreateTableAsSelect {

  private static final SchemaTableName NAME = new SchemaTableName("s", "t");
  private static final ConnectorTableMetadata TABLE =
      new ConnectorTableMetadata(NAME, List.of(new ColumnMetadata("a", IntegerType.INTEGER)));
  private static final Answer<?> INSERT_SUCCEEDS =
      invocation -> mock(ConnectorInsertTableHandle.class);
  private static final Answer<?> INSERT_FAILS =
      invocation -> {
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "insert failed");
      };

  @AfterEach
  void clearInterrupt() {
    // Keeps an interrupt that a failed assertion left set from reaching the next test.
    Thread.interrupted();
  }

  @Test
  void testFailedStatementDropsTableWhenCatalogAllowsIt() {
    CatalogConnectorMetadata catalog = mock(CatalogConnectorMetadata.class);
    GravitinoMetadata metadata = newMetadata(catalog, true, INSERT_SUCCEEDS);

    beginCreateTable(metadata);
    metadata.dropCreatedTables();
    verify(catalog).dropTable(NAME);
  }

  @Test
  void testFailedStatementKeepsTableWhenCatalogCannotDropItSafely() {
    CatalogConnectorMetadata catalog = mock(CatalogConnectorMetadata.class);
    GravitinoMetadata metadata = newMetadata(catalog, false, INSERT_SUCCEEDS);

    beginCreateTable(metadata);
    metadata.dropCreatedTables();
    verify(catalog, never()).dropTable(any());
  }

  @Test
  void testFailedBeginInsertDropsTableOnlyOnce() {
    CatalogConnectorMetadata catalog = mock(CatalogConnectorMetadata.class);
    GravitinoMetadata metadata = newMetadata(catalog, true, INSERT_FAILS);

    assertThrows(TrinoException.class, () -> beginCreateTable(metadata));
    metadata.dropCreatedTables();
    verify(catalog, times(1)).dropTable(NAME);
  }

  @Test
  void testFailedBeginInsertKeepsTableWhenCatalogCannotDropItSafely() {
    CatalogConnectorMetadata catalog = mock(CatalogConnectorMetadata.class);
    GravitinoMetadata metadata = newMetadata(catalog, false, INSERT_FAILS);

    assertThrows(TrinoException.class, () -> beginCreateTable(metadata));
    metadata.dropCreatedTables();
    verify(catalog).createTable(any(), anyBoolean());
    verify(catalog, never()).dropTable(any());
  }

  @Test
  void testTransactionEndingDuringBeginCreateTableDropsTable() {
    CatalogConnectorMetadata catalog = mock(CatalogConnectorMetadata.class);
    GravitinoMetadata metadata = newMetadataEndingDuringBeginInsert(catalog, true);

    TrinoException e = assertThrows(TrinoException.class, () -> beginCreateTable(metadata));
    assertEquals(TRANSACTION_ALREADY_ABORTED.toErrorCode(), e.getErrorCode());
    verify(catalog).dropTable(NAME);
  }

  @Test
  void testTransactionEndingDuringBeginCreateTableKeepsTableWhenCatalogCannotDropItSafely() {
    CatalogConnectorMetadata catalog = mock(CatalogConnectorMetadata.class);
    GravitinoMetadata metadata = newMetadataEndingDuringBeginInsert(catalog, false);

    TrinoException e = assertThrows(TrinoException.class, () -> beginCreateTable(metadata));
    assertEquals(TRANSACTION_ALREADY_ABORTED.toErrorCode(), e.getErrorCode());
    verify(catalog).createTable(any(), anyBoolean());
    verify(catalog, never()).dropTable(any());
  }

  @Test
  void testEndedTransactionCreatesNoTable() {
    CatalogConnectorMetadata catalog = mock(CatalogConnectorMetadata.class);
    GravitinoMetadata metadata = newMetadata(catalog, false, INSERT_SUCCEEDS);
    metadata.dropCreatedTables();

    TrinoException e = assertThrows(TrinoException.class, () -> beginCreateTable(metadata));
    assertEquals(TRANSACTION_ALREADY_ABORTED.toErrorCode(), e.getErrorCode());
    verify(catalog, never()).createTable(any(), anyBoolean());
  }

  @Test
  void testDropAfterTransactionEndedClearsAndRestoresInterrupt() {
    CatalogConnectorMetadata catalog = mock(CatalogConnectorMetadata.class);
    AtomicBoolean interruptedOnDrop = recordInterruptOnDrop(catalog);
    AtomicReference<GravitinoMetadata> metadata = new AtomicReference<>();
    metadata.set(
        newMetadata(
            catalog,
            true,
            invocation -> {
              metadata.get().dropCreatedTables();
              Thread.currentThread().interrupt();
              return mock(ConnectorInsertTableHandle.class);
            }));

    assertThrows(TrinoException.class, () -> beginCreateTable(metadata.get()));
    assertTrue(Thread.interrupted());
    assertFalse(interruptedOnDrop.get());
  }

  @Test
  void testDropAfterFailedBeginInsertClearsAndRestoresInterrupt() {
    CatalogConnectorMetadata catalog = mock(CatalogConnectorMetadata.class);
    AtomicBoolean interruptedOnDrop = recordInterruptOnDrop(catalog);
    GravitinoMetadata metadata =
        newMetadata(
            catalog,
            true,
            invocation -> {
              Thread.currentThread().interrupt();
              throw new TrinoException(GENERIC_INTERNAL_ERROR, "insert interrupted");
            });

    assertThrows(TrinoException.class, () -> beginCreateTable(metadata));
    assertTrue(Thread.interrupted());
    assertFalse(interruptedOnDrop.get());
  }

  @Test
  void testOnlyHiveExternalTablesCanBeDropped() {
    HiveMetadataAdapter hive =
        new HiveMetadataAdapter(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    assertFalse(hive.canDropFailedNewTable(TABLE));
    assertFalse(hive.canDropFailedNewTable(withProperty("location", "hdfs://nn/warehouse/t")));
    assertFalse(hive.canDropFailedNewTable(withProperty("table_type", "MANAGED_TABLE")));
    assertTrue(hive.canDropFailedNewTable(withProperty("table_type", "EXTERNAL_TABLE")));
    assertTrue(hive.canDropFailedNewTable(withProperty("table_type", "external_table")));

    GlueMetadataAdapter glue = new GlueMetadataAdapter(Collections.emptyList());
    assertTrue(glue.canDropFailedNewTable(TABLE));
  }

  private static void beginCreateTable(GravitinoMetadata metadata) {
    metadata.beginCreateTable(
        mock(ConnectorSession.class), TABLE, Optional.empty(), RetryMode.NO_RETRIES, false);
  }

  private static ConnectorTableMetadata withProperty(String key, Object value) {
    return new ConnectorTableMetadata(NAME, TABLE.getColumns(), ImmutableMap.of(key, value));
  }

  /** Simulates Trino rolling back on another thread by ending the transaction in beginInsert. */
  private static GravitinoMetadata newMetadataEndingDuringBeginInsert(
      CatalogConnectorMetadata catalog, boolean canDrop) {
    AtomicReference<GravitinoMetadata> metadata = new AtomicReference<>();
    metadata.set(
        newMetadata(
            catalog,
            canDrop,
            invocation -> {
              metadata.get().dropCreatedTables();
              return mock(ConnectorInsertTableHandle.class);
            }));
    return metadata.get();
  }

  /** Records whether the planning thread was interrupted when the table was dropped. */
  private static AtomicBoolean recordInterruptOnDrop(CatalogConnectorMetadata catalog) {
    AtomicBoolean interruptedOnDrop = new AtomicBoolean(true);
    doAnswer(
            invocation -> {
              interruptedOnDrop.set(Thread.currentThread().isInterrupted());
              return null;
            })
        .when(catalog)
        .dropTable(NAME);
    return interruptedOnDrop;
  }

  private static GravitinoMetadata newMetadata(
      CatalogConnectorMetadata catalog, boolean canDrop, Answer<?> beginInsert) {
    CatalogConnectorMetadataAdapter adapter = mock(CatalogConnectorMetadataAdapter.class);
    when(adapter.canDropFailedNewTable(TABLE)).thenReturn(canDrop);
    ConnectorMetadata internal = mock(ConnectorMetadata.class);
    ConnectorTableHandle handle = mock(ConnectorTableHandle.class);
    when(internal.getTableHandle(any(), any(), any(), any())).thenReturn(handle);
    when(internal.getColumnHandles(any(), any())).thenReturn(Map.of("a", mock(ColumnHandle.class)));
    when(internal.beginInsert(any(), any(), anyList(), any())).thenAnswer(beginInsert);
    return new GravitinoMetadata(catalog, adapter, internal) {};
  }
}
