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

package org.apache.gravitino.hive.client.hive3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.hive.converter.HiveTableConverter;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

/**
 * Unit tests for {@link HiveShimV3}, using a mocked {@link IMetaStoreClient} to verify NOT NULL /
 * DEFAULT constraint construction and the {@code alterTable} drop/recreate sequence, following the
 * same mocking style as {@code TestHiveShimAlterTable} for the base {@link
 * org.apache.gravitino.hive.client.HiveShim}.
 */
class TestHiveShimV3 {

  private static final String CATALOG = "hive";
  private static final String DB = "db";
  private static final String TABLE = "tbl";

  /**
   * A {@link HiveShimV3} that uses a mocked metastore client instead of connecting to a real Hive
   * Metastore. The mock is created inside {@link #createMetaStoreClient(Properties)} because that
   * method is invoked from the {@code HiveShim} superclass constructor, before any subclass field
   * is initialized.
   */
  private static class MockHiveShimV3 extends HiveShimV3 {
    MockHiveShimV3() {
      super(new Properties());
    }

    @Override
    public IMetaStoreClient createMetaStoreClient(Properties properties) {
      return mock(IMetaStoreClient.class);
    }

    IMetaStoreClient metaStoreClient() {
      return client;
    }
  }

  private HiveTable testTable(Column... columns) {
    Map<String, String> properties = new HashMap<>();
    properties.put(HiveConstants.LOCATION, "hdfs://ns/warehouse/db.db/tbl");
    return HiveTable.builder()
        .withName(TABLE)
        .withColumns(columns)
        .withProperties(properties)
        .withAuditInfo(
            AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
        .withCatalogName(CATALOG)
        .withDatabaseName(DB)
        .build();
  }

  @Test
  void testCreateTableBuildsNotNullAndDefaultConstraints() throws Exception {
    MockHiveShimV3 shim = new MockHiveShimV3();
    IMetaStoreClient client = shim.metaStoreClient();

    Column notNullColumn =
        Column.of("id", Types.IntegerType.get(), null, /* nullable */ false, false, null);
    Column defaultValueColumn =
        Column.of("name", Types.StringType.get(), null, true, false, Literals.stringLiteral("abc"));
    HiveTable table = testTable(notNullColumn, defaultValueColumn);

    shim.createTable(table);

    ArgumentCaptor<List<SQLNotNullConstraint>> notNullCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<SQLDefaultConstraint>> defaultCaptor = ArgumentCaptor.forClass(List.class);
    verify(client)
        .createTableWithConstraints(
            any(Table.class),
            eq(Collections.emptyList()),
            eq(Collections.emptyList()),
            eq(Collections.emptyList()),
            notNullCaptor.capture(),
            defaultCaptor.capture(),
            eq(Collections.emptyList()));

    List<SQLNotNullConstraint> notNulls = notNullCaptor.getValue();
    assertEquals(1, notNulls.size());
    SQLNotNullConstraint notNull = notNulls.get(0);
    assertEquals("id", notNull.getColumn_name());
    assertTrue(notNull.getNn_name().startsWith(TABLE + "_id_nn"));
    assertTrue(notNull.isEnable_cstr());
    assertFalse(notNull.isValidate_cstr());
    assertFalse(notNull.isRely_cstr());

    List<SQLDefaultConstraint> defaults = defaultCaptor.getValue();
    assertEquals(1, defaults.size());
    SQLDefaultConstraint defaultConstraint = defaults.get(0);
    assertEquals("name", defaultConstraint.getColumn_name());
    assertEquals("'abc'", defaultConstraint.getDefault_value());
    assertTrue(defaultConstraint.getDc_name().startsWith(TABLE + "_name_dv"));
    assertTrue(defaultConstraint.isEnable_cstr());
    assertFalse(defaultConstraint.isValidate_cstr());
    assertFalse(defaultConstraint.isRely_cstr());
  }

  @Test
  void testCreateTableWithoutConstraintsSkipsConstraintsCall() throws Exception {
    MockHiveShimV3 shim = new MockHiveShimV3();
    IMetaStoreClient client = shim.metaStoreClient();

    Column plainColumn = Column.of("id", Types.IntegerType.get(), null, true, false, null);
    shim.createTable(testTable(plainColumn));

    verify(client).createTable(any(Table.class));
    verify(client, never())
        .createTableWithConstraints(any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void testAlterTablePreservesExistingConstraintMetadata() throws Exception {
    MockHiveShimV3 shim = new MockHiveShimV3();
    IMetaStoreClient client = shim.metaStoreClient();

    SQLNotNullConstraint existingNotNull =
        new SQLNotNullConstraint(CATALOG, DB, TABLE, "id", "external_not_null", false, true, true);
    SQLDefaultConstraint existingDefault =
        new SQLDefaultConstraint(
            CATALOG, DB, TABLE, "name", "'abc'", "external_default", false, true, true);
    when(client.getNotNullConstraints(new NotNullConstraintsRequest(CATALOG, DB, TABLE)))
        .thenReturn(List.of(existingNotNull));
    when(client.getDefaultConstraints(new DefaultConstraintsRequest(CATALOG, DB, TABLE)))
        .thenReturn(List.of(existingDefault));

    Column notNullColumn =
        Column.of("id", Types.IntegerType.get(), "updated comment", false, false, null);
    Column defaultValueColumn =
        Column.of("name", Types.StringType.get(), null, true, false, Literals.stringLiteral("abc"));
    HiveTable alteredTable = testTable(notNullColumn, defaultValueColumn);

    shim.alterTable(CATALOG, DB, TABLE, alteredTable, false);

    ArgumentCaptor<List<SQLNotNullConstraint>> notNullCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<SQLDefaultConstraint>> defaultCaptor = ArgumentCaptor.forClass(List.class);
    InOrder calls = inOrder(client);
    calls.verify(client).dropConstraint(CATALOG, DB, TABLE, "external_not_null");
    calls.verify(client).dropConstraint(CATALOG, DB, TABLE, "external_default");
    calls.verify(client).alter_table(eq(CATALOG), eq(DB), eq(TABLE), any(Table.class));
    calls.verify(client).addNotNullConstraint(notNullCaptor.capture());
    calls.verify(client).addDefaultConstraint(defaultCaptor.capture());

    SQLNotNullConstraint recreatedNotNull = notNullCaptor.getValue().get(0);
    assertEquals("external_not_null", recreatedNotNull.getNn_name());
    assertFalse(recreatedNotNull.isEnable_cstr());
    assertTrue(recreatedNotNull.isValidate_cstr());
    assertTrue(recreatedNotNull.isRely_cstr());

    SQLDefaultConstraint recreatedDefault = defaultCaptor.getValue().get(0);
    assertEquals("external_default", recreatedDefault.getDc_name());
    assertFalse(recreatedDefault.isEnable_cstr());
    assertTrue(recreatedDefault.isValidate_cstr());
    assertTrue(recreatedDefault.isRely_cstr());
  }

  @Test
  void testAlterTableRestoresConstraintsWhenAlterFails() throws Exception {
    MockHiveShimV3 shim = new MockHiveShimV3();
    IMetaStoreClient client = shim.metaStoreClient();

    SQLNotNullConstraint existingNotNull =
        new SQLNotNullConstraint(CATALOG, DB, TABLE, "id", "tbl_id_nn_old", true, false, false);
    when(client.getNotNullConstraints(new NotNullConstraintsRequest(CATALOG, DB, TABLE)))
        .thenReturn(List.of(existingNotNull), List.of());
    when(client.getDefaultConstraints(new DefaultConstraintsRequest(CATALOG, DB, TABLE)))
        .thenReturn(List.of(), List.of());
    doThrow(new MetaException("boom"))
        .when(client)
        .alter_table(anyString(), anyString(), anyString(), any(Table.class));

    Column notNullColumn = Column.of("id", Types.IntegerType.get(), null, false, false, null);
    HiveTable alteredTable = testTable(notNullColumn);

    assertThrows(
        RuntimeException.class, () -> shim.alterTable(CATALOG, DB, TABLE, alteredTable, false));

    // The alter failed, so the previously dropped constraints must be put back.
    verify(client).addNotNullConstraint(List.of(existingNotNull));
  }

  @Test
  void testAlterTableFailurePropagatesWhenConstraintsCannotBeRestored() throws Exception {
    MockHiveShimV3 shim = new MockHiveShimV3();
    IMetaStoreClient client = shim.metaStoreClient();

    SQLNotNullConstraint existingNotNull =
        new SQLNotNullConstraint(CATALOG, DB, TABLE, "id", "tbl_id_nn_old", true, false, false);
    when(client.getNotNullConstraints(new NotNullConstraintsRequest(CATALOG, DB, TABLE)))
        .thenReturn(List.of(existingNotNull), List.of());
    when(client.getDefaultConstraints(new DefaultConstraintsRequest(CATALOG, DB, TABLE)))
        .thenReturn(List.of(), List.of());
    doThrow(new MetaException("boom"))
        .when(client)
        .alter_table(anyString(), anyString(), anyString(), any(Table.class));
    doThrow(new MetaException("restore also fails")).when(client).addNotNullConstraint(any());

    Column notNullColumn = Column.of("id", Types.IntegerType.get(), null, false, false, null);
    HiveTable alteredTable = testTable(notNullColumn);

    // The original alter failure must still be the one that is (eventually) surfaced, even though
    // restoring the dropped constraints also failed.
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class, () -> shim.alterTable(CATALOG, DB, TABLE, alteredTable, false));
    assertTrue(thrown.getMessage() == null || !thrown.getMessage().contains("restore"));
  }

  @Test
  void testAlterTableRestoresEarlierConstraintWhenLaterDropFails() throws Exception {
    MockHiveShimV3 shim = new MockHiveShimV3();
    IMetaStoreClient client = shim.metaStoreClient();

    SQLNotNullConstraint existingNotNull =
        new SQLNotNullConstraint(CATALOG, DB, TABLE, "id", "existing_not_null", true, false, false);
    SQLDefaultConstraint existingDefault =
        new SQLDefaultConstraint(
            CATALOG, DB, TABLE, "name", "'abc'", "existing_default", true, false, false);
    when(client.getNotNullConstraints(new NotNullConstraintsRequest(CATALOG, DB, TABLE)))
        .thenReturn(List.of(existingNotNull), List.of());
    when(client.getDefaultConstraints(new DefaultConstraintsRequest(CATALOG, DB, TABLE)))
        .thenReturn(List.of(existingDefault), List.of(existingDefault));
    doThrow(new MetaException("second drop failed"))
        .when(client)
        .dropConstraint(CATALOG, DB, TABLE, "existing_default");

    Column notNullColumn = Column.of("id", Types.IntegerType.get(), null, false, false, null);
    Column defaultValueColumn =
        Column.of("name", Types.StringType.get(), null, true, false, Literals.stringLiteral("abc"));

    assertThrows(
        RuntimeException.class,
        () ->
            shim.alterTable(
                CATALOG, DB, TABLE, testTable(notNullColumn, defaultValueColumn), false));

    verify(client).addNotNullConstraint(List.of(existingNotNull));
    verify(client, never()).addDefaultConstraint(any());
    verify(client, never()).alter_table(anyString(), anyString(), anyString(), any(Table.class));
  }

  @Test
  void testAlterTableRethrowsWithClearMessageWhenConstraintReAddFails() throws Exception {
    MockHiveShimV3 shim = new MockHiveShimV3();
    IMetaStoreClient client = shim.metaStoreClient();

    when(client.getNotNullConstraints(new NotNullConstraintsRequest(CATALOG, DB, TABLE)))
        .thenReturn(List.of());
    when(client.getDefaultConstraints(new DefaultConstraintsRequest(CATALOG, DB, TABLE)))
        .thenReturn(List.of());
    doThrow(new MetaException("add failed")).when(client).addNotNullConstraint(any());

    Column notNullColumn = Column.of("id", Types.IntegerType.get(), null, false, false, null);
    HiveTable alteredTable = testTable(notNullColumn);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class, () -> shim.alterTable(CATALOG, DB, TABLE, alteredTable, false));

    // alter_table itself succeeded (no stub failure), so the table was altered but constraints
    // could not be re-created; the surfaced message must make that unambiguous.
    verify(client).alter_table(eq(CATALOG), eq(DB), eq(TABLE), any(Table.class));
    assertTrue(thrown.getMessage().contains("dropped"));
    assertTrue(thrown.getMessage().contains("could not be re-created"));
  }

  @Test
  void testGetTableObjectsByNameDoesNotLoadConstraints() throws Exception {
    MockHiveShimV3 shim = new MockHiveShimV3();
    IMetaStoreClient client = shim.metaStoreClient();

    Column plainColumn = Column.of("id", Types.IntegerType.get(), null, true, false, null);
    Table hiveTable = HiveTableConverter.toHiveTable(testTable(plainColumn));
    hiveTable.setCatName(CATALOG);
    when(client.getTableObjectsByName(CATALOG, DB, List.of(TABLE))).thenReturn(List.of(hiveTable));

    shim.getTableObjectsByName(CATALOG, DB, List.of(TABLE));

    verify(client, never()).getNotNullConstraints(any());
    verify(client, never()).getDefaultConstraints(any());
  }
}
