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

package org.apache.gravitino.lance.common.ops.gravitino;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_CREATION_MODE;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_TABLE_VERSION;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.lance.common.ops.gravitino.GravitinoLanceTableAlterHandler.AlterColumnsGravitinoLance;
import org.apache.gravitino.lance.common.ops.gravitino.GravitinoLanceTableAlterHandler.DropColumns;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.model.AlterColumnsEntry;
import org.lance.namespace.model.AlterTableAlterColumnsRequest;
import org.lance.namespace.model.AlterTableAlterColumnsResponse;
import org.lance.namespace.model.AlterTableDropColumnsRequest;
import org.lance.namespace.model.AlterTableDropColumnsResponse;
import org.lance.namespace.model.CreateTableResponse;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
class TestGravitinoLanceTableOperations {

  @Test
  void testDropColumnsHandlerBuildsChangesAndSetsVersion() {
    AlterTableDropColumnsRequest request = new AlterTableDropColumnsRequest();
    request.setColumns(List.of("col1", "col2"));

    DropColumns handler = new DropColumns();

    TableChange[] changes = handler.buildGravitinoTableChange(request);
    Assertions.assertEquals(2, changes.length);

    Table table = Mockito.mock(Table.class);
    Mockito.when(table.properties()).thenReturn(Map.of(LANCE_TABLE_VERSION, "7"));

    AlterTableDropColumnsResponse response = handler.handle(table, request);
    Assertions.assertEquals(7L, response.getVersion());
  }

  @Test
  void testAlterColumnsHandlerBuildsChangesAndSetsVersion() {
    AlterTableAlterColumnsRequest request = new AlterTableAlterColumnsRequest();
    AlterColumnsEntry alteration = new AlterColumnsEntry();
    alteration.setPath("c1");
    alteration.setRename("c1_new");
    request.setAlterations(List.of(alteration));

    AlterColumnsGravitinoLance handler = new AlterColumnsGravitinoLance();

    TableChange[] changes = handler.buildGravitinoTableChange(request);
    Assertions.assertEquals(1, changes.length);

    Table table = Mockito.mock(Table.class);
    Mockito.when(table.properties()).thenReturn(Map.of(LANCE_TABLE_VERSION, "3"));

    AlterTableAlterColumnsResponse response = handler.handle(table, request);
    Assertions.assertEquals(3L, response.getVersion());
  }

  @Test
  void testAlterColumnsHandlerRejectsUnsupportedFields() {
    AlterTableAlterColumnsRequest request = new AlterTableAlterColumnsRequest();
    AlterColumnsEntry alteration = new AlterColumnsEntry();
    alteration.setPath("c1");
    alteration.setRename("c1_new");
    alteration.setNullable(Boolean.TRUE);
    request.setAlterations(List.of(alteration));

    AlterColumnsGravitinoLance handler = new AlterColumnsGravitinoLance();

    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> handler.buildGravitinoTableChange(request));
    Assertions.assertEquals(
        "Only RENAME alteration is supported currently.", exception.getMessage());
  }

  @ParameterizedTest
  @CsvSource(
      value = {"NULL", "false", "true"},
      nullValues = "NULL")
  void testDeregisterLegacyRegisteredTable(String external) {
    Table table = Mockito.mock(Table.class);
    Map<String, String> properties =
        new HashMap<>(
            Map.of(
                Table.PROPERTY_TABLE_FORMAT,
                "lance",
                "lance.register",
                "true",
                "location",
                "/existing"));
    if (external != null) {
      properties.put(Table.PROPERTY_EXTERNAL, external);
    }
    Mockito.when(table.properties()).thenReturn(properties);
    TableCatalog tableCatalog = Mockito.mock(TableCatalog.class);
    Mockito.when(tableCatalog.loadTable(Mockito.any(NameIdentifier.class))).thenReturn(table);
    Mockito.when(tableCatalog.dropTable(Mockito.any(NameIdentifier.class))).thenReturn(true);

    Assertions.assertEquals(
        "/existing",
        operations(tableCatalog).deregisterTable("catalog.schema.table", ".").getLocation());

    Mockito.verify(tableCatalog).dropTable(NameIdentifier.of("schema", "table"));
    Mockito.verify(tableCatalog, Mockito.never()).purgeTable(Mockito.any());
  }

  @Test
  void testDeregisterTableRejectsManagedTable() {
    // Mock a managed table (no PROPERTY_EXTERNAL=true)
    Table managedTable = Mockito.mock(Table.class);
    Mockito.when(managedTable.properties())
        .thenReturn(new HashMap<>(Map.of(Table.PROPERTY_TABLE_FORMAT, "lance")));

    TableCatalog tableCatalog = Mockito.mock(TableCatalog.class);
    Mockito.when(tableCatalog.loadTable(Mockito.any(NameIdentifier.class)))
        .thenReturn(managedTable);

    Catalog catalog = Mockito.mock(Catalog.class);

    GravitinoLanceNamespaceWrapper wrapper = Mockito.mock(GravitinoLanceNamespaceWrapper.class);
    Mockito.when(wrapper.loadAndValidateLakehouseCatalog(Mockito.anyString())).thenReturn(catalog);
    // In auxiliary mode the catalog is accessed through the namespace wrapper's dispatcher rather
    // than catalog.asTableCatalog() directly, so stub the wrapper routing accordingly.
    Mockito.when(wrapper.asTableCatalog(catalog)).thenReturn(tableCatalog);

    GravitinoLanceTableOperations ops = new GravitinoLanceTableOperations(wrapper);

    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> ops.deregisterTable("catalog.schema.table", "."));
    Assertions.assertTrue(exception.getMessage().contains("only supports external tables"));

    // Verify dropTable was never called — the guard must reject before reaching the catalog layer.
    Mockito.verify(tableCatalog, Mockito.never()).dropTable(Mockito.any());
  }

  @Test
  void testDescribeTableRejectsNonLanceTable() {
    TableCatalog tableCatalog = tableCatalogWithTable("delta");
    GravitinoLanceTableOperations ops = operations(tableCatalog);

    InvalidInputException exception =
        Assertions.assertThrows(
            InvalidInputException.class,
            () ->
                ops.describeTable(
                    "catalog.schema.table", ".", java.util.Optional.empty(), false, false));

    Assertions.assertTrue(exception.getMessage().contains("not a Lance table"));
  }

  @Test
  void testTableExistsTreatsNonLanceTableAsAbsent() {
    TableCatalog tableCatalog = tableCatalogWithTable("delta");
    GravitinoLanceTableOperations ops = operations(tableCatalog);

    Assertions.assertFalse(ops.tableExists("catalog.schema.table", "."));
    Mockito.verify(tableCatalog, Mockito.never()).tableExists(Mockito.any());
  }

  @Test
  void testDropTableRejectsNonLanceTableBeforePurge() {
    TableCatalog tableCatalog = tableCatalogWithTable("delta");
    GravitinoLanceTableOperations ops = operations(tableCatalog);

    Assertions.assertThrows(
        InvalidInputException.class, () -> ops.dropTable("catalog.schema.table", "."));

    Mockito.verify(tableCatalog, Mockito.never()).purgeTable(Mockito.any());
  }

  @Test
  void testDeregisterTableRejectsNonLanceTableBeforeDrop() {
    TableCatalog tableCatalog = tableCatalogWithTable("delta");
    GravitinoLanceTableOperations ops = operations(tableCatalog);

    Assertions.assertThrows(
        InvalidInputException.class, () -> ops.deregisterTable("catalog.schema.table", "."));

    Mockito.verify(tableCatalog, Mockito.never()).dropTable(Mockito.any());
  }

  @Test
  void testAlterTableRejectsNonLanceTableBeforeAlter() {
    TableCatalog tableCatalog = tableCatalogWithTable("delta");
    GravitinoLanceTableOperations ops = operations(tableCatalog);
    AlterTableDropColumnsRequest request = new AlterTableDropColumnsRequest();
    request.setColumns(List.of("col1"));

    Assertions.assertThrows(
        InvalidInputException.class, () -> ops.alterTable("catalog.schema.table", ".", request));

    Mockito.verify(tableCatalog, Mockito.never()).alterTable(Mockito.any(), Mockito.any());
  }

  @Test
  void testExistOkReturnsExistingTableWithoutCreating() {
    // Creating a table makes the caller its owner, so an existing table must never reach
    // createTable.
    TableCatalog tableCatalog = tableCatalogWithTable("lance");
    GravitinoLanceTableOperations ops = operations(tableCatalog);

    CreateTableResponse response =
        ops.createTable("catalog.schema.table", "exist_ok", ".", null, Map.of(), null);

    Assertions.assertEquals("lance", response.getProperties().get(Table.PROPERTY_TABLE_FORMAT));
    Mockito.verify(tableCatalog).loadTable(NameIdentifier.of("schema", "table"));
    Mockito.verify(tableCatalog, Mockito.never())
        .createTable(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyMap());
  }

  @Test
  void testExistOkCreatesMissingTableInCreateMode() {
    TableCatalog tableCatalog = Mockito.mock(TableCatalog.class);
    Mockito.when(tableCatalog.loadTable(Mockito.any(NameIdentifier.class)))
        .thenThrow(new NoSuchTableException("Table does not exist"));
    Table created = lanceTable();
    Mockito.when(
            tableCatalog.createTable(
                Mockito.any(NameIdentifier.class),
                Mockito.any(Column[].class),
                Mockito.isNull(),
                Mockito.anyMap()))
        .thenReturn(created);

    operations(tableCatalog)
        .createTable("catalog.schema.table", "exist_ok", ".", null, Map.of(), null);

    ArgumentCaptor<Map<String, String>> properties = ArgumentCaptor.forClass(Map.class);
    Mockito.verify(tableCatalog)
        .createTable(
            Mockito.eq(NameIdentifier.of("schema", "table")),
            Mockito.any(Column[].class),
            Mockito.isNull(),
            properties.capture());
    Assertions.assertEquals("CREATE", properties.getValue().get(LANCE_CREATION_MODE));
  }

  @Test
  void testExistOkReturnsTableCreatedByConcurrentRequest() {
    TableCatalog tableCatalog = Mockito.mock(TableCatalog.class);
    Table concurrent = lanceTable();
    Mockito.when(tableCatalog.loadTable(Mockito.any(NameIdentifier.class)))
        .thenThrow(new NoSuchTableException("Table does not exist"))
        .thenReturn(concurrent);
    Mockito.when(
            tableCatalog.createTable(
                Mockito.any(NameIdentifier.class),
                Mockito.any(Column[].class),
                Mockito.isNull(),
                Mockito.anyMap()))
        .thenThrow(new TableAlreadyExistsException("Table already exists"));

    CreateTableResponse response =
        operations(tableCatalog)
            .createTable("catalog.schema.table", "exist_ok", ".", null, Map.of(), null);

    Assertions.assertEquals("lance", response.getProperties().get(Table.PROPERTY_TABLE_FORMAT));
    Mockito.verify(tableCatalog, Mockito.times(2)).loadTable(NameIdentifier.of("schema", "table"));
  }

  @Test
  void testExistOkReportsConflictWhenConcurrentTableIsGoneAgain() {
    TableCatalog tableCatalog = Mockito.mock(TableCatalog.class);
    Mockito.when(tableCatalog.loadTable(Mockito.any(NameIdentifier.class)))
        .thenThrow(new NoSuchTableException("Table does not exist"));
    TableAlreadyExistsException conflict = new TableAlreadyExistsException("Table already exists");
    Mockito.when(
            tableCatalog.createTable(
                Mockito.any(NameIdentifier.class),
                Mockito.any(Column[].class),
                Mockito.isNull(),
                Mockito.anyMap()))
        .thenThrow(conflict);

    TableAlreadyExistsException thrown =
        Assertions.assertThrows(
            TableAlreadyExistsException.class,
            () ->
                operations(tableCatalog)
                    .createTable("catalog.schema.table", "exist_ok", ".", null, Map.of(), null));
    Assertions.assertSame(conflict, thrown);
  }

  @Test
  void testExistOkRejectsExistingNonLanceTableWithoutCreating() {
    TableCatalog tableCatalog = tableCatalogWithTable("delta");

    InvalidInputException exception =
        Assertions.assertThrows(
            InvalidInputException.class,
            () ->
                operations(tableCatalog)
                    .createTable("catalog.schema.table", "exist_ok", ".", null, Map.of(), null));

    Assertions.assertTrue(exception.getMessage().contains("not a Lance table"));
    Mockito.verify(tableCatalog, Mockito.never())
        .createTable(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyMap());
  }

  private static Table lanceTable() {
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.properties())
        .thenReturn(Map.of(Table.PROPERTY_TABLE_FORMAT, "lance", Table.PROPERTY_EXTERNAL, "true"));
    return table;
  }

  private static TableCatalog tableCatalogWithTable(String format) {
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.properties())
        .thenReturn(Map.of(Table.PROPERTY_TABLE_FORMAT, format, Table.PROPERTY_EXTERNAL, "true"));
    TableCatalog tableCatalog = Mockito.mock(TableCatalog.class);
    Mockito.when(tableCatalog.loadTable(Mockito.any(NameIdentifier.class))).thenReturn(table);
    return tableCatalog;
  }

  private static GravitinoLanceTableOperations operations(TableCatalog tableCatalog) {
    Catalog catalog = Mockito.mock(Catalog.class);
    GravitinoLanceNamespaceWrapper wrapper = Mockito.mock(GravitinoLanceNamespaceWrapper.class);
    Mockito.when(wrapper.loadAndValidateLakehouseCatalog(Mockito.anyString())).thenReturn(catalog);
    Mockito.when(wrapper.asTableCatalog(catalog)).thenReturn(tableCatalog);
    return new GravitinoLanceTableOperations(wrapper);
  }
}
