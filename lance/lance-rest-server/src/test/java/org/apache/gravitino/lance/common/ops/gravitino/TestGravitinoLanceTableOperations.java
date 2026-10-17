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

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_TABLE_VERSION;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.lance.common.ops.gravitino.GravitinoLanceTableAlterHandler.AlterColumnsGravitinoLance;
import org.apache.gravitino.lance.common.ops.gravitino.GravitinoLanceTableAlterHandler.DropColumns;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.TableAlreadyExistsException;
import org.lance.namespace.model.AlterColumnsEntry;
import org.lance.namespace.model.AlterTableAlterColumnsRequest;
import org.lance.namespace.model.AlterTableAlterColumnsResponse;
import org.lance.namespace.model.AlterTableDropColumnsRequest;
import org.lance.namespace.model.AlterTableDropColumnsResponse;
import org.lance.namespace.model.RenameTableRequest;
import org.mockito.Mockito;

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
  void testRenameTableMapsSameSchemaRequest() {
    TableCatalog tableCatalog = tableCatalogWithTable("lance");
    GravitinoLanceTableOperations ops = operations(tableCatalog);
    RenameTableRequest request = new RenameTableRequest();
    request.setNewTableName("renamed_table");

    Assertions.assertNotNull(ops.renameTable("catalog.schema.table", ".", request));

    Mockito.verify(tableCatalog)
        .alterTable(
            Mockito.eq(NameIdentifier.of("schema", "table")),
            Mockito.eq(TableChange.rename("renamed_table")));
    Mockito.verify(tableCatalog, Mockito.never()).loadTable(Mockito.any(NameIdentifier.class));
  }

  @Test
  void testRenameTableMapsNewNamespaceRequest() {
    TableCatalog tableCatalog = tableCatalogWithTable("lance");
    GravitinoLanceTableOperations ops = operations(tableCatalog);
    RenameTableRequest request = new RenameTableRequest();
    request.setNewTableName("renamed_table");
    request.setNewNamespaceId(List.of("catalog", "target_schema"));

    Assertions.assertNotNull(ops.renameTable("catalog.schema.table", ".", request));

    Mockito.verify(tableCatalog)
        .alterTable(
            Mockito.eq(NameIdentifier.of("schema", "table")),
            Mockito.eq(TableChange.rename("renamed_table", "target_schema")));
  }

  @Test
  void testRenameTableMapsEntityConflictToLanceAlreadyExists() {
    TableCatalog tableCatalog = tableCatalogWithTable("lance");
    Mockito.doThrow(
            new IllegalArgumentException(
                "Table already exists", new EntityAlreadyExistsException("exists")))
        .when(tableCatalog)
        .alterTable(
            Mockito.eq(NameIdentifier.of("schema", "table")),
            Mockito.eq(TableChange.rename("existing_table")));
    GravitinoLanceTableOperations ops = operations(tableCatalog);
    RenameTableRequest request = new RenameTableRequest();
    request.setNewTableName("existing_table");

    Assertions.assertThrows(
        TableAlreadyExistsException.class,
        () -> ops.renameTable("catalog.schema.table", ".", request));
  }

  @Test
  void testRenameTableRejectsCrossCatalogTargetBeforeMutation() {
    TableCatalog tableCatalog = tableCatalogWithTable("lance");
    GravitinoLanceTableOperations ops = operations(tableCatalog);
    RenameTableRequest request = new RenameTableRequest();
    request.setNewTableName("renamed_table");
    request.setNewNamespaceId(List.of("another_catalog", "target_schema"));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ops.renameTable("catalog.schema.table", ".", request));

    Mockito.verify(tableCatalog, Mockito.never())
        .alterTable(Mockito.any(NameIdentifier.class), Mockito.<TableChange>any());
  }

  @Test
  void testRenameTableRejectsNonLanceTableBeforeMutation() {
    TableCatalog tableCatalog = tableCatalogWithTable("delta");
    Mockito.doThrow(new UnsupportedOperationException("Delta does not support ALTER TABLE"))
        .when(tableCatalog)
        .alterTable(
            Mockito.eq(NameIdentifier.of("schema", "table")),
            Mockito.eq(TableChange.rename("renamed_table")));
    GravitinoLanceTableOperations ops = operations(tableCatalog);
    RenameTableRequest request = new RenameTableRequest();
    request.setNewTableName("renamed_table");

    Assertions.assertThrows(
        InvalidInputException.class, () -> ops.renameTable("catalog.schema.table", ".", request));

    Mockito.verify(tableCatalog, Mockito.never()).loadTable(Mockito.any(NameIdentifier.class));
  }

  @Test
  void testRenameTableRejectsMalformedTargetNamespaceBeforeMutation() {
    TableCatalog tableCatalog = tableCatalogWithTable("lance");
    GravitinoLanceTableOperations ops = operations(tableCatalog);
    RenameTableRequest request = new RenameTableRequest();
    request.setNewTableName("renamed_table");
    request.setNewNamespaceId(List.of("catalog"));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ops.renameTable("catalog.schema.table", ".", request));

    Mockito.verify(tableCatalog, Mockito.never())
        .alterTable(Mockito.any(NameIdentifier.class), Mockito.<TableChange>any());
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
    Mockito.when(wrapper.schemaExists(Mockito.eq(catalog), Mockito.anyString())).thenReturn(true);
    return new GravitinoLanceTableOperations(wrapper);
  }
}
