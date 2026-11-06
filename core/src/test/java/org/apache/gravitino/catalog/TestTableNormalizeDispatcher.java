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
package org.apache.gravitino.catalog;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.TestColumn;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestTableNormalizeDispatcher extends TestOperationDispatcher {

  private static TableNormalizeDispatcher tableNormalizeDispatcher;
  private static SchemaNormalizeDispatcher schemaNormalizeDispatcher;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    TestTableOperationDispatcher.initialize();
    tableNormalizeDispatcher =
        new TableNormalizeDispatcher(
            TestTableOperationDispatcher.tableOperationDispatcher, catalogManager);
    schemaNormalizeDispatcher =
        new SchemaNormalizeDispatcher(
            TestTableOperationDispatcher.schemaOperationDispatcher, catalogManager);
  }

  @Test
  void testAlterChangesAreNormalizedOnlyOnceBeforeReachingCatalog() throws Exception {
    CatalogManager manager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    Capability capability = Mockito.mock(Capability.class);
    Mockito.when(capability.caseSensitiveOnName(Mockito.any()))
        .thenReturn(CapabilityResult.unsupported("case insensitive"));
    Mockito.when(capability.specificationOnName(Mockito.any(), Mockito.anyString()))
        .thenReturn(CapabilityResult.SUPPORTED);
    Mockito.when(capability.specificationOnExistingName(Mockito.any(), Mockito.anyString()))
        .thenCallRealMethod();
    Mockito.when(catalog.capability()).thenReturn(capability);
    CatalogTestUtils.mockDoWithCatalog(manager, catalog);
    CatalogManager.CatalogWrapper wrapper = Mockito.mock(CatalogManager.CatalogWrapper.class);
    Mockito.when(wrapper.capabilities()).thenReturn(capability);
    Mockito.when(wrapper.doWithCatalog(Mockito.any())).thenReturn(true);
    Mockito.doAnswer(
            invocation -> {
              ThrowableFunction<CatalogManager.CatalogWrapper, Object> operation =
                  invocation.getArgument(1);
              return operation.apply(wrapper);
            })
        .when(manager)
        .doWithCatalogWrapper(Mockito.any(), Mockito.any());
    TableCatalog tables = Mockito.mock(TableCatalog.class);
    IllegalArgumentException stop =
        new IllegalArgumentException("Stop after observing provider changes");
    Mockito.doAnswer(
            invocation -> {
              ThrowableFunction<TableCatalog, Object> operation = invocation.getArgument(0);
              return operation.apply(tables);
            })
        .when(wrapper)
        .doWithTableOps(Mockito.any());
    Mockito.when(tables.alterTable(Mockito.any(), Mockito.<TableChange[]>any())).thenThrow(stop);
    TableNormalizeDispatcher dispatcher =
        new TableNormalizeDispatcher(
            new TableOperationDispatcher(manager, entityStore, idGenerator), manager);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "table");
    TableChange[] requests = {TableChange.renameColumn(new String[] {"MixedOld"}, "MixedNew")};
    Assertions.assertSame(
        stop,
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> dispatcher.alterTable(ident, requests)));
    ArgumentCaptor<TableChange[]> captor = ArgumentCaptor.forClass(TableChange[].class);
    Mockito.verify(tables).alterTable(Mockito.any(), captor.capture());
    TableChange[] actual = captor.getValue();
    Assertions.assertEquals("mixedold", ((TableChange.RenameColumn) actual[0]).fieldName()[0]);
    Assertions.assertEquals("mixednew", ((TableChange.RenameColumn) actual[0]).getNewName());
    // One rename pass makes three capability probes in 1.3. A second operation-dispatcher pass
    // would double those probes even though lowercasing itself is idempotent on this branch.
    Mockito.verify(capability, Mockito.times(3)).caseSensitiveOnName(Capability.Scope.COLUMN);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testExistingColumnReferencesAreCaseFoldedBeforeValidation(boolean uppercaseReference)
      throws Exception {
    TableDispatcher delegate = Mockito.mock(TableDispatcher.class);
    CatalogManager manager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    Capability capability =
        new Capability() {
          @Override
          public CapabilityResult caseSensitiveOnName(Scope scope) {
            return scope == Scope.COLUMN
                ? CapabilityResult.unsupported("case insensitive columns")
                : CapabilityResult.SUPPORTED;
          }

          @Override
          public CapabilityResult specificationOnName(Scope scope, String name) {
            return scope != Scope.COLUMN || name.matches("[a-z_][a-z0-9_]*")
                ? CapabilityResult.SUPPORTED
                : CapabilityResult.unsupported("lowercase column names required");
          }
        };
    Mockito.when(catalog.capability()).thenReturn(capability);
    CatalogTestUtils.mockDoWithCatalog(manager, catalog);
    TableNormalizeDispatcher dispatcher = new TableNormalizeDispatcher(delegate, manager);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "table");
    String[] column = {uppercaseReference ? "COL" : "col"};
    String previous = uppercaseReference ? "previous" : "PREVIOUS";
    dispatcher.alterTable(
        ident,
        TableChange.updateColumnComment(column, "comment"),
        TableChange.renameColumn(column, "RENAMED"),
        TableChange.deleteColumn(column, false),
        TableChange.updateColumnPosition(column, TableChange.ColumnPosition.after(previous)));

    ArgumentCaptor<TableChange[]> captor = ArgumentCaptor.forClass(TableChange[].class);
    Mockito.verify(delegate).alterTable(Mockito.eq(ident), captor.capture());
    TableChange[] changes = captor.getValue();
    Assertions.assertEquals(4, changes.length);
    for (TableChange change : changes) {
      Assertions.assertArrayEquals(
          new String[] {"col"}, ((TableChange.ColumnChange) change).fieldName());
    }
    Assertions.assertEquals("renamed", ((TableChange.RenameColumn) changes[1]).getNewName());
    Assertions.assertEquals(
        "previous",
        ((TableChange.After) ((TableChange.UpdateColumnPosition) changes[3]).getPosition())
            .getColumn());
  }

  @Test
  void testDeletionCanUseProviderValidatedNativeNamesWithoutRelaxingCreation() throws Exception {
    TableDispatcher delegate = Mockito.mock(TableDispatcher.class);
    CatalogManager manager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    Capability capability =
        new Capability() {
          @Override
          public CapabilityResult specificationOnExistingName(Scope scope, String name) {
            return CapabilityResult.SUPPORTED;
          }
        };
    Mockito.when(catalog.capability()).thenReturn(capability);
    CatalogTestUtils.mockDoWithCatalog(manager, catalog);
    TableNormalizeDispatcher dispatcher = new TableNormalizeDispatcher(delegate, manager);
    NameIdentifier nativeIdent =
        NameIdentifier.of("metalake", "catalog", "native schema", "native\"table");
    dispatcher.dropTable(nativeIdent);
    dispatcher.purgeTable(nativeIdent);
    Mockito.verify(delegate).dropTable(nativeIdent);
    Mockito.verify(delegate).purgeTable(nativeIdent);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            dispatcher.createTable(
                nativeIdent,
                new Column[0],
                null,
                Map.of(),
                new Transform[0],
                Distributions.NONE,
                new SortOrder[0],
                new Index[0]));
  }

  @Test
  void testDeletionKeepsDefaultNameRestrictions() throws Exception {
    TableDispatcher delegate = Mockito.mock(TableDispatcher.class);
    CatalogManager manager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalog.capability()).thenReturn(Capability.DEFAULT);
    CatalogTestUtils.mockDoWithCatalog(manager, catalog);
    TableNormalizeDispatcher dispatcher = new TableNormalizeDispatcher(delegate, manager);
    NameIdentifier invalid = NameIdentifier.of("metalake", "catalog", "schema", "native\"table");
    Assertions.assertThrows(IllegalArgumentException.class, () -> dispatcher.dropTable(invalid));
    Assertions.assertThrows(IllegalArgumentException.class, () -> dispatcher.purgeTable(invalid));
    Mockito.verifyNoInteractions(delegate);
  }

  @Test
  public void testNameCaseInsensitive() {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema81");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaNormalizeDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    // test case-insensitive in creation
    NameIdentifier tableIdent = NameIdentifier.of(tableNs, "tableNAME");
    Column[] columns =
        new Column[] {
          TestColumn.builder()
              .withName("colNAME1")
              .withPosition(0)
              .withType(Types.StringType.get())
              .build(),
          TestColumn.builder()
              .withName("colNAME2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .build()
        };
    RangePartition assignedPartition =
        Partitions.range(
            "partition_V1",
            Literals.stringLiteral("value1"),
            Literals.stringLiteral("value2"),
            null);
    Transform[] transforms =
        new Transform[] {
          Transforms.range(
              new String[] {columns[0].name()}, new RangePartition[] {assignedPartition})
        };
    Distribution distribution =
        Distributions.fields(Strategy.HASH, 5, new String[] {columns[0].name()});
    SortOrder[] sortOrders =
        new SortOrder[] {SortOrders.ascending(NamedReference.field(columns[0].name()))};
    Index[] indexes = new Index[] {Indexes.primary("index1", new String[][] {{columns[0].name()}})};
    Table createdTable =
        tableNormalizeDispatcher.createTable(
            tableIdent, columns, "comment", props, transforms, distribution, sortOrders, indexes);
    assertTableCaseInsensitive(tableIdent, columns, createdTable);
    // test case-insensitive in loading
    Table loadedTable = tableNormalizeDispatcher.loadTable(tableIdent);
    assertTableCaseInsensitive(tableIdent, columns, loadedTable);
    Assertions.assertEquals(
        assignedPartition.name().toLowerCase(),
        loadedTable.partitioning()[0].assignments()[0].name());

    // test case-insensitive in listing
    NameIdentifier[] tableIdents = tableNormalizeDispatcher.listTables(tableNs);
    Arrays.stream(tableIdents)
        .forEach(s -> Assertions.assertEquals(s.name().toLowerCase(), s.name()));

    // test case-insensitive in altering
    Table alteredTable =
        tableNormalizeDispatcher.alterTable(
            NameIdentifier.of(tableNs, tableIdent.name().toLowerCase()),
            TableChange.setProperty("k2", "v2"));
    assertTableCaseInsensitive(tableIdent, columns, alteredTable);

    Exception exception =
        Assertions.assertThrows(
            TableAlreadyExistsException.class,
            () ->
                tableNormalizeDispatcher.alterTable(
                    NameIdentifier.of(tableNs, tableIdent.name().toUpperCase()),
                    TableChange.rename(tableIdent.name().toUpperCase())));
    Assertions.assertEquals(
        "Table metalake.catalog.schema81.tablename already exists", exception.getMessage());

    // test case-insensitive in dropping
    Assertions.assertTrue(
        tableNormalizeDispatcher.dropTable(
            NameIdentifier.of(tableNs, tableIdent.name().toUpperCase())));
  }

  @Test
  public void testNameSpec() {
    Namespace tableNs = Namespace.of(metalake, catalog, "testNameSpec");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaNormalizeDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent1 =
        NameIdentifier.of(tableNs, MetadataObjects.METADATA_OBJECT_RESERVED_NAME);
    Column[] columns =
        new Column[] {
          TestColumn.builder()
              .withName("colNAME1")
              .withPosition(0)
              .withType(Types.StringType.get())
              .build(),
          TestColumn.builder()
              .withName("colNAME2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .build()
        };
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> tableNormalizeDispatcher.createTable(tableIdent1, columns, "comment", props));
    Assertions.assertEquals(
        "The TABLE name '*' is reserved. Illegal name: *", exception.getMessage());

    NameIdentifier tableIdent2 = NameIdentifier.of(tableNs, "a?");
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> tableNormalizeDispatcher.createTable(tableIdent2, columns, "comment", props));
    Assertions.assertEquals(
        "The TABLE name 'a?' is illegal. Illegal name: a?", exception.getMessage());

    NameIdentifier tableIdent3 = NameIdentifier.of(tableNs, "abc");
    Column[] columns1 =
        new Column[] {
          TestColumn.builder()
              .withName(MetadataObjects.METADATA_OBJECT_RESERVED_NAME)
              .withPosition(0)
              .withType(Types.StringType.get())
              .build()
        };
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> tableNormalizeDispatcher.createTable(tableIdent3, columns1, "comment", props));
    Assertions.assertEquals(
        "The COLUMN name '*' is reserved. Illegal name: *", exception.getMessage());
  }

  private void assertTableCaseInsensitive(
      NameIdentifier tableIdent, Column[] expectedColumns, Table table) {
    Assertions.assertEquals(tableIdent.name().toLowerCase(), table.name());
    Set<String> expectedColumnNames =
        Arrays.stream(expectedColumns).map(c -> c.name().toLowerCase()).collect(Collectors.toSet());
    Set<String> actualColumnNames =
        Arrays.stream(table.columns()).map(Column::name).collect(Collectors.toSet());
    Assertions.assertEquals(expectedColumnNames, actualColumnNames);
    Assertions.assertEquals(
        expectedColumns[0].name().toLowerCase(),
        table.partitioning()[0].references()[0].fieldName()[0]);
    Assertions.assertEquals(
        expectedColumns[0].name().toLowerCase(),
        table.distribution().references()[0].fieldName()[0]);
    Assertions.assertEquals(
        expectedColumns[0].name().toLowerCase(),
        table.sortOrder()[0].expression().references()[0].fieldName()[0]);
    Assertions.assertEquals(
        expectedColumns[0].name().toLowerCase(), table.index()[0].fieldNames()[0][0].toLowerCase());
  }
}
