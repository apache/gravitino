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
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.SupportsTableNameResolution;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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

  @Test
  public void testCreateTableListTablesLoadTableRoundTrip() throws Exception {
    // Mirrors the correct usage pattern for a quote-aware catalog (e.g. Oracle):
    // 1. CREATE TABLE "My Table" is issued with a quoted name, so the catalog stores the
    //    physical table with its case preserved: My Table.
    // 2. listTables() surfaces that physical name unquoted (My Table) -- this is the behavior
    //    this PR's fix guarantees: the name is not re-folded by the core layer.
    // 3. Loading the table again requires re-quoting the case-sensitive name ("My Table"), not
    //    passing the bare listed name back in: normalizeName cannot tell an already-canonical
    //    name apart from raw user input, so an unquoted "My Table" would be folded to MY TABLE.
    Namespace tableNs = Namespace.of(metalake, catalog, "schema");
    TableDispatcher mockDispatcher = Mockito.mock(TableDispatcher.class);
    Mockito.when(
            mockDispatcher.createTable(
                Mockito.any(NameIdentifier.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(Mockito.mock(Table.class));
    Mockito.when(mockDispatcher.loadTable(Mockito.any(NameIdentifier.class)))
        .thenReturn(Mockito.mock(Table.class));

    CatalogManager mockCatalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> mockCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(mockCatalog.capability()).thenReturn(TestCapabilityHelpers.QUOTE_AWARE_CAPABILITY);
    CatalogTestUtils.mockDoWithCatalog(mockCatalogManager, mockCatalog);

    TableNormalizeDispatcher dispatcher =
        new TableNormalizeDispatcher(mockDispatcher, mockCatalogManager);

    // 1. Create the table with a quoted, case-sensitive name.
    NameIdentifier quotedCreateIdent = NameIdentifier.of(tableNs, "\"My Table\"");
    dispatcher.createTable(
        quotedCreateIdent,
        new Column[0],
        "comment",
        ImmutableMap.of(),
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);

    ArgumentCaptor<NameIdentifier> createdIdentCaptor =
        ArgumentCaptor.forClass(NameIdentifier.class);
    Mockito.verify(mockDispatcher)
        .createTable(
            createdIdentCaptor.capture(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
    String physicalName = createdIdentCaptor.getValue().name();
    Assertions.assertEquals("My Table", physicalName);

    // 2. listTables() must surface that physical name unchanged.
    Mockito.when(mockDispatcher.listTables(Mockito.any(Namespace.class)))
        .thenReturn(new NameIdentifier[] {NameIdentifier.of(tableNs, physicalName)});
    NameIdentifier[] listed = dispatcher.listTables(tableNs);
    Assertions.assertEquals(1, listed.length);
    Assertions.assertEquals("My Table", listed[0].name());

    // 3. Loading the table again requires re-quoting the listed name.
    NameIdentifier quotedLoadIdent = NameIdentifier.of(tableNs, "\"" + listed[0].name() + "\"");
    dispatcher.loadTable(quotedLoadIdent);

    ArgumentCaptor<NameIdentifier> loadedIdentCaptor =
        ArgumentCaptor.forClass(NameIdentifier.class);
    Mockito.verify(mockDispatcher).loadTable(loadedIdentCaptor.capture());
    Assertions.assertEquals("My Table", loadedIdentCaptor.getValue().name());
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

  @Test
  public void testPhysicalNameResolutionDrivesDownstreamIdentifier() throws Exception {
    // When the catalog ops implements SupportsTableNameResolution and maps a normalized name to a
    // differently-stored physical name, TableNormalizeDispatcher must hand that resolved name to
    // the downstream dispatcher for load/alter/drop/purge/exists. Resolving above the hook and
    // operation layers means the same resolved identifier drives the authorization hooks, the
    // catalog call and the entity store key.
    Namespace tableNs = Namespace.of(metalake, catalog, "schema");
    // An unquoted request folds to uppercase PHYSICAL_NAME; the resolver maps it to the stored
    // case-sensitive "physical_Name".
    NameIdentifier requested = NameIdentifier.of(tableNs, "physical_name");
    NameIdentifier resolved = NameIdentifier.of(tableNs, "physical_Name");

    TableDispatcher mockDispatcher = Mockito.mock(TableDispatcher.class);
    Mockito.when(mockDispatcher.loadTable(Mockito.any())).thenReturn(Mockito.mock(Table.class));
    Mockito.when(mockDispatcher.alterTable(Mockito.any())).thenReturn(Mockito.mock(Table.class));
    Mockito.when(mockDispatcher.dropTable(Mockito.any())).thenReturn(true);
    Mockito.when(mockDispatcher.purgeTable(Mockito.any())).thenReturn(true);
    Mockito.when(mockDispatcher.tableExists(Mockito.any())).thenReturn(true);

    // Resolver maps normalized "PHYSICAL_NAME" -> stored "physical_Name"; identity otherwise.
    SupportsTableNameResolution resolver =
        (requestedIdent, normalizedIdent) ->
            "PHYSICAL_NAME".equals(normalizedIdent.name()) ? resolved : normalizedIdent;
    TableNormalizeDispatcher dispatcher =
        newResolvingDispatcher(mockDispatcher, resolvingCatalogOps(resolver));

    dispatcher.loadTable(requested);
    dispatcher.alterTable(requested, TableChange.rename("x"));
    dispatcher.dropTable(requested);
    dispatcher.purgeTable(requested);
    dispatcher.tableExists(requested);

    assertDispatchedName(mockDispatcher, "physical_Name");
  }

  @Test
  public void testPhysicalNameResolutionReceivesRequestedAndNormalizedNames() throws Exception {
    // The resolver sees both the original requested name and the normalized one, so it can honor a
    // case-sensitive name supplied verbatim.
    Namespace tableNs = Namespace.of(metalake, catalog, "schema");
    NameIdentifier requested = NameIdentifier.of(tableNs, "\"MixedCase\"");

    TableDispatcher mockDispatcher = Mockito.mock(TableDispatcher.class);
    Mockito.when(mockDispatcher.loadTable(Mockito.any())).thenReturn(Mockito.mock(Table.class));

    SupportsTableNameResolution resolver =
        (requestedIdent, normalizedIdent) -> {
          // requested is the raw caller input (still quoted); normalized is the unquoted form.
          Assertions.assertEquals("\"MixedCase\"", requestedIdent.name());
          Assertions.assertEquals("MixedCase", normalizedIdent.name());
          return normalizedIdent;
        };
    TableNormalizeDispatcher dispatcher =
        newResolvingDispatcher(mockDispatcher, resolvingCatalogOps(resolver));

    dispatcher.loadTable(requested);
    ArgumentCaptor<NameIdentifier> captor = ArgumentCaptor.forClass(NameIdentifier.class);
    Mockito.verify(mockDispatcher).loadTable(captor.capture());
    Assertions.assertEquals("MixedCase", captor.getValue().name());
  }

  @Test
  public void testPhysicalNameResolutionAmbiguousKeepsNormalized() throws Exception {
    // A resolver that cannot disambiguate returns the normalized name; the dispatcher passes it
    // through unchanged so the usual not-found behavior surfaces.
    Namespace tableNs = Namespace.of(metalake, catalog, "schema");
    NameIdentifier requested = NameIdentifier.of(tableNs, "\"ambiguous\"");

    TableDispatcher mockDispatcher = Mockito.mock(TableDispatcher.class);
    Mockito.when(mockDispatcher.dropTable(Mockito.any())).thenReturn(false);
    Mockito.when(mockDispatcher.tableExists(Mockito.any())).thenReturn(false);

    SupportsTableNameResolution resolver =
        (requestedIdent, normalizedIdent) -> normalizedIdent; // keep normalized (ambiguous/absent)
    TableNormalizeDispatcher dispatcher =
        newResolvingDispatcher(mockDispatcher, resolvingCatalogOps(resolver));

    // Not-found keeps boolean semantics (never throws from resolution).
    Assertions.assertFalse(dispatcher.dropTable(requested));
    Assertions.assertFalse(dispatcher.tableExists(requested));
    ArgumentCaptor<NameIdentifier> captor = ArgumentCaptor.forClass(NameIdentifier.class);
    Mockito.verify(mockDispatcher).dropTable(captor.capture());
    Assertions.assertEquals("ambiguous", captor.getValue().name());
  }

  @Test
  public void testPhysicalNameResolutionNoCapabilityIsIdentity() throws Exception {
    // A catalog whose ops does not implement SupportsTableNameResolution leaves the normalized
    // identifier unchanged -- no behavior change for the vast majority of catalogs.
    Namespace tableNs = Namespace.of(metalake, catalog, "schema");
    NameIdentifier requested = NameIdentifier.of(tableNs, "\"MixedCase\"");

    TableDispatcher mockDispatcher = Mockito.mock(TableDispatcher.class);
    Mockito.when(mockDispatcher.loadTable(Mockito.any())).thenReturn(Mockito.mock(Table.class));

    // ops() returns a plain CatalogOperations that is NOT a SupportsTableNameResolution.
    TableNormalizeDispatcher dispatcher =
        newResolvingDispatcher(mockDispatcher, Mockito.mock(CatalogOperations.class));

    dispatcher.loadTable(requested);
    ArgumentCaptor<NameIdentifier> captor = ArgumentCaptor.forClass(NameIdentifier.class);
    Mockito.verify(mockDispatcher).loadTable(captor.capture());
    // Quote-aware capability unquotes to the preserved "MixedCase"; resolution is a no-op.
    Assertions.assertEquals("MixedCase", captor.getValue().name());
  }

  @Test
  public void testPhysicalNameResolutionPropagatesNoSuchCatalog() {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema");
    NameIdentifier requested = NameIdentifier.of(tableNs, "t");

    TableDispatcher mockDispatcher = Mockito.mock(TableDispatcher.class);
    CatalogManager mockCatalogManager = Mockito.mock(CatalogManager.class);
    Mockito.when(mockCatalogManager.doWithCatalog(Mockito.any(), Mockito.any()))
        .thenThrow(new NoSuchCatalogException("no catalog"));

    TableNormalizeDispatcher dispatcher =
        new TableNormalizeDispatcher(mockDispatcher, mockCatalogManager);

    Assertions.assertThrows(NoSuchCatalogException.class, () -> dispatcher.loadTable(requested));
  }

  private void assertDispatchedName(TableDispatcher mockDispatcher, String expected)
      throws Exception {
    ArgumentCaptor<NameIdentifier> loadCaptor = ArgumentCaptor.forClass(NameIdentifier.class);
    Mockito.verify(mockDispatcher).loadTable(loadCaptor.capture());
    Assertions.assertEquals(expected, loadCaptor.getValue().name());

    ArgumentCaptor<NameIdentifier> alterCaptor = ArgumentCaptor.forClass(NameIdentifier.class);
    Mockito.verify(mockDispatcher).alterTable(alterCaptor.capture(), Mockito.any());
    Assertions.assertEquals(expected, alterCaptor.getValue().name());

    ArgumentCaptor<NameIdentifier> dropCaptor = ArgumentCaptor.forClass(NameIdentifier.class);
    Mockito.verify(mockDispatcher).dropTable(dropCaptor.capture());
    Assertions.assertEquals(expected, dropCaptor.getValue().name());

    ArgumentCaptor<NameIdentifier> purgeCaptor = ArgumentCaptor.forClass(NameIdentifier.class);
    Mockito.verify(mockDispatcher).purgeTable(purgeCaptor.capture());
    Assertions.assertEquals(expected, purgeCaptor.getValue().name());

    ArgumentCaptor<NameIdentifier> existsCaptor = ArgumentCaptor.forClass(NameIdentifier.class);
    Mockito.verify(mockDispatcher).tableExists(existsCaptor.capture());
    Assertions.assertEquals(expected, existsCaptor.getValue().name());
  }

  /**
   * Builds a TableNormalizeDispatcher whose (mocked) catalog uses a quote-aware capability and
   * whose ops is the given CatalogOperations.
   */
  private TableNormalizeDispatcher newResolvingDispatcher(
      TableDispatcher downstream, CatalogOperations ops) {
    CatalogManager mockCatalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> mockCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(mockCatalog.capability()).thenReturn(TestCapabilityHelpers.QUOTE_AWARE_CAPABILITY);
    Mockito.when(mockCatalog.ops()).thenReturn(ops);
    CatalogTestUtils.mockDoWithCatalog(mockCatalogManager, mockCatalog);
    return new TableNormalizeDispatcher(downstream, mockCatalogManager);
  }

  /**
   * A CatalogOperations that also implements SupportsTableNameResolution with the given resolver.
   */
  private CatalogOperations resolvingCatalogOps(SupportsTableNameResolution resolver) {
    CatalogOperations ops =
        Mockito.mock(
            CatalogOperations.class,
            Mockito.withSettings().extraInterfaces(SupportsTableNameResolution.class));
    Mockito.when(((SupportsTableNameResolution) ops).resolveTableName(Mockito.any(), Mockito.any()))
        .thenAnswer(
            invocation ->
                resolver.resolveTableName(invocation.getArgument(0), invocation.getArgument(1)));
    return ops;
  }
}
