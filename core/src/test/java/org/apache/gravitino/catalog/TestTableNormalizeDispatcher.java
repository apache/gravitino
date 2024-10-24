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
