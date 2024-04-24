/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.Entity.SECURABLE_ENTITY_RESERVED_NAME;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.TestColumn;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.distributions.Strategy;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestTableNormalizeDispatcher extends TestTableOperationDispatcher {
  private static TableNormalizeDispatcher tableNormalizeDispatcher;
  private static SchemaNormalizeDispatcher schemaNormalizeDispatcher;

  @BeforeAll
  public static void initialize() throws IOException {
    TestTableOperationDispatcher.initialize();
    tableNormalizeDispatcher = new TableNormalizeDispatcher(tableOperationDispatcher);
    schemaNormalizeDispatcher = new SchemaNormalizeDispatcher(schemaOperationDispatcher);
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
          TestColumn.builder().withName("colNAME1").withType(Types.StringType.get()).build(),
          TestColumn.builder().withName("colNAME2").withType(Types.StringType.get()).build()
        };
    Transform[] transforms = new Transform[] {Transforms.identity(columns[0].name())};
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

    NameIdentifier tableIdent1 = NameIdentifier.of(tableNs, SECURABLE_ENTITY_RESERVED_NAME);
    Column[] columns =
        new Column[] {
          TestColumn.builder().withName("colNAME1").withType(Types.StringType.get()).build(),
          TestColumn.builder().withName("colNAME2").withType(Types.StringType.get()).build()
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
              .withName(SECURABLE_ENTITY_RESERVED_NAME)
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
    Assertions.assertEquals(expectedColumns[0].name().toLowerCase(), table.columns()[0].name());
    Assertions.assertEquals(expectedColumns[1].name().toLowerCase(), table.columns()[1].name());
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
