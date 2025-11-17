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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.GenericColumn;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestManagedTableOperations {

  public static class InMemoryManagedTableOperations extends ManagedTableOperations {

    private final EntityStore entityStore;

    private final SupportsSchemas supportsSchemas;

    private final IdGenerator idGenerator;

    public InMemoryManagedTableOperations(
        EntityStore entityStore, SupportsSchemas supportsSchemas, IdGenerator idGenerator) {
      this.entityStore = entityStore;
      this.supportsSchemas = supportsSchemas;
      this.idGenerator = idGenerator;
    }

    @Override
    protected EntityStore store() {
      return entityStore;
    }

    @Override
    protected SupportsSchemas schemas() {
      return supportsSchemas;
    }

    @Override
    protected IdGenerator idGenerator() {
      return idGenerator;
    }
  }

  private static final String METALAKE_NAME = "test_metalake";
  private static final String CATALOG_NAME = "test_catalog";
  private static final String SCHEMA_NAME = "schema1";

  private final EntityStore store = new TestMemoryEntityStore.InMemoryEntityStore();
  private final SupportsSchemas schemas =
      new ManagedSchemaOperations() {
        @Override
        protected EntityStore store() {
          return store;
        }
      };
  private final IdGenerator idGenerator = new RandomIdGenerator();

  private ManagedTableOperations tableOperations =
      new InMemoryManagedTableOperations(store, schemas, idGenerator);

  @BeforeEach
  public void setUp() {
    ((TestMemoryEntityStore.InMemoryEntityStore) store).clear();
    // Create a schema
    NameIdentifier schemaIdent =
        NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> schemaProperties =
        StringIdentifier.newPropertiesWithId(stringId, Collections.emptyMap());
    Schema schema = schemas.createSchema(schemaIdent, "Test Schema 1", schemaProperties);
    Assertions.assertEquals("schema1", schema.name());
  }

  @Test
  public void testCreateAndListTables() {
    // Create a table
    NameIdentifier table1Ident =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "table1");
    Column column1 = createColumn("col1", Types.StringType.get(), null);
    Column column2 = createColumn("col2", Types.IntegerType.get(), Literals.integerLiteral(1));
    Column[] columns = new Column[] {column1, column2};
    Transform[] partitioning = new Transform[0];
    Distribution distribution = Distributions.NONE;
    Index[] indexes = Indexes.EMPTY_INDEXES;
    SortOrder[] sortOrders = new SortOrder[0];

    tableOperations.createTable(
        table1Ident,
        columns,
        "Test Table 1",
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(idGenerator.nextId()), Collections.emptyMap()),
        partitioning,
        distribution,
        sortOrders,
        indexes);

    NameIdentifier table2Ident =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "table2");
    tableOperations.createTable(
        table2Ident,
        columns,
        "Test Table 2",
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(idGenerator.nextId()), Collections.emptyMap()),
        partitioning,
        distribution,
        sortOrders,
        indexes);

    // List tables
    NameIdentifier[] tableIdents =
        tableOperations.listTables(NamespaceUtil.ofTable(METALAKE_NAME, CATALOG_NAME, "schema1"));
    Assertions.assertEquals(2, tableIdents.length);
    Set<String> tableNames =
        Arrays.stream(tableIdents).map(NameIdentifier::name).collect(Collectors.toSet());

    Assertions.assertTrue(tableNames.contains("table1"));
    Assertions.assertTrue(tableNames.contains("table2"));
  }

  @Test
  public void testCreateAndLoadTable() {
    NameIdentifier table1Ident =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "table1");
    Column column1 = createColumn("col1", Types.StringType.get(), null);
    Column column2 = createColumn("col2", Types.IntegerType.get(), Literals.integerLiteral(1));
    Column[] columns = new Column[] {column1, column2};
    Transform[] partitioning = new Transform[0];
    Distribution distribution = Distributions.NONE;
    Index[] indexes = Indexes.EMPTY_INDEXES;
    SortOrder[] sortOrders = new SortOrder[0];

    Table newTable =
        tableOperations.createTable(
            table1Ident,
            columns,
            "Test Table 1",
            StringIdentifier.newPropertiesWithId(
                StringIdentifier.fromId(idGenerator.nextId()), Collections.emptyMap()),
            partitioning,
            distribution,
            sortOrders,
            indexes);

    Table loadedTable = tableOperations.loadTable(table1Ident);
    assertTableEquals(newTable, loadedTable);

    // Test create table that already exists
    Assertions.assertThrows(
        TableAlreadyExistsException.class,
        () ->
            tableOperations.createTable(
                table1Ident,
                columns,
                "Test Table 1",
                StringIdentifier.newPropertiesWithId(
                    StringIdentifier.fromId(idGenerator.nextId()), Collections.emptyMap()),
                partitioning,
                distribution,
                sortOrders,
                indexes));

    // Test load non-existing table
    NameIdentifier nonExistingTableIdent =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "non_existing_table");
    Assertions.assertThrows(
        NoSuchTableException.class, () -> tableOperations.loadTable(nonExistingTableIdent));
  }

  @Test
  public void testCreateAndDropTable() {
    NameIdentifier table1Ident =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "table1");
    Column column1 = createColumn("col1", Types.StringType.get(), null);
    Column column2 = createColumn("col2", Types.IntegerType.get(), Literals.integerLiteral(1));
    Column[] columns = new Column[] {column1, column2};
    Transform[] partitioning = new Transform[0];
    Distribution distribution = Distributions.NONE;
    Index[] indexes = Indexes.EMPTY_INDEXES;
    SortOrder[] sortOrders = new SortOrder[0];

    tableOperations.createTable(
        table1Ident,
        columns,
        "Test Table 1",
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(idGenerator.nextId()), Collections.emptyMap()),
        partitioning,
        distribution,
        sortOrders,
        indexes);

    // Drop the table
    boolean dropped = tableOperations.dropTable(table1Ident);
    Assertions.assertTrue(dropped);

    // Verify the table is dropped
    Assertions.assertThrows(
        NoSuchTableException.class, () -> tableOperations.loadTable(table1Ident));

    // Test drop non-existing table
    Assertions.assertFalse(() -> tableOperations.dropTable(table1Ident));
  }

  @Test
  public void testCreateAndPurgeTable() {
    NameIdentifier table1Ident =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "table1");
    Column column1 = createColumn("col1", Types.StringType.get(), null);
    Column column2 = createColumn("col2", Types.IntegerType.get(), Literals.integerLiteral(1));
    Column[] columns = new Column[] {column1, column2};
    Transform[] partitioning = new Transform[0];
    Distribution distribution = Distributions.NONE;
    Index[] indexes = Indexes.EMPTY_INDEXES;
    SortOrder[] sortOrders = new SortOrder[0];

    tableOperations.createTable(
        table1Ident,
        columns,
        "Test Table 1",
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(idGenerator.nextId()), Collections.emptyMap()),
        partitioning,
        distribution,
        sortOrders,
        indexes);

    // Purge the table
    boolean purged = tableOperations.purgeTable(table1Ident);
    Assertions.assertTrue(purged);

    // Verify the table is purged
    Assertions.assertThrows(
        NoSuchTableException.class, () -> tableOperations.loadTable(table1Ident));

    // Test purge non-existing table
    Assertions.assertFalse(() -> tableOperations.purgeTable(table1Ident));
  }

  private Column createColumn(String name, Type dataType, Expression defaultValue) {
    return GenericColumn.builder()
        .withName(name)
        .withComment("Test column " + name)
        .withType(dataType)
        .withDefaultValue(defaultValue)
        .withNullable(true)
        .withAutoIncrement(false)
        .build();
  }

  @Test
  public void testAlterTable() {
    NameIdentifier table1Ident =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "table1");
    Column column1 = createColumn("col1", Types.StringType.get(), null);
    Column column2 = createColumn("col2", Types.IntegerType.get(), Literals.integerLiteral(1));
    Column[] columns = new Column[] {column1, column2};
    Transform[] partitioning = new Transform[0];
    Distribution distribution = Distributions.NONE;
    Index[] indexes = Indexes.EMPTY_INDEXES;
    SortOrder[] sortOrders = new SortOrder[0];

    tableOperations.createTable(
        table1Ident,
        columns,
        "Test Table 1",
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(idGenerator.nextId()), Collections.emptyMap()),
        partitioning,
        distribution,
        sortOrders,
        indexes);

    // Test rename the table
    Table renamedTable =
        tableOperations.alterTable(table1Ident, TableChange.rename("table1_renamed"));
    Assertions.assertEquals("table1_renamed", renamedTable.name());

    Table loadedRenamedTable =
        tableOperations.loadTable(
            NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "table1_renamed"));
    Assertions.assertEquals("table1_renamed", loadedRenamedTable.name());

    // Test rename the table to another schema
    NameIdentifier renamedTable1Ident =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "table1_renamed");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableOperations.alterTable(
                renamedTable1Ident, TableChange.rename("table1_moved", "schema2")));

    // Test update the table comment
    String newComment = "Updated Test Table 1 Comment";
    Table updatedCommentTable =
        tableOperations.alterTable(renamedTable1Ident, TableChange.updateComment(newComment));
    Assertions.assertEquals(newComment, updatedCommentTable.comment());

    Table loadedUpdatedCommentTable = tableOperations.loadTable(renamedTable1Ident);
    Assertions.assertEquals(newComment, loadedUpdatedCommentTable.comment());

    // Test set a new property
    String propertyKey = "property1";
    String propertyValue = "value1";
    Table updatedPropertyTable =
        tableOperations.alterTable(
            renamedTable1Ident, TableChange.setProperty(propertyKey, propertyValue));
    Assertions.assertEquals(propertyValue, updatedPropertyTable.properties().get(propertyKey));

    Table loadedUpdatedPropertyTable = tableOperations.loadTable(renamedTable1Ident);
    Assertions.assertEquals(
        propertyValue, loadedUpdatedPropertyTable.properties().get(propertyKey));

    // Test remove the property
    Table removedPropertyTable =
        tableOperations.alterTable(renamedTable1Ident, TableChange.removeProperty(propertyKey));
    Assertions.assertNull(removedPropertyTable.properties().get(propertyKey));

    Table loadedRemovedPropertyTable = tableOperations.loadTable(renamedTable1Ident);
    Assertions.assertNull(loadedRemovedPropertyTable.properties().get(propertyKey));

    // Test remove the non-existing property
    Table removeNonExistingPropertyTable =
        tableOperations.alterTable(
            renamedTable1Ident, TableChange.removeProperty("non_existing_property"));
    Assertions.assertEquals(
        removedPropertyTable.properties(), removeNonExistingPropertyTable.properties());

    // Test Add the index
    Table addedIndexTable =
        tableOperations.alterTable(
            renamedTable1Ident,
            TableChange.addIndex(Index.IndexType.PRIMARY_KEY, "index1", new String[][] {{"col1"}}));
    Assertions.assertEquals(1, addedIndexTable.index().length);
    Assertions.assertEquals("index1", addedIndexTable.index()[0].name());
    Assertions.assertEquals(Index.IndexType.PRIMARY_KEY, addedIndexTable.index()[0].type());

    Table loadedAddedIndexTable = tableOperations.loadTable(renamedTable1Ident);
    Assertions.assertEquals(1, loadedAddedIndexTable.index().length);
    Assertions.assertEquals("index1", loadedAddedIndexTable.index()[0].name());
    Assertions.assertEquals(Index.IndexType.PRIMARY_KEY, loadedAddedIndexTable.index()[0].type());

    // Test Remove the index
    Table removedIndexTable =
        tableOperations.alterTable(renamedTable1Ident, TableChange.deleteIndex("index1", true));
    Assertions.assertEquals(0, removedIndexTable.index().length);

    Table loadedRemovedIndexTable = tableOperations.loadTable(renamedTable1Ident);
    Assertions.assertEquals(0, loadedRemovedIndexTable.index().length);

    // Test Remove the non-existing index
    Table removeNonExistingIndexTable =
        tableOperations.alterTable(
            renamedTable1Ident, TableChange.deleteIndex("non_existing_index", true));
    Assertions.assertEquals(0, removeNonExistingIndexTable.index().length);

    // Test Remove the non-existing index without ifExists
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableOperations.alterTable(
                renamedTable1Ident, TableChange.deleteIndex("non_existing_index", false)));

    // Test alter non-existing table
    NameIdentifier nonExistingTableIdent =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "non_existing_table");
    Assertions.assertThrows(
        NoSuchTableException.class,
        () ->
            tableOperations.alterTable(nonExistingTableIdent, TableChange.rename("another_name")));
  }

  @Test
  public void testAlterAddAndDeleteColumns() {
    NameIdentifier table1Ident =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "table1");
    Column column1 = createColumn("col1", Types.StringType.get(), null);
    Column column2 = createColumn("col2", Types.IntegerType.get(), Literals.integerLiteral(1));
    Column[] columns = new Column[] {column1, column2};
    Transform[] partitioning = new Transform[0];
    Distribution distribution = Distributions.NONE;
    Index[] indexes = Indexes.EMPTY_INDEXES;
    SortOrder[] sortOrders = new SortOrder[0];

    tableOperations.createTable(
        table1Ident,
        columns,
        "Test Table 1",
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(idGenerator.nextId()), Collections.emptyMap()),
        partitioning,
        distribution,
        sortOrders,
        indexes);

    // Test add a new column
    Table tableWithNewColumn =
        tableOperations.alterTable(
            table1Ident,
            TableChange.addColumn(
                new String[] {"col3"},
                Types.BooleanType.get(),
                TableChange.ColumnPosition.defaultPos(),
                Literals.booleanLiteral(true)));
    Assertions.assertEquals(3, tableWithNewColumn.columns().length);
    // col3 should be the last column
    Assertions.assertEquals("col3", tableWithNewColumn.columns()[2].name());
    Assertions.assertEquals(Types.BooleanType.get(), tableWithNewColumn.columns()[2].dataType());

    Table loadedTableWithNewColumn = tableOperations.loadTable(table1Ident);
    Assertions.assertEquals(3, loadedTableWithNewColumn.columns().length);
    // col3 should be the last column
    Assertions.assertEquals("col3", loadedTableWithNewColumn.columns()[2].name());
    Assertions.assertEquals(
        Types.BooleanType.get(), loadedTableWithNewColumn.columns()[2].dataType());

    // Test add a new column at first position
    Table tableWithNewColumnAtFirst =
        tableOperations.alterTable(
            table1Ident,
            TableChange.addColumn(
                new String[] {"col0"},
                Types.FloatType.get(),
                TableChange.ColumnPosition.first(),
                null));
    Assertions.assertEquals(4, tableWithNewColumnAtFirst.columns().length);
    // col0 should be the first column
    Assertions.assertEquals("col0", tableWithNewColumnAtFirst.columns()[0].name());
    Assertions.assertEquals(
        Types.FloatType.get(), tableWithNewColumnAtFirst.columns()[0].dataType());

    Table loadedTableWithNewColumnAtFirst = tableOperations.loadTable(table1Ident);
    Assertions.assertEquals(4, loadedTableWithNewColumnAtFirst.columns().length);
    // col0 should be the first column
    Assertions.assertEquals("col0", loadedTableWithNewColumnAtFirst.columns()[0].name());
    Assertions.assertEquals(
        Types.FloatType.get(), loadedTableWithNewColumnAtFirst.columns()[0].dataType());

    // Test add a new column after col1
    Table tableWithNewColumnAfterCol1 =
        tableOperations.alterTable(
            table1Ident,
            TableChange.addColumn(
                new String[] {"col1_5"},
                Types.DoubleType.get(),
                TableChange.ColumnPosition.after("col1"),
                null));
    Assertions.assertEquals(5, tableWithNewColumnAfterCol1.columns().length);
    // col1_5 should be after col1
    Assertions.assertEquals("col1_5", tableWithNewColumnAfterCol1.columns()[2].name());
    Assertions.assertEquals(
        Types.DoubleType.get(), tableWithNewColumnAfterCol1.columns()[2].dataType());

    Table loadedTableWithNewColumnAfterCol1 = tableOperations.loadTable(table1Ident);
    Assertions.assertEquals(5, loadedTableWithNewColumnAfterCol1.columns().length);
    // col1_5 should be after col1
    Assertions.assertEquals("col1_5", loadedTableWithNewColumnAfterCol1.columns()[2].name());
    Assertions.assertEquals(
        Types.DoubleType.get(), loadedTableWithNewColumnAfterCol1.columns()[2].dataType());

    // Test add a new column after non-existing column
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableOperations.alterTable(
                table1Ident,
                TableChange.addColumn(
                    new String[] {"colX"},
                    Types.DoubleType.get(),
                    TableChange.ColumnPosition.after("non_existing_column"),
                    null)));

    // Test add an existing column
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableOperations.alterTable(
                table1Ident,
                TableChange.addColumn(
                    new String[] {"col1"},
                    Types.DoubleType.get(),
                    TableChange.ColumnPosition.defaultPos(),
                    null)));

    // Test delete a column
    Table tableAfterDeleteColumn =
        tableOperations.alterTable(
            table1Ident, TableChange.deleteColumn(new String[] {"col2"}, true));
    Assertions.assertEquals(4, tableAfterDeleteColumn.columns().length);
    // col2 should be deleted
    Assertions.assertFalse(
        Arrays.stream(tableAfterDeleteColumn.columns())
            .anyMatch(column -> column.name().equals("col2")));

    Table loadedTableAfterDeleteColumn = tableOperations.loadTable(table1Ident);
    Assertions.assertEquals(4, loadedTableAfterDeleteColumn.columns().length);
    // col2 should be deleted
    Assertions.assertFalse(
        Arrays.stream(loadedTableAfterDeleteColumn.columns())
            .anyMatch(column -> column.name().equals("col2")));

    // Test delete a non-existing column with ifExists
    Table tableAfterDeleteNonExistingColumn =
        tableOperations.alterTable(
            table1Ident, TableChange.deleteColumn(new String[] {"non_existing_col"}, true));
    // The table schema should remain unchanged
    Assertions.assertEquals(
        tableAfterDeleteColumn.columns().length,
        tableAfterDeleteNonExistingColumn.columns().length);
    Assertions.assertArrayEquals(
        tableAfterDeleteColumn.columns(), tableAfterDeleteNonExistingColumn.columns());

    Table loadedTableAfterDeleteNonExistingColumn = tableOperations.loadTable(table1Ident);
    // The table schema should remain unchanged
    Assertions.assertEquals(
        tableAfterDeleteColumn.columns().length,
        loadedTableAfterDeleteNonExistingColumn.columns().length);
    Assertions.assertArrayEquals(
        tableAfterDeleteColumn.columns(), loadedTableAfterDeleteNonExistingColumn.columns());

    // Test delete a non-existing column without ifExists
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableOperations.alterTable(
                table1Ident, TableChange.deleteColumn(new String[] {"non_existing_col"}, false)));
  }

  @Test
  public void testUpdateTableColumn() {
    NameIdentifier table1Ident =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "table1");
    Column column1 = createColumn("col1", Types.StringType.get(), null);
    Column column2 = createColumn("col2", Types.IntegerType.get(), Literals.integerLiteral(1));
    Column[] columns = new Column[] {column1, column2};
    Transform[] partitioning = new Transform[0];
    Distribution distribution = Distributions.NONE;
    Index[] indexes = Indexes.EMPTY_INDEXES;
    SortOrder[] sortOrders = new SortOrder[0];

    tableOperations.createTable(
        table1Ident,
        columns,
        "Test Table 1",
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(idGenerator.nextId()), Collections.emptyMap()),
        partitioning,
        distribution,
        sortOrders,
        indexes);

    // Test rename column col1 to col1_renamed
    Table tableAfterRenameColumn =
        tableOperations.alterTable(
            table1Ident, TableChange.renameColumn(new String[] {"col1"}, "col1_renamed"));
    Assertions.assertEquals(2, tableAfterRenameColumn.columns().length);
    // col1 should be renamed to col1_renamed
    Assertions.assertTrue(
        Arrays.stream(tableAfterRenameColumn.columns())
            .anyMatch(column -> column.name().equals("col1_renamed")));
    Assertions.assertFalse(
        Arrays.stream(tableAfterRenameColumn.columns())
            .anyMatch(column -> column.name().equals("col1")));

    // Test rename non-existing column
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableOperations.alterTable(
                table1Ident,
                TableChange.renameColumn(new String[] {"non_existing_col"}, "new_name")));

    // Test rename column to an existing column name
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableOperations.alterTable(
                table1Ident, TableChange.renameColumn(new String[] {"col2"}, "col1_renamed")));

    // Test update column default value
    Expression newDefaultValue = Literals.integerLiteral(100);
    Table tableAfterUpdateDefaultValue =
        tableOperations.alterTable(
            table1Ident,
            TableChange.updateColumnDefaultValue(new String[] {"col2"}, newDefaultValue));
    Assertions.assertEquals(2, tableAfterUpdateDefaultValue.columns().length);
    // col2 should have the new default value
    Column updatedCol2 =
        Arrays.stream(tableAfterUpdateDefaultValue.columns())
            .filter(column -> column.name().equals("col2"))
            .findFirst()
            .orElseThrow();
    Assertions.assertEquals(newDefaultValue, updatedCol2.defaultValue());

    // Test update non-existing column default value
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableOperations.alterTable(
                table1Ident,
                TableChange.updateColumnDefaultValue(
                    new String[] {"non_existing_col"}, Literals.stringLiteral("default"))));

    // Test update column type
    Type newType = Types.LongType.get();
    Table tableAfterUpdateColumnType =
        tableOperations.alterTable(
            table1Ident, TableChange.updateColumnType(new String[] {"col2"}, newType));
    Assertions.assertEquals(2, tableAfterUpdateColumnType.columns().length);
    // col2 should have the new type
    Column updatedTypeCol2 =
        Arrays.stream(tableAfterUpdateColumnType.columns())
            .filter(column -> column.name().equals("col2"))
            .findFirst()
            .orElseThrow();
    Assertions.assertEquals(newType, updatedTypeCol2.dataType());

    // Test update column comment
    String newComment = "Updated column comment";
    Table tableAfterUpdateColumnComment =
        tableOperations.alterTable(
            table1Ident, TableChange.updateColumnComment(new String[] {"col2"}, newComment));
    Assertions.assertEquals(2, tableAfterUpdateColumnComment.columns().length);
    // col2 should have the new comment
    Column updatedCommentCol2 =
        Arrays.stream(tableAfterUpdateColumnComment.columns())
            .filter(column -> column.name().equals("col2"))
            .findFirst()
            .orElseThrow();
    Assertions.assertEquals(newComment, updatedCommentCol2.comment());

    // Test update the column position
    Table tableAfterUpdateColumnPosition =
        tableOperations.alterTable(
            table1Ident,
            TableChange.updateColumnPosition(
                new String[] {"col2"}, TableChange.ColumnPosition.first()));
    Assertions.assertEquals(2, tableAfterUpdateColumnPosition.columns().length);
    // col2 should be the first column now
    Assertions.assertEquals("col2", tableAfterUpdateColumnPosition.columns()[0].name());

    // Test update the column position after a specific column
    Table tableAfterUpdateColumnPositionAfter =
        tableOperations.alterTable(
            table1Ident,
            TableChange.updateColumnPosition(
                new String[] {"col2"}, TableChange.ColumnPosition.after("col1_renamed")));

    Assertions.assertEquals(2, tableAfterUpdateColumnPositionAfter.columns().length);
    // col2 should be after col1_renamed
    Assertions.assertEquals(
        "col1_renamed", tableAfterUpdateColumnPositionAfter.columns()[0].name());
    Assertions.assertEquals("col2", tableAfterUpdateColumnPositionAfter.columns()[1].name());

    // Test update column position after non-existing column
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableOperations.alterTable(
                table1Ident,
                TableChange.updateColumnPosition(
                    new String[] {"col2"}, TableChange.ColumnPosition.after("non_existing_col"))));

    // Test update column position with unsupported position type
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableOperations.alterTable(
                table1Ident,
                TableChange.updateColumnPosition(
                    new String[] {"col2"}, TableChange.ColumnPosition.defaultPos())));

    // Test update the column nullable property
    Table tableAfterUpdateColumnNullable =
        tableOperations.alterTable(
            table1Ident, TableChange.updateColumnNullability(new String[] {"col2"}, false));
    Assertions.assertEquals(2, tableAfterUpdateColumnNullable.columns().length);
    // col2 should be not nullable now
    Column updatedNullableCol2 =
        Arrays.stream(tableAfterUpdateColumnNullable.columns())
            .filter(column -> column.name().equals("col2"))
            .findFirst()
            .orElseThrow();
    Assertions.assertFalse(updatedNullableCol2.nullable());

    // Test update the column auto-increment property
    Table tableAfterUpdateColumnAutoIncrement =
        tableOperations.alterTable(
            table1Ident, TableChange.updateColumnAutoIncrement(new String[] {"col2"}, true));
    Assertions.assertEquals(2, tableAfterUpdateColumnAutoIncrement.columns().length);
    // col2 should be auto-increment now
    Column updatedAutoIncrementCol2 =
        Arrays.stream(tableAfterUpdateColumnAutoIncrement.columns())
            .filter(column -> column.name().equals("col2"))
            .findFirst()
            .orElseThrow();
    Assertions.assertTrue(updatedAutoIncrementCol2.autoIncrement());
  }

  private void assertTableEquals(Table expected, Table actual) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.comment(), actual.comment());
    Assertions.assertArrayEquals(expected.columns(), actual.columns());
    Assertions.assertArrayEquals(expected.partitioning(), actual.partitioning());
    Assertions.assertArrayEquals(expected.sortOrder(), actual.sortOrder());
    Assertions.assertEquals(expected.distribution(), actual.distribution());
    Assertions.assertArrayEquals(expected.index(), actual.index());
  }
}
