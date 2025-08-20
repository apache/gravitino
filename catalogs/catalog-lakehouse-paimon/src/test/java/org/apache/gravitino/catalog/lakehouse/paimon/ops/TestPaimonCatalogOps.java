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
package org.apache.gravitino.catalog.lakehouse.paimon.ops;

import static org.apache.gravitino.catalog.lakehouse.paimon.utils.TableOpsUtils.getFieldName;
import static org.apache.gravitino.catalog.lakehouse.paimon.utils.TypeUtils.toPaimonType;
import static org.apache.gravitino.rel.TableChange.ColumnPosition.after;
import static org.apache.gravitino.rel.TableChange.ColumnPosition.defaultPos;
import static org.apache.gravitino.rel.TableChange.ColumnPosition.first;
import static org.apache.gravitino.rel.TableChange.addColumn;
import static org.apache.gravitino.rel.TableChange.deleteColumn;
import static org.apache.gravitino.rel.TableChange.removeProperty;
import static org.apache.gravitino.rel.TableChange.renameColumn;
import static org.apache.gravitino.rel.TableChange.setProperty;
import static org.apache.gravitino.rel.TableChange.updateColumnComment;
import static org.apache.gravitino.rel.TableChange.updateColumnNullability;
import static org.apache.gravitino.rel.TableChange.updateColumnPosition;
import static org.apache.gravitino.rel.TableChange.updateColumnType;
import static org.apache.gravitino.rel.TableChange.updateComment;
import static org.apache.paimon.CoreOptions.BUCKET;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.TableChange.ColumnPosition;
import org.apache.gravitino.rel.TableChange.UpdateColumnComment;
import org.apache.gravitino.rel.TableChange.UpdateComment;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.ColumnAlreadyExistException;
import org.apache.paimon.catalog.Catalog.ColumnNotExistException;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.apache.paimon.catalog.Catalog.TableAlreadyExistException;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaChange.AddColumn;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link org.apache.gravitino.catalog.lakehouse.paimon.ops.PaimonCatalogOps}. */
public class TestPaimonCatalogOps {

  private PaimonCatalogOps paimonCatalogOps;
  @TempDir private File warehouse;

  private static final String DATABASE = "test_table_ops_database";
  private static final String TABLE = "test_table_ops_table";
  private static final String COMMENT = "table_ops_table_comment";
  private static final NameIdentifier IDENTIFIER = NameIdentifier.of(Namespace.of(DATABASE), TABLE);
  private static final Map<String, String> OPTIONS = ImmutableMap.of(BUCKET.key(), "10");

  @BeforeEach
  public void setUp() throws Exception {
    paimonCatalogOps =
        new PaimonCatalogOps(
            new PaimonConfig(
                ImmutableMap.of(PaimonCatalogPropertiesMetadata.WAREHOUSE, warehouse.getPath())));
    createDatabase();
    createTable();
  }

  @AfterEach
  public void tearDown() throws Exception {
    dropDatabase();
    if (paimonCatalogOps != null) {
      paimonCatalogOps.close();
    }
  }

  @Test
  void testLoadListAndDropTableOperations() throws Exception {
    // list tables
    Assertions.assertEquals(
        1, paimonCatalogOps.listTables(IDENTIFIER.namespace().toString()).size());

    // load table
    Table table = paimonCatalogOps.loadTable(IDENTIFIER.toString());

    assertEquals(TABLE, table.name());
    assertTrue(table.comment().isPresent());
    assertEquals(
        RowType.builder()
            .field("col_1", DataTypes.INT().notNull(), IntType.class.getSimpleName())
            .field("col_2", DataTypes.STRING(), VarCharType.class.getSimpleName())
            .field("col_3", DataTypes.STRING().notNull(), VarCharType.class.getSimpleName())
            .field(
                "col_4",
                DataTypes.ARRAY(
                    RowType.builder()
                        .field(
                            "sub_col_1",
                            DataTypes.DATE(),
                            RowType.class.getSimpleName() + DateType.class.getSimpleName())
                        .field(
                            "sub_col_2",
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                            RowType.class.getSimpleName() + MapType.class.getSimpleName())
                        .field(
                            "sub_col_3",
                            DataTypes.TIMESTAMP().notNull(),
                            RowType.class.getSimpleName() + TimestampType.class.getSimpleName())
                        .build()),
                ArrayType.class.getSimpleName())
            .build()
            .toString(),
        table.rowType().toString());
    assertEquals(COMMENT, table.comment().get());
    assertEquals(OPTIONS.get(BUCKET.key()), table.options().get(BUCKET.key()));

    // drop table
    Assertions.assertDoesNotThrow(() -> paimonCatalogOps.purgeTable(IDENTIFIER.toString()));
    Assertions.assertThrowsExactly(
        Catalog.TableNotExistException.class,
        () -> paimonCatalogOps.purgeTable(IDENTIFIER.toString()));

    // list table again
    Assertions.assertEquals(
        0, paimonCatalogOps.listTables(IDENTIFIER.namespace().toString()).size());

    // create a new table to make database not empty to test drop database cascade
    createTable();
    Assertions.assertNotNull(paimonCatalogOps.loadTable(IDENTIFIER.toString()));
  }

  @Test
  void testAddColumn() throws Exception {
    // Test AddColumn after column.
    assertAddColumn(5, Types.BooleanType.get(), after("col_2"), 2);
    // Test AddColumn first column.
    assertAddColumn(6, Types.FloatType.get(), first(), 0);
    // Test AddColumn last column.
    assertAddColumn(7, Types.DoubleType.get(), defaultPos(), 6);
    assertAddColumn(8, Types.DoubleType.get(), null, 7);
    // Test NullPointerException with AddColumn for after non-existent column.
    assertThrowsExactly(
        NullPointerException.class,
        () -> assertAddColumn(9, Types.LongType.get(), after("col_10"), null));
  }

  @Test
  void testUpdateColumnComment() throws Exception {
    assertAlterTable(
        table -> {
          DataField dataField = table.rowType().getFields().get(0);
          assertEquals("col_1", dataField.name());
          assertEquals(UpdateColumnComment.class.getSimpleName(), dataField.description());
        },
        updateColumnComment(getFieldName("col_1"), UpdateColumnComment.class.getSimpleName()));
    assertColumnNotExist(
        updateColumnComment(getFieldName("col_10"), UpdateComment.class.getSimpleName()));
  }

  @Test
  void testUpdateColumnNullability() throws Exception {
    assertAlterTable(
        table -> {
          DataField dataField = table.rowType().getFields().get(1);
          assertEquals("col_2", dataField.name());
          assertFalse(dataField.type().isNullable());
        },
        updateColumnNullability(getFieldName("col_2"), false));
    assertColumnNotExist(updateColumnNullability(getFieldName("col_5"), true));
  }

  @Test
  void testUpdateColumnPosition() throws Exception {
    // Test UpdateColumnPosition after column.
    assertUpdateColumnPosition(3, after("col_1"), 0, 2, 1, 3);
    // Test UpdateColumnPosition first column.
    assertUpdateColumnPosition(4, first(), 1, 3, 2, 0);
    // Test NullPointerException with UpdateColumnPosition for non-existent column.
    assertThrowsExactly(
        IllegalArgumentException.class, () -> assertUpdateColumnPosition(5, defaultPos()));
    // Test NullPointerException with UpdateColumnPosition for after non-existent column.
    assertThrowsExactly(
        IllegalArgumentException.class, () -> assertUpdateColumnPosition(1, after("col_5")));
  }

  @Test
  void testUpdateColumnType() throws Exception {
    assertAlterTable(
        table -> {
          DataField dataField = table.rowType().getFields().get(0);
          assertEquals("col_1", dataField.name());
          assertEquals(DataTypes.BIGINT().notNull(), dataField.type());
        },
        updateColumnType(getFieldName("col_1"), Types.LongType.get()));
    assertColumnNotExist(updateColumnType(getFieldName("col_5"), Types.ShortType.get()));
    assertThrowsExactly(
        IllegalStateException.class,
        () ->
            assertAlterTable(
                table -> {}, updateColumnType(getFieldName("col_1"), Types.DateType.get())));
    assertThrowsExactly(
        IllegalStateException.class,
        () ->
            assertAlterTable(
                table -> {}, updateColumnType(getFieldName("col_4"), Types.LongType.get())));
  }

  @Test
  void testRenameColumn() throws Exception {
    assertAlterTable(
        table -> {
          List<String> fieldNames = table.rowType().getFieldNames();
          assertFalse(fieldNames.contains("col2"));
          assertEquals("col_5", fieldNames.get(1));
          assertEquals(4, fieldNames.size());
        },
        renameColumn(getFieldName("col_2"), "col_5"));
    assertColumnNotExist(renameColumn(getFieldName("col_6"), "col_7"));
    assertThrowsExactly(
        ColumnAlreadyExistException.class,
        () -> assertAlterTable(table -> {}, renameColumn(getFieldName("col_1"), "col_4")));
  }

  @Test
  void testDeleteColumn() throws Exception {
    assertAlterTable(
        table -> {
          List<String> fieldNames = table.rowType().getFieldNames();
          assertFalse(fieldNames.contains("col_2"));
          assertEquals("col_3", fieldNames.get(1));
          assertEquals(3, fieldNames.size());
        },
        deleteColumn(getFieldName("col_2"), true));
    assertColumnNotExist(deleteColumn(getFieldName("col_5"), true));
    assertColumnNotExist(deleteColumn(getFieldName("col_5"), false));
  }

  @Test
  void testUpdateComment() throws Exception {
    assertAlterTable(
        table -> {
          assertTrue(table.comment().isPresent());
          assertEquals(UpdateComment.class.getSimpleName(), table.comment().get());
        },
        updateComment(UpdateComment.class.getSimpleName()));
  }

  @Test
  void testSetAndRemoveProperty() throws Exception {
    String propertyKey = "test_property_key_1";
    assertFalse(
        paimonCatalogOps.loadTable(IDENTIFIER.toString()).options().containsKey(propertyKey));
    // Test SetProperty with non-existent property.
    String propertyValue = "test_property_value_1";
    assertAlterTable(
        table -> {
          Map<String, String> options = table.options();
          assertTrue(options.containsKey(propertyKey));
          assertEquals(propertyValue, options.get(propertyKey));
        },
        setProperty(propertyKey, propertyValue));
    // Test SetProperty with overwrite existing property.
    String newPropertyValue = "test_property_value_2";
    assertAlterTable(
        table -> {
          Map<String, String> options = table.options();
          assertTrue(options.containsKey(propertyKey));
          assertEquals(newPropertyValue, options.get(propertyKey));
        },
        setProperty(propertyKey, newPropertyValue));
    // Test RemoveProperty with existing property.
    assertAlterTable(
        table -> assertFalse(table.options().containsKey(propertyKey)),
        removeProperty(propertyKey));
    // Test RemoveProperty with non-existent property.
    assertDoesNotThrow(() -> assertAlterTable(table -> {}, removeProperty(propertyKey)));
  }

  @Test
  void testMultipleAlterTable() throws Exception {
    assertAlterTable(
        table -> {
          List<String> fieldNames = table.rowType().getFieldNames();
          assertEquals("col_5", fieldNames.get(0));
          assertFalse(fieldNames.contains("col_2"));
          assertEquals(3, fieldNames.size());
          Map<String, String> options = table.options();
          assertTrue(options.containsKey("test_property_key"));
          assertEquals("test_property_value", options.get("test_property_key"));
        },
        renameColumn(getFieldName("col_1"), "col_5"),
        deleteColumn(getFieldName("col_2"), true),
        setProperty("test_property_key", "test_property_value"));
  }

  private void assertUpdateColumnPosition(int column, ColumnPosition columnPosition, int... fields)
      throws Exception {
    String columnName = "col_" + column;
    assertAlterTable(
        table -> {
          List<String> fieldNames = table.rowType().getFieldNames();
          assertEquals("col_1", fieldNames.get(fields[0]));
          assertEquals("col_2", fieldNames.get(fields[1]));
          assertEquals("col_3", fieldNames.get(fields[2]));
          assertEquals("col_4", fieldNames.get(fields[3]));
        },
        updateColumnPosition(getFieldName(columnName), columnPosition));
  }

  private void assertAddColumn(int column, Type type, ColumnPosition columnPosition, Integer field)
      throws Exception {
    String columnName = "col_" + column;
    assertAlterTable(
        table -> {
          DataField dataField = table.rowType().getFields().get(field);
          assertEquals(columnName, dataField.name());
          assertEquals(toPaimonType(type), dataField.type());
          assertEquals(AddColumn.class.getSimpleName(), dataField.description());
          assertTrue(dataField.type().isNullable());
        },
        addColumn(
            getFieldName(columnName),
            type,
            SchemaChange.AddColumn.class.getSimpleName(),
            columnPosition,
            true,
            false));
  }

  private void assertColumnNotExist(TableChange tableChange) {
    assertThrowsExactly(
        ColumnNotExistException.class, () -> assertAlterTable(table -> {}, tableChange));
  }

  private void assertAlterTable(Consumer<Table> consumer, TableChange... tableChanges)
      throws Exception {
    consumer.accept(alterTable(tableChanges));
  }

  private Table alterTable(TableChange... tableChanges)
      throws Catalog.ColumnAlreadyExistException, Catalog.TableNotExistException,
          Catalog.ColumnNotExistException {
    paimonCatalogOps.alterTable(IDENTIFIER.toString(), tableChanges);
    return paimonCatalogOps.loadTable(IDENTIFIER.toString());
  }

  private void createDatabase() throws Exception {
    // list databases
    assertEquals(0, paimonCatalogOps.listDatabases().size());

    // create database
    paimonCatalogOps.createDatabase(DATABASE, Maps.newHashMap());
    assertEquals(1, paimonCatalogOps.listDatabases().size());
    // load database
    assertNotNull(paimonCatalogOps.loadDatabase(DATABASE));
  }

  private void createTable() throws TableAlreadyExistException, DatabaseNotExistException {
    Pair<String, Schema> tableInfo =
        Pair.of(
            IDENTIFIER.toString(),
            Schema.newBuilder()
                .column("col_1", DataTypes.INT().notNull(), IntType.class.getSimpleName())
                .column("col_2", DataTypes.STRING(), VarCharType.class.getSimpleName())
                .column("col_3", DataTypes.STRING().notNull(), VarCharType.class.getSimpleName())
                .column(
                    "col_4",
                    DataTypes.ARRAY(
                        RowType.builder()
                            .field(
                                "sub_col_1",
                                DataTypes.DATE(),
                                RowType.class.getSimpleName() + DateType.class.getSimpleName())
                            .field(
                                "sub_col_2",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                RowType.class.getSimpleName() + MapType.class.getSimpleName())
                            .field(
                                "sub_col_3",
                                DataTypes.TIMESTAMP().notNull(),
                                RowType.class.getSimpleName() + TimestampType.class.getSimpleName())
                            .build()),
                    ArrayType.class.getSimpleName())
                .comment(COMMENT)
                .primaryKey("col_1")
                .option("alter-column-null-to-not-null.disabled", "false")
                .options(OPTIONS)
                .build());
    paimonCatalogOps.createTable(tableInfo.getKey(), tableInfo.getValue());
  }

  private void dropDatabase() throws Exception {
    Assertions.assertEquals(1, paimonCatalogOps.listDatabases().size());
    Assertions.assertEquals(1, paimonCatalogOps.listTables(DATABASE).size());
    paimonCatalogOps.dropDatabase(DATABASE, true);
    Assertions.assertTrue(paimonCatalogOps.listDatabases().isEmpty());
  }
}
