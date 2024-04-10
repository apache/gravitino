/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.paimon.ops;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig.WAREHOUSE;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.TableOpsUtils.fieldName;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.TypeUtils.toPaimonType;
import static com.datastrato.gravitino.rel.TableChange.ColumnPosition.after;
import static com.datastrato.gravitino.rel.TableChange.ColumnPosition.defaultPos;
import static com.datastrato.gravitino.rel.TableChange.ColumnPosition.first;
import static com.datastrato.gravitino.rel.TableChange.addColumn;
import static com.datastrato.gravitino.rel.TableChange.deleteColumn;
import static com.datastrato.gravitino.rel.TableChange.removeProperty;
import static com.datastrato.gravitino.rel.TableChange.renameColumn;
import static com.datastrato.gravitino.rel.TableChange.setProperty;
import static com.datastrato.gravitino.rel.TableChange.updateColumnComment;
import static com.datastrato.gravitino.rel.TableChange.updateColumnNullability;
import static com.datastrato.gravitino.rel.TableChange.updateColumnPosition;
import static com.datastrato.gravitino.rel.TableChange.updateColumnType;
import static com.datastrato.gravitino.rel.TableChange.updateComment;
import static org.apache.paimon.CoreOptions.BUCKET;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.TableChange.ColumnPosition;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnComment;
import com.datastrato.gravitino.rel.TableChange.UpdateComment;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.ColumnAlreadyExistException;
import org.apache.paimon.catalog.Catalog.ColumnNotExistException;
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
import org.apache.paimon.utils.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link PaimonTableOps}. */
public class TestPaimonTableOps {

  private PaimonTableOps paimonTableOps;
  @TempDir private File warehouse;

  private static final String DATABASE = "test_table_ops_database";
  private static final String TABLE = "test_table_ops_table";
  private static final String COMMENT = "table_ops_table_comment";
  private static final NameIdentifier IDENTIFIER = NameIdentifier.of(Namespace.of(DATABASE), TABLE);
  private static final Map<String, String> OPTIONS = ImmutableMap.of(BUCKET.key(), "10");

  @BeforeEach
  public void setUp() throws Exception {
    paimonTableOps =
        new PaimonTableOps(
            new PaimonConfig(ImmutableMap.of(WAREHOUSE.getKey(), warehouse.getPath())));
    createDatabase();
    createTable();
  }

  @AfterEach
  public void tearDown() throws Exception {
    dropTable();
    dropDatabase();
    paimonTableOps.close();
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
  public void testUpdateColumnComment() throws Exception {
    assertAlterTable(
        table -> {
          DataField dataField = table.rowType().getFields().get(0);
          assertEquals("col_1", dataField.name());
          assertEquals(UpdateColumnComment.class.getSimpleName(), dataField.description());
        },
        updateColumnComment(fieldName("col_1"), UpdateColumnComment.class.getSimpleName()));
    assertColumnNotExist(
        updateColumnComment(fieldName("col_5"), UpdateComment.class.getSimpleName()));
  }

  @Test
  public void testUpdateColumnNullability() throws Exception {
    assertAlterTable(
        table -> {
          DataField dataField = table.rowType().getFields().get(1);
          assertEquals("col_2", dataField.name());
          assertFalse(dataField.type().isNullable());
        },
        updateColumnNullability(fieldName("col_2"), false));
    assertColumnNotExist(updateColumnNullability(fieldName("col_5"), true));
  }

  @Test
  public void testUpdateColumnPosition() throws Exception {
    // Test UpdateColumnPosition after column.
    assertUpdateColumnPosition(3, after("col_1"), 0, 2, 1, 3);
    // Test UpdateColumnPosition first column.
    assertUpdateColumnPosition(4, first(), 1, 3, 2, 0);
    // Test NullPointerException with UpdateColumnPosition for non-existent column.
    assertThrowsExactly(
        NullPointerException.class, () -> assertUpdateColumnPosition(5, defaultPos()));
    // Test NullPointerException with UpdateColumnPosition for after non-existent column.
    assertThrowsExactly(
        NullPointerException.class, () -> assertUpdateColumnPosition(1, after("col_5")));
  }

  @Test
  public void testUpdateColumnType() throws Exception {
    assertAlterTable(
        table -> {
          DataField dataField = table.rowType().getFields().get(0);
          assertEquals("col_1", dataField.name());
          assertEquals(DataTypes.BIGINT(), dataField.type());
        },
        updateColumnType(fieldName("col_1"), Types.LongType.get()));
    assertColumnNotExist(updateColumnType(fieldName("col_5"), Types.ShortType.get()));
    assertThrowsExactly(
        IllegalStateException.class,
        () ->
            assertAlterTable(
                emptyConsumer(), updateColumnType(fieldName("col_1"), Types.DateType.get())));
    assertThrowsExactly(
        IllegalStateException.class,
        () ->
            assertAlterTable(
                emptyConsumer(), updateColumnType(fieldName("col_4"), Types.LongType.get())));
  }

  @Test
  public void testRenameColumn() throws Exception {
    assertAlterTable(
        table -> {
          List<String> fieldNames = table.rowType().getFieldNames();
          assertFalse(fieldNames.contains("col2"));
          assertEquals("col_5", fieldNames.get(1));
          assertEquals(4, fieldNames.size());
        },
        renameColumn(fieldName("col_2"), "col_5"));
    assertColumnNotExist(renameColumn(fieldName("col_6"), "col_7"));
    assertThrowsExactly(
        ColumnAlreadyExistException.class,
        () -> assertAlterTable(emptyConsumer(), renameColumn(fieldName("col_1"), "col_4")));
  }

  @Test
  public void testDeleteColumn() throws Exception {
    assertAlterTable(
        table -> {
          List<String> fieldNames = table.rowType().getFieldNames();
          assertFalse(fieldNames.contains("col_2"));
          assertEquals("col_3", fieldNames.get(1));
          assertEquals(3, fieldNames.size());
        },
        deleteColumn(fieldName("col_2"), true));
    assertColumnNotExist(deleteColumn(fieldName("col_5"), true));
    assertColumnNotExist(deleteColumn(fieldName("col_5"), false));
  }

  @Test
  public void testUpdateComment() throws Exception {
    assertAlterTable(
        table -> {
          assertTrue(table.comment().isPresent());
          assertEquals(UpdateComment.class.getSimpleName(), table.comment().get());
        },
        updateComment(UpdateComment.class.getSimpleName()));
  }

  @Test
  public void testSetAndRemoveProperty() throws Exception {
    String propertyKey = "test_property_key_1";
    assertFalse(paimonTableOps.loadTable(IDENTIFIER.toString()).options().containsKey(propertyKey));
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
    assertDoesNotThrow(() -> assertAlterTable(emptyConsumer(), removeProperty(propertyKey)));
  }

  @Test
  public void testMultipleAlterTable() throws Exception {
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
        renameColumn(fieldName("col_1"), "col_5"),
        deleteColumn(fieldName("col_2"), true),
        setProperty("test_property_key", "test_property_value"));
  }

  public void assertUpdateColumnPosition(int column, ColumnPosition columnPosition, int... fields)
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
        updateColumnPosition(fieldName(columnName), columnPosition));
  }

  public void assertAddColumn(int column, Type type, ColumnPosition columnPosition, Integer field)
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
            fieldName(columnName),
            type,
            SchemaChange.AddColumn.class.getSimpleName(),
            columnPosition,
            true,
            false));
  }

  public void assertColumnNotExist(TableChange tableChange) {
    assertThrowsExactly(
        ColumnNotExistException.class, () -> assertAlterTable(emptyConsumer(), tableChange));
  }

  public void assertAlterTable(Consumer<Table> consumer, TableChange... tableChanges)
      throws Exception {
    consumer.accept(alterTable(tableChanges));
  }

  private Consumer<Table> emptyConsumer() {
    return table -> {};
  }

  private void createDatabase() throws Exception {
    paimonTableOps.createDatabase(Pair.of(DATABASE, Maps.newHashMap()));
    assertEquals(1, paimonTableOps.listDatabases().size());
    assertNotNull(paimonTableOps.loadDatabase(DATABASE));
  }

  private void createTable() throws Exception {
    paimonTableOps.createTable(
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
                .options(OPTIONS)
                .build()));
    Table table = paimonTableOps.loadTable(IDENTIFIER.toString());
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
  }

  public Table alterTable(TableChange... tableChanges)
      throws Catalog.ColumnAlreadyExistException, Catalog.TableNotExistException,
          Catalog.ColumnNotExistException {
    paimonTableOps.alterTable(IDENTIFIER.toString(), tableChanges);
    return paimonTableOps.loadTable(IDENTIFIER.toString());
  }

  private void dropDatabase() throws Exception {
    paimonTableOps.dropDatabase(DATABASE, true);
    assertTrue(paimonTableOps.listDatabases().isEmpty());
  }

  private void dropTable() throws Exception {
    paimonTableOps.dropTable(IDENTIFIER.toString());
    assertTrue(paimonTableOps.listTables(DATABASE).isEmpty());
  }
}
