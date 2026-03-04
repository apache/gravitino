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
package org.apache.gravitino.catalog.lakehouse.paimon.utils;

import static org.apache.gravitino.catalog.lakehouse.paimon.utils.TableOpsUtils.buildSchemaChange;
import static org.apache.gravitino.catalog.lakehouse.paimon.utils.TableOpsUtils.getFieldName;
import static org.apache.gravitino.rel.TableChange.ColumnPosition.after;
import static org.apache.gravitino.rel.TableChange.ColumnPosition.defaultPos;
import static org.apache.gravitino.rel.TableChange.addColumn;
import static org.apache.gravitino.rel.TableChange.addIndex;
import static org.apache.gravitino.rel.TableChange.deleteColumn;
import static org.apache.gravitino.rel.TableChange.deleteIndex;
import static org.apache.gravitino.rel.TableChange.removeProperty;
import static org.apache.gravitino.rel.TableChange.rename;
import static org.apache.gravitino.rel.TableChange.renameColumn;
import static org.apache.gravitino.rel.TableChange.setProperty;
import static org.apache.gravitino.rel.TableChange.updateColumnAutoIncrement;
import static org.apache.gravitino.rel.TableChange.updateColumnComment;
import static org.apache.gravitino.rel.TableChange.updateColumnDefaultValue;
import static org.apache.gravitino.rel.TableChange.updateColumnNullability;
import static org.apache.gravitino.rel.TableChange.updateColumnPosition;
import static org.apache.gravitino.rel.TableChange.updateColumnType;
import static org.apache.gravitino.rel.TableChange.updateComment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.indexes.Index.IndexType;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.rel.types.Types.DoubleType;
import org.apache.gravitino.rel.types.Types.FloatType;
import org.apache.gravitino.rel.types.Types.IntegerType;
import org.apache.gravitino.rel.types.Types.ListType;
import org.apache.gravitino.rel.types.Types.MapType;
import org.apache.gravitino.rel.types.Types.StringType;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaChange.AddColumn;
import org.apache.paimon.schema.SchemaChange.DropColumn;
import org.apache.paimon.schema.SchemaChange.Move.MoveType;
import org.apache.paimon.schema.SchemaChange.RemoveOption;
import org.apache.paimon.schema.SchemaChange.RenameColumn;
import org.apache.paimon.schema.SchemaChange.SetOption;
import org.apache.paimon.schema.SchemaChange.UpdateColumnComment;
import org.apache.paimon.schema.SchemaChange.UpdateColumnNullability;
import org.apache.paimon.schema.SchemaChange.UpdateColumnPosition;
import org.apache.paimon.schema.SchemaChange.UpdateColumnType;
import org.apache.paimon.schema.SchemaChange.UpdateComment;
import org.apache.paimon.types.DataTypeRoot;
import org.junit.jupiter.api.Test;

public class TestTableOpsUtils {

  @Test
  void testAddColumnFirst() {
    assertTableChange(
        addColumn(
            getFieldName("col_1"),
            IntegerType.get(),
            AddColumn.class.getSimpleName(),
            TableChange.ColumnPosition.first(),
            false,
            false),
        AddColumn.class,
        schemaChange -> {
          AddColumn addColumn = (AddColumn) schemaChange;
          assertEquals("col_1", addColumn.fieldNames()[0]);
          assertEquals(DataTypeRoot.INTEGER, addColumn.dataType().getTypeRoot());
          assertEquals(AddColumn.class.getSimpleName(), addColumn.description());
          assertNotNull(addColumn.move());
          assertEquals(MoveType.FIRST, addColumn.move().type());
          assertEquals("col_1", addColumn.move().fieldName());
          assertNull(addColumn.move().referenceFieldName());
          assertFalse(addColumn.dataType().isNullable());
        });
  }

  @Test
  void testAddColumnAfter() {
    assertTableChange(
        addColumn(
            getFieldName("col_2"),
            FloatType.get(),
            AddColumn.class.getSimpleName(),
            after("col_1"),
            true,
            false),
        AddColumn.class,
        schemaChange -> {
          AddColumn addColumn = (AddColumn) schemaChange;
          assertEquals("col_2", addColumn.fieldNames()[0]);
          assertEquals(DataTypeRoot.FLOAT, addColumn.dataType().getTypeRoot());
          assertEquals(AddColumn.class.getSimpleName(), addColumn.description());
          assertNotNull(addColumn.move());
          assertEquals(MoveType.AFTER, addColumn.move().type());
          assertEquals("col_2", addColumn.move().fieldName());
          assertEquals("col_1", addColumn.move().referenceFieldName());
          assertTrue(addColumn.dataType().isNullable());
        });
  }

  @Test
  void testAddColumnDefaultPosition() {
    assertTableChange(
        addColumn(
            getFieldName("col_3"),
            ListType.of(StringType.get(), false),
            AddColumn.class.getSimpleName(),
            defaultPos(),
            false,
            false),
        AddColumn.class,
        schemaChange -> {
          AddColumn addColumn = (AddColumn) schemaChange;
          assertEquals("col_3", addColumn.fieldNames()[0]);
          assertEquals(DataTypeRoot.ARRAY, addColumn.dataType().getTypeRoot());
          assertEquals(AddColumn.class.getSimpleName(), addColumn.description());
          assertNull(addColumn.move());
          assertFalse(addColumn.dataType().isNullable());
        });
  }

  @Test
  void testAddColumnWitNullPosition() {
    assertTableChange(
        addColumn(
            getFieldName("col_4"),
            MapType.of(StringType.get(), IntegerType.get(), true),
            AddColumn.class.getSimpleName(),
            null,
            false,
            false),
        AddColumn.class,
        schemaChange -> {
          AddColumn addColumn = (AddColumn) schemaChange;
          assertEquals("col_4", addColumn.fieldNames()[0]);
          assertEquals(DataTypeRoot.MAP, addColumn.dataType().getTypeRoot());
          assertEquals(AddColumn.class.getSimpleName(), addColumn.description());
          assertNull(addColumn.move());
          assertFalse(addColumn.dataType().isNullable());
        });
  }

  @Test
  void testupdateColumnComment() {
    assertTableChange(
        updateColumnComment(getFieldName("col_1"), UpdateColumnComment.class.getSimpleName()),
        UpdateColumnComment.class,
        schemaChange -> {
          UpdateColumnComment updateColumnComment = (UpdateColumnComment) schemaChange;
          assertEquals("col_1", getFieldName(updateColumnComment.fieldNames()));
          assertEquals(
              UpdateColumnComment.class.getSimpleName(), updateColumnComment.newDescription());
        });
  }

  @Test
  void testUpdateColumnNullability() {
    assertTableChange(
        updateColumnNullability(getFieldName("col_2"), false),
        UpdateColumnNullability.class,
        schemaChange -> {
          UpdateColumnNullability updateColumnNullability = (UpdateColumnNullability) schemaChange;
          assertEquals("col_2", getFieldName(updateColumnNullability.fieldNames()));
          assertFalse(updateColumnNullability.newNullability());
        });
  }

  @Test
  void testUpdateColumnType() {
    assertTableChange(
        updateColumnType(getFieldName("col_4"), DoubleType.get()),
        UpdateColumnType.class,
        schemaChange -> {
          UpdateColumnType updateColumnType = (UpdateColumnType) schemaChange;
          assertEquals("col_4", updateColumnType.fieldNames()[0]);
          assertEquals(DataTypeRoot.DOUBLE, updateColumnType.newDataType().getTypeRoot());
        });
  }

  @Test
  void testRenameColumn() {
    assertTableChange(
        renameColumn(getFieldName("col_1"), "col_5"),
        RenameColumn.class,
        schemaChange -> {
          RenameColumn renameColumn = (RenameColumn) schemaChange;
          assertEquals("col_1", renameColumn.fieldNames()[0]);
          assertEquals("col_5", renameColumn.newName());
        });
  }

  @Test
  void testDeleteColumn() {
    assertTableChange(
        deleteColumn(getFieldName("col_2"), true),
        DropColumn.class,
        schemaChange -> {
          DropColumn dropColumn = (DropColumn) schemaChange;
          assertEquals("col_2", dropColumn.fieldNames()[0]);
        });
  }

  @Test
  void testUpdateComment() {
    assertTableChange(
        updateComment(UpdateComment.class.getSimpleName()),
        UpdateComment.class,
        schemaChange -> {
          UpdateComment updateComment = (UpdateComment) schemaChange;
          assertEquals(UpdateComment.class.getSimpleName(), updateComment.comment());
        });
  }

  @Test
  void testSetProperty() {
    assertTableChange(
        setProperty("prop_k1", "prop_v1"),
        SetOption.class,
        schemaChange -> {
          SetOption setOption = (SetOption) schemaChange;
          assertEquals("prop_k1", setOption.key());
          assertEquals("prop_v1", setOption.value());
        });
  }

  @Test
  void testRemoveProperty() {
    assertTableChange(
        removeProperty("prop_k1"),
        RemoveOption.class,
        schemaChange -> {
          RemoveOption removeOption = (RemoveOption) schemaChange;
          assertEquals("prop_k1", removeOption.key());
        });
  }

  @Test
  void testUpdateColumnPosition() {
    assertTableChange(
        updateColumnPosition(getFieldName("col_3"), after("col_1")),
        UpdateColumnPosition.class,
        schemaChange -> {
          UpdateColumnPosition updateColumnPosition = (UpdateColumnPosition) schemaChange;
          assertEquals("col_3", updateColumnPosition.move().fieldName());
          assertEquals("col_1", updateColumnPosition.move().referenceFieldName());
        });
  }

  @Test
  void testUnsupportedTableChanges() {
    // Test UnsupportedOperationException with AddIndex, DeleteIndex, RenameTable,
    // UpdateColumnAutoIncrement, UpdateColumnDefaultValue.
    Arrays.asList(
            addIndex(IndexType.UNIQUE_KEY, "uk", new String[][] {{"col_5"}}),
            deleteIndex("uk", true),
            rename("tb_1"),
            updateColumnAutoIncrement(getFieldName("col_5"), true),
            updateColumnDefaultValue(
                getFieldName("col_5"), Literals.of("default", Types.VarCharType.of(255))))
        .forEach(this::assertUnsupportedTableChange);

    // Test IllegalArgumentException with AddColumn default value and auto increment.
    Arrays.asList(
            Pair.of(
                addColumn(
                    getFieldName("col_1"),
                    IntegerType.get(),
                    AddColumn.class.getSimpleName(),
                    TableChange.ColumnPosition.first(),
                    false,
                    false,
                    Literals.of("default", Types.StringType.get())),
                "Paimon set column default value through table properties instead of column info. Illegal column: col_1."),
            Pair.of(
                addColumn(
                    getFieldName("col_1"),
                    IntegerType.get(),
                    AddColumn.class.getSimpleName(),
                    TableChange.ColumnPosition.first(),
                    false,
                    true),
                "Paimon does not support auto increment column. Illegal column: col_1."))
        .forEach(this::assertIllegalTableChange);
  }

  private void assertTableChange(
      TableChange tableChange, Class<?> expected, Consumer<SchemaChange> consumer) {
    SchemaChange schemaChange = buildSchemaChange(tableChange);
    assertEquals(expected, schemaChange.getClass());
    consumer.accept(schemaChange);
  }

  private void assertUnsupportedTableChange(TableChange tableChange) {
    UnsupportedOperationException exception =
        assertThrowsExactly(
            UnsupportedOperationException.class, () -> buildSchemaChange(tableChange));
    assertEquals(
        String.format(
            "Paimon does not support %s table change.", tableChange.getClass().getSimpleName()),
        exception.getMessage());
  }

  private void assertIllegalTableChange(Pair<TableChange, String> tableChange) {
    IllegalArgumentException exception =
        assertThrowsExactly(
            IllegalArgumentException.class, () -> buildSchemaChange(tableChange.getKey()));
    assertEquals(tableChange.getValue(), exception.getMessage());
  }
}
