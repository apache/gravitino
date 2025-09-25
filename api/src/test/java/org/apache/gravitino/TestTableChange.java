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
package org.apache.gravitino;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.TableChange.AddColumn;
import org.apache.gravitino.rel.TableChange.ColumnPosition;
import org.apache.gravitino.rel.TableChange.DeleteColumn;
import org.apache.gravitino.rel.TableChange.RemoveProperty;
import org.apache.gravitino.rel.TableChange.RenameColumn;
import org.apache.gravitino.rel.TableChange.RenameTable;
import org.apache.gravitino.rel.TableChange.SetProperty;
import org.apache.gravitino.rel.TableChange.UpdateColumnComment;
import org.apache.gravitino.rel.TableChange.UpdateColumnDefaultValue;
import org.apache.gravitino.rel.TableChange.UpdateColumnPosition;
import org.apache.gravitino.rel.TableChange.UpdateColumnType;
import org.apache.gravitino.rel.TableChange.UpdateComment;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableChange {

  @Test
  public void testRenameTable() {
    String newName = "New table name";
    RenameTable renameTable = (RenameTable) TableChange.rename(newName);

    assertEquals(newName, renameTable.getNewName());
  }

  @Test
  public void testUpdateComment() {
    String newComment = "New comment";
    UpdateComment updateComment = (UpdateComment) TableChange.updateComment(newComment);

    assertEquals(newComment, updateComment.getNewComment());
  }

  @Test
  public void testSetProperty() {
    String property = "Jam";
    String value = "Marmalade";
    SetProperty setProperty = (SetProperty) TableChange.setProperty(property, value);

    assertEquals(property, setProperty.getProperty());
    assertEquals(value, setProperty.getValue());
  }

  @Test
  public void testColumnPosition() {
    String column = "Name";
    ColumnPosition first = TableChange.ColumnPosition.first();
    ColumnPosition afterColumn = TableChange.ColumnPosition.after(column);

    assertEquals(first, TableChange.ColumnPosition.first());
    assertEquals(column, ((TableChange.After) afterColumn).getColumn());
  }

  @Test
  public void testAddColumn() {
    String[] fieldName = {"Name"};
    Type dataType = Types.StringType.get();
    String comment = "Person name";
    AddColumn addColumn = (AddColumn) TableChange.addColumn(fieldName, dataType, comment);

    assertArrayEquals(fieldName, addColumn.fieldName());
    assertEquals(dataType, addColumn.getDataType());
    assertEquals(comment, addColumn.getComment());
    assertEquals(ColumnPosition.defaultPos(), addColumn.getPosition());
    assertEquals(Column.DEFAULT_VALUE_NOT_SET, addColumn.getDefaultValue());
  }

  @Test
  public void testAddColumnWithPosition() {
    String[] fieldName = {"Full Name", "First Name"};
    Type dataType = Types.StringType.get();
    String comment = "First or given name";
    TableChange.ColumnPosition position = TableChange.ColumnPosition.after("Address");
    AddColumn addColumn = (AddColumn) TableChange.addColumn(fieldName, dataType, comment, position);

    assertArrayEquals(fieldName, addColumn.fieldName());
    assertEquals(dataType, addColumn.getDataType());
    assertEquals(comment, addColumn.getComment());
    assertEquals(position, addColumn.getPosition());
    assertEquals(Column.DEFAULT_VALUE_NOT_SET, addColumn.getDefaultValue());
  }

  @Test
  public void testAddColumnWithNullCommentAndPosition() {
    String[] fieldName = {"Middle Name"};
    Type dataType = Types.StringType.get();
    AddColumn addColumn =
        (AddColumn) TableChange.addColumn(fieldName, dataType, null, (ColumnPosition) null);

    assertArrayEquals(fieldName, addColumn.fieldName());
    assertEquals(dataType, addColumn.getDataType());
    assertNull(addColumn.getComment());
    assertEquals(ColumnPosition.defaultPos(), addColumn.getPosition());
    assertEquals(Column.DEFAULT_VALUE_NOT_SET, addColumn.getDefaultValue());
  }

  @Test
  public void testRenameColumn() {
    String[] fieldName = {"Last Name"};
    String newName = "Family Name";
    RenameColumn renameColumn = (RenameColumn) TableChange.renameColumn(fieldName, newName);

    assertArrayEquals(fieldName, renameColumn.fieldName());
    assertEquals(newName, renameColumn.getNewName());
  }

  @Test
  public void testRenameNestedColumn() {
    String[] fieldName = {"Name", "First Name"};
    String newName = "Name.first";
    RenameColumn renameColumn = (RenameColumn) TableChange.renameColumn(fieldName, newName);

    assertArrayEquals(fieldName, renameColumn.fieldName());
    assertEquals(newName, renameColumn.getNewName());
  }

  @Test
  public void testUpdateColumnPosition() {
    String[] fieldName = {"First Name"};
    ColumnPosition newPosition = TableChange.ColumnPosition.first();
    UpdateColumnPosition updateColumnPosition =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fieldName, newPosition);

    assertArrayEquals(fieldName, updateColumnPosition.fieldName());
    assertEquals(newPosition, updateColumnPosition.getPosition());
  }

  @Test
  public void testUpdateNestedColumnPosition() {
    String[] fieldName = {"Name", "Last Name"};
    ColumnPosition newPosition = TableChange.ColumnPosition.after("First Name");
    UpdateColumnPosition updateColumnPosition =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fieldName, newPosition);

    assertArrayEquals(fieldName, updateColumnPosition.fieldName());
    assertEquals(newPosition, updateColumnPosition.getPosition());
  }

  @Test
  public void testDoesNotAllowDefaultInColumnPosition() {
    String[] fieldName = {"Name", "Last Name"};
    ColumnPosition newPosition = TableChange.ColumnPosition.defaultPos();
    Exception exception =
        Assertions.assertThrowsExactly(
            IllegalArgumentException.class,
            () -> TableChange.updateColumnPosition(fieldName, newPosition));
    assertEquals("Position cannot be DEFAULT", exception.getMessage());
  }

  @Test
  public void testUpdateColumnComment() {
    String[] fieldName = {"First Name"};
    String newComment = "First or given name";
    UpdateColumnComment updateColumnComment =
        (UpdateColumnComment) TableChange.updateColumnComment(fieldName, newComment);

    assertArrayEquals(fieldName, updateColumnComment.fieldName());
    assertEquals(newComment, updateColumnComment.getNewComment());
  }

  @Test
  public void testUpdateNestedColumnComment() {
    String[] fieldName = {"Name", "Last Name"};
    String newComment = "Last or family name";
    UpdateColumnComment updateColumnComment =
        (UpdateColumnComment) TableChange.updateColumnComment(fieldName, newComment);

    assertArrayEquals(fieldName, updateColumnComment.fieldName());
    assertEquals(newComment, updateColumnComment.getNewComment());
  }

  @Test
  public void testDeleteColumn() {
    String[] fieldName = {"existing_column"};
    Boolean ifExists = true;
    DeleteColumn deleteColumn = (DeleteColumn) TableChange.deleteColumn(fieldName, ifExists);

    assertArrayEquals(fieldName, deleteColumn.fieldName());
    assertEquals(ifExists, deleteColumn.getIfExists());
  }

  @Test
  public void testDeleteNestedColumn() {
    String[] fieldName = {"nested", "existing_column"};
    Boolean ifExists = false;
    DeleteColumn deleteColumn = (DeleteColumn) TableChange.deleteColumn(fieldName, ifExists);

    assertArrayEquals(fieldName, deleteColumn.fieldName());
    assertEquals(ifExists, deleteColumn.getIfExists());
  }

  @Test
  public void testUpdateColumnNullability() {
    String[] fieldName = {"existing_column"};
    TableChange.UpdateColumnNullability updateColumnType =
        (TableChange.UpdateColumnNullability) TableChange.updateColumnNullability(fieldName, false);

    assertArrayEquals(fieldName, updateColumnType.fieldName());
    assertFalse(updateColumnType.nullable());
  }

  @Test
  public void testUpdateColumnDefaultValue() {
    String[] fieldName = {"existing_column"};
    Expression newDefaultValue = Literals.of("Default Value", Types.VarCharType.of(255));
    UpdateColumnDefaultValue updateColumnDefaultValue =
        (UpdateColumnDefaultValue) TableChange.updateColumnDefaultValue(fieldName, newDefaultValue);

    assertArrayEquals(fieldName, updateColumnDefaultValue.fieldName());
    assertEquals(newDefaultValue, updateColumnDefaultValue.getNewDefaultValue());
  }

  @Test
  public void testUpdateNestedColumnDefaultValue() {
    String[] fieldName = {"nested", "existing_column"};
    Expression newDefaultValue = Literals.of("Default Value", Types.VarCharType.of(255));
    UpdateColumnDefaultValue updateColumnType =
        (UpdateColumnDefaultValue) TableChange.updateColumnDefaultValue(fieldName, newDefaultValue);

    assertArrayEquals(fieldName, updateColumnType.fieldName());
    assertEquals(newDefaultValue, updateColumnType.getNewDefaultValue());
  }

  @Test
  public void testUpdateColumnType() {
    String[] fieldName = {"existing_column"};
    Type dataType = Types.StringType.get();
    UpdateColumnType updateColumnType =
        (UpdateColumnType) TableChange.updateColumnType(fieldName, dataType);

    assertArrayEquals(fieldName, updateColumnType.fieldName());
    assertEquals(dataType, updateColumnType.getNewDataType());
  }

  @Test
  public void testUpdateNestedColumnType() {
    String[] fieldName = {"nested", "existing_column"};
    Type dataType = Types.StringType.get();
    UpdateColumnType updateColumnType =
        (UpdateColumnType) TableChange.updateColumnType(fieldName, dataType);

    assertArrayEquals(fieldName, updateColumnType.fieldName());
    assertEquals(dataType, updateColumnType.getNewDataType());
  }

  @Test
  void testRemoveProperty() {
    String property = "Null values";
    RemoveProperty change = (RemoveProperty) TableChange.removeProperty(property);

    assertEquals(property, change.getProperty());
  }

  @Test
  void testRenameEqualsAndHashCode() {
    String nameA = "Table name";
    RenameTable change1 = (RenameTable) TableChange.rename(nameA);
    String nameB = "Table name";
    RenameTable change2 = (RenameTable) TableChange.rename(nameB);

    assertTrue(change1.equals(change2));
    assertTrue(change2.equals(change1));
    assertEquals(change1.hashCode(), change2.hashCode());
  }

  @Test
  void testRenameNotEqualsAndHashCode() {
    String nameA = "Table name";
    RenameTable change1 = (RenameTable) TableChange.rename(nameA);
    String nameB = "New table name";
    RenameTable change2 = (RenameTable) TableChange.rename(nameB);

    assertFalse(change1.equals(null));
    assertFalse(change1.equals(change2));
    assertFalse(change2.equals(change1));
    assertNotEquals(change1.hashCode(), change2.hashCode());

    String schemaNameA = "Schema A";
    RenameTable change3 = (RenameTable) TableChange.rename(nameA, schemaNameA);
    RenameTable change4 = (RenameTable) TableChange.rename(nameB, schemaNameA);

    assertNotEquals(change3, change4);
  }

  @Test
  void testUpdateEqualsAndHashCode() {
    String commentA = "a comment";
    UpdateComment update1 = (UpdateComment) TableChange.updateComment(commentA);
    String commentB = "a comment";
    UpdateComment update2 = (UpdateComment) TableChange.updateComment(commentB);

    assertTrue(update1.equals(update2));
    assertTrue(update2.equals(update1));
    assertEquals(update1.hashCode(), update2.hashCode());
  }

  @Test
  void testUpdateNotEqualsAndHashCode() {
    String commentA = "a comment";
    UpdateComment update1 = (UpdateComment) TableChange.updateComment(commentA);
    String commentB = "a new comment";
    UpdateComment update2 = (UpdateComment) TableChange.updateComment(commentB);

    assertFalse(update1.equals(null));
    assertFalse(update1.equals(update2));
    assertFalse(update2.equals(update1));
    assertNotEquals(update1.hashCode(), update2.hashCode());
  }

  @Test
  void testColumnPositionEqualsAndHashCode() {
    ColumnPosition positionA = TableChange.ColumnPosition.first();
    ColumnPosition positionB = TableChange.ColumnPosition.first();

    assertTrue(positionA.equals(positionB));
    assertTrue(positionB.equals(positionA));
    assertEquals(positionA.hashCode(), positionB.hashCode());
  }

  @Test
  void testColumnPositionNotEqualsAndHashCode() {
    ColumnPosition positionA = TableChange.ColumnPosition.first();
    String column = "Name";
    ColumnPosition positionB = TableChange.ColumnPosition.after(column);

    assertFalse(positionA.equals(null));
    assertFalse(positionA.equals(positionB));
    assertFalse(positionB.equals(positionA));
    assertNotEquals(positionA.hashCode(), positionB.hashCode());
  }

  @Test
  void testColumnDeleteEqualsAndHashCode() {
    String[] nameA = {"Column A"};
    Boolean ifExists = true;
    DeleteColumn columnA = (DeleteColumn) TableChange.deleteColumn(nameA, ifExists);
    String[] nameB = {"Column A"};
    DeleteColumn columnB = (DeleteColumn) TableChange.deleteColumn(nameB, ifExists);

    assertTrue(columnA.equals(columnB));
    assertTrue(columnB.equals(columnA));
    assertEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  void testColumnDeleteNotEqualsAndHashCode() {
    String[] nameA = {"Column A"};
    Boolean ifExists = true;
    DeleteColumn columnA = (DeleteColumn) TableChange.deleteColumn(nameA, ifExists);
    String[] nameB = {"Column B"};
    DeleteColumn columnB = (DeleteColumn) TableChange.deleteColumn(nameB, ifExists);

    assertFalse(columnA.equals(null));
    assertFalse(columnA.equals(columnB));
    assertFalse(columnB.equals(columnA));
    assertNotEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  void testColumnRenameEqualsAndHashCode() {
    String[] nameA = {"Last Name"};
    String newName = "Family Name";
    RenameColumn columnA = (RenameColumn) TableChange.renameColumn(nameA, newName);
    String[] nameB = {"Last Name"};
    RenameColumn columnB = (RenameColumn) TableChange.renameColumn(nameB, newName);

    assertTrue(columnA.equals(columnB));
    assertTrue(columnB.equals(columnA));
    assertEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  void testColumnRenameNotEqualsAndHashCode() {
    String[] nameA = {"Last Name"};
    String newName = "Family Name";
    RenameColumn columnA = (RenameColumn) TableChange.renameColumn(nameA, newName);
    String[] nameB = {"Family Name"};
    RenameColumn columnB = (RenameColumn) TableChange.renameColumn(nameB, newName);

    assertFalse(columnA.equals(null));
    assertFalse(columnA.equals(columnB));
    assertFalse(columnB.equals(columnA));
    assertNotEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  void testUpdateColumnDefaultValueEqualsAndHashCode() {
    String[] nameA = {"Column Name"};
    Expression newDefaultValueA = Literals.of("Default Value", Types.VarCharType.of(255));
    UpdateColumnDefaultValue columnA =
        (UpdateColumnDefaultValue) TableChange.updateColumnDefaultValue(nameA, newDefaultValueA);
    String[] nameB = {"Column Name"};
    Expression newDefaultValueB = Literals.of("Default Value", Types.VarCharType.of(255));
    UpdateColumnDefaultValue columnB =
        (UpdateColumnDefaultValue) TableChange.updateColumnDefaultValue(nameB, newDefaultValueB);

    assertTrue(columnA.equals(columnB));
    assertTrue(columnB.equals(columnA));
    assertEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  void testUpdateColumnDefaultValueNotEqualsAndHashCode() {
    String[] nameA = {"Column Name A"};
    Expression newDefaultValueA = Literals.of("New Default Value A", Types.VarCharType.of(255));
    UpdateColumnDefaultValue columnA =
        (UpdateColumnDefaultValue) TableChange.updateColumnDefaultValue(nameA, newDefaultValueA);
    String[] nameB = {"Column Name B"};
    Expression newDefaultValueB = Literals.of("New Default Value B", Types.VarCharType.of(255));
    UpdateColumnDefaultValue columnB =
        (UpdateColumnDefaultValue) TableChange.updateColumnDefaultValue(nameB, newDefaultValueB);

    assertFalse(columnA.equals(null));
    assertFalse(columnA.equals(columnB));
    assertFalse(columnB.equals(columnA));
    assertNotEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  void testColumnUpdateTypeEqualsAndHashCode() {
    String[] nameA = {"First Name"};
    Type dataType = Types.StringType.get();
    UpdateColumnType columnA = (UpdateColumnType) TableChange.updateColumnType(nameA, dataType);
    String[] nameB = {"First Name"};
    UpdateColumnType columnB = (UpdateColumnType) TableChange.updateColumnType(nameB, dataType);

    assertTrue(columnA.equals(columnB));
    assertTrue(columnB.equals(columnA));
    assertEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  void testColumnUpdateTypeNotEqualsAndHashCode() {
    String[] nameA = {"First Name"};
    Type dataType = Types.StringType.get();
    UpdateColumnType columnA = (UpdateColumnType) TableChange.updateColumnType(nameA, dataType);
    String[] nameB = {"Given Name"};
    UpdateColumnType columnB = (UpdateColumnType) TableChange.updateColumnType(nameB, dataType);

    assertFalse(columnA.equals(null));
    assertFalse(columnA.equals(columnB));
    assertFalse(columnB.equals(columnA));
    assertNotEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  void testColumnUpdateCommentEqualsAndHashCode() {
    String[] nameA = {"First Name"};
    String commentA = "First or given name";
    UpdateColumnComment columnA =
        (UpdateColumnComment) TableChange.updateColumnComment(nameA, commentA);
    String[] nameB = {"First Name"};
    String commentB = "First or given name";
    UpdateColumnComment columnB =
        (UpdateColumnComment) TableChange.updateColumnComment(nameB, commentB);

    assertTrue(columnA.equals(columnB));
    assertTrue(columnB.equals(columnA));
    assertEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  void testColumnUpdateCommentNotEqualsAndHashCode() {
    String[] nameA = {"First Name"};
    String commentA = "First or given name";
    UpdateColumnComment columnA =
        (UpdateColumnComment) TableChange.updateColumnComment(nameA, commentA);
    String[] nameB = {"Given Name"};
    String commentB = "A persons first or given name";
    UpdateColumnComment columnB =
        (UpdateColumnComment) TableChange.updateColumnComment(nameB, commentB);

    assertFalse(columnA.equals(null));
    assertFalse(columnA.equals(columnB));
    assertFalse(columnB.equals(columnA));
    assertNotEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  void testRemovePropertyEqualsAndHashCode() {
    String propertyA = "property A";
    RemoveProperty changeA = (RemoveProperty) TableChange.removeProperty(propertyA);
    String propertyB = "property A";
    RemoveProperty changeB = (RemoveProperty) TableChange.removeProperty(propertyB);

    assertTrue(changeA.equals(changeB));
    assertTrue(changeB.equals(changeA));
    assertEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testRemovePropertyNotEqualsAndHashCode() {
    String propertyA = "property A";
    RemoveProperty changeA = (RemoveProperty) TableChange.removeProperty(propertyA);
    String propertyB = "property B";
    RemoveProperty changeB = (RemoveProperty) TableChange.removeProperty(propertyB);

    assertFalse(changeA.equals(null));
    assertFalse(changeA.equals(changeB));
    assertFalse(changeB.equals(changeA));
    assertNotEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testUpdateColumnPositionEqualsAndHashCode() {
    String[] fieldNameA = {"First Name"};
    ColumnPosition newPosition = TableChange.ColumnPosition.first();
    UpdateColumnPosition changeA =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fieldNameA, newPosition);
    String[] fieldNameB = {"First Name"};
    UpdateColumnPosition changeB =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fieldNameB, newPosition);

    assertTrue(changeA.equals(changeB));
    assertTrue(changeB.equals(changeA));
    assertEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testUpdateColumnPositionNotEqualsAndHashCode() {
    String[] fieldNameA = {"First Name"};
    ColumnPosition newPosition = TableChange.ColumnPosition.first();
    UpdateColumnPosition changeA =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fieldNameA, newPosition);
    String[] fieldNameB = {"Last Name"};
    UpdateColumnPosition changeB =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fieldNameB, newPosition);

    assertFalse(changeA.equals(null));
    assertFalse(changeA.equals(changeB));
    assertFalse(changeB.equals(changeA));
    assertNotEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testSetPropertyEqualsAndHashCode() {
    String propertyA = "property A";
    String valueA = "A";
    SetProperty changeA = (SetProperty) TableChange.setProperty(propertyA, valueA);
    String propertyB = "property A";
    String valueB = "A";
    SetProperty changeB = (SetProperty) TableChange.setProperty(propertyB, valueB);

    assertTrue(changeA.equals(changeB));
    assertTrue(changeB.equals(changeA));
    assertEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testSetPropertyNotEqualsAndHashCode() {
    String propertyA = "property A";
    String valueA = "A";
    SetProperty changeA = (SetProperty) TableChange.setProperty(propertyA, valueA);
    String propertyB = "property B";
    String valueB = "B";
    SetProperty changeB = (SetProperty) TableChange.setProperty(propertyB, valueB);

    assertFalse(changeA.equals(null));
    assertFalse(changeA.equals(changeB));
    assertFalse(changeB.equals(changeA));
    assertNotEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testAddColumnEqualsAndHashCode() {
    String[] fieldNameA = {"Name"};
    Type dataTypeA = Types.StringType.get();
    String commentA = "Person name";
    AddColumn columnA = (AddColumn) TableChange.addColumn(fieldNameA, dataTypeA, commentA);
    String[] fieldNameB = {"Name"};
    Type dataTypeB = Types.StringType.get();
    String commentB = "Person name";
    AddColumn columnB = (AddColumn) TableChange.addColumn(fieldNameB, dataTypeB, commentB);

    assertTrue(columnA.equals(columnB));
    assertTrue(columnB.equals(columnA));
    assertEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  void testAddColumnNotEqualsAndHashCode() {
    String[] fieldNameA = {"Name"};
    Type dataTypeA = Types.StringType.get();
    String commentA = "Person name";
    AddColumn columnA = (AddColumn) TableChange.addColumn(fieldNameA, dataTypeA, commentA);
    String[] fieldNameB = {"First Name"};
    Type dataTypeB = Types.StringType.get();
    String commentB = "Person name";
    AddColumn columnB = (AddColumn) TableChange.addColumn(fieldNameB, dataTypeB, commentB);

    assertFalse(columnA.equals(null));
    assertFalse(columnA.equals(columnB));
    assertFalse(columnB.equals(columnA));
    assertNotEquals(columnA.hashCode(), columnB.hashCode());
  }
}
