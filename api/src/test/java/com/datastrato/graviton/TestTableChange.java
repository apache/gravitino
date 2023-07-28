/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import static org.junit.jupiter.api.Assertions.*;

import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.rel.TableChange.AddColumn;
import com.datastrato.graviton.rel.TableChange.ColumnPosition;
import com.datastrato.graviton.rel.TableChange.DeleteColumn;
import com.datastrato.graviton.rel.TableChange.RemoveProperty;
import com.datastrato.graviton.rel.TableChange.RenameColumn;
import com.datastrato.graviton.rel.TableChange.RenameTable;
import com.datastrato.graviton.rel.TableChange.SetProperty;
import com.datastrato.graviton.rel.TableChange.UpdateColumnComment;
import com.datastrato.graviton.rel.TableChange.UpdateColumnPosition;
import com.datastrato.graviton.rel.TableChange.UpdateColumnType;
import com.datastrato.graviton.rel.TableChange.UpdateComment;
import io.substrait.type.Type;
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
    String[] fieldNames = {"Name"};
    Type dataType = Type.withNullability(false).STRING;
    String comment = "Person name";
    AddColumn addColumn = (AddColumn) TableChange.addColumn(fieldNames, dataType, comment);

    assertArrayEquals(fieldNames, addColumn.fieldNames());
    assertEquals(dataType, addColumn.getDataType());
    assertEquals(comment, addColumn.getComment());
    assertNull(addColumn.getPosition());
  }

  @Test
  public void testAddColumnWithPosition() {
    String[] fieldNames = {"Full Name", "First Name"};
    Type dataType = Type.withNullability(false).STRING;
    String comment = "First or given name";
    TableChange.ColumnPosition position = TableChange.ColumnPosition.after("Address");
    AddColumn addColumn =
        (AddColumn) TableChange.addColumn(fieldNames, dataType, comment, position);

    assertArrayEquals(fieldNames, addColumn.fieldNames());
    assertEquals(dataType, addColumn.getDataType());
    assertEquals(comment, addColumn.getComment());
    assertEquals(position, addColumn.getPosition());
  }

  @Test
  public void testAddColumnWithNullCommentAndPosition() {
    String[] fieldNames = {"Middle Name"};
    Type dataType = Type.withNullability(false).STRING;
    AddColumn addColumn = (AddColumn) TableChange.addColumn(fieldNames, dataType, null, null);

    assertArrayEquals(fieldNames, addColumn.fieldNames());
    assertEquals(dataType, addColumn.getDataType());
    assertNull(addColumn.getComment());
    assertNull(addColumn.getPosition());
  }

  @Test
  public void testRenameColumn() {
    String[] fieldNames = {"Last Name"};
    String newName = "Family Name";
    RenameColumn renameColumn = (RenameColumn) TableChange.renameColumn(fieldNames, newName);

    assertArrayEquals(fieldNames, renameColumn.fieldNames());
    assertEquals(newName, renameColumn.getNewName());
  }

  @Test
  public void testRenameNestedColumn() {
    String[] fieldNames = {"Name", "First Name"};
    String newName = "Name.first";
    RenameColumn renameColumn = (RenameColumn) TableChange.renameColumn(fieldNames, newName);

    assertArrayEquals(fieldNames, renameColumn.fieldNames());
    assertEquals(newName, renameColumn.getNewName());
  }

  @Test
  public void testUpdateColumnPosition() {
    String[] fieldNames = {"First Name"};
    ColumnPosition newPosition = TableChange.ColumnPosition.first();
    UpdateColumnPosition updateColumnPosition =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fieldNames, newPosition);

    assertArrayEquals(fieldNames, updateColumnPosition.fieldNames());
    assertEquals(newPosition, updateColumnPosition.getPosition());
  }

  @Test
  public void testUpdateNestedColumnPosition() {
    String[] fieldNames = {"Name", "Last Name"};
    ColumnPosition newPosition = TableChange.ColumnPosition.after("First Name");
    UpdateColumnPosition updateColumnPosition =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fieldNames, newPosition);

    assertArrayEquals(fieldNames, updateColumnPosition.fieldNames());
    assertEquals(newPosition, updateColumnPosition.getPosition());
  }

  @Test
  public void testUpdateColumnComment() {
    String[] fieldNames = {"First Name"};
    String newComment = "First or given name";
    UpdateColumnComment updateColumnComment =
        (UpdateColumnComment) TableChange.updateColumnComment(fieldNames, newComment);

    assertArrayEquals(fieldNames, updateColumnComment.fieldNames());
    assertEquals(newComment, updateColumnComment.getNewComment());
  }

  @Test
  public void testUpdateNestedColumnComment() {
    String[] fieldNames = {"Name", "Last Name"};
    String newComment = "Last or family name";
    UpdateColumnComment updateColumnComment =
        (UpdateColumnComment) TableChange.updateColumnComment(fieldNames, newComment);

    assertArrayEquals(fieldNames, updateColumnComment.fieldNames());
    assertEquals(newComment, updateColumnComment.getNewComment());
  }

  @Test
  public void testDeleteColumn() {
    String[] fieldNames = {"existing_column"};
    Boolean ifExists = true;
    DeleteColumn deleteColumn = (DeleteColumn) TableChange.deleteColumn(fieldNames, ifExists);

    assertArrayEquals(fieldNames, deleteColumn.fieldNames());
    assertEquals(ifExists, deleteColumn.getIfExists());
  }

  @Test
  public void testDeleteNestedColumn() {
    String[] fieldNames = {"nested", "existing_column"};
    Boolean ifExists = false;
    DeleteColumn deleteColumn = (DeleteColumn) TableChange.deleteColumn(fieldNames, ifExists);

    assertArrayEquals(fieldNames, deleteColumn.fieldNames());
    assertEquals(ifExists, deleteColumn.getIfExists());
  }

  @Test
  public void testUpdateColumnType() {
    String[] fieldNames = {"existing_column"};
    Type dataType = Type.withNullability(false).STRING;
    UpdateColumnType updateColumnType =
        (UpdateColumnType) TableChange.updateColumnType(fieldNames, dataType);

    assertArrayEquals(fieldNames, updateColumnType.fieldNames());
    assertEquals(dataType, updateColumnType.getNewDataType());
  }

  @Test
  public void testUpdateNestedColumnType() {
    String[] fieldNames = {"nested", "existing_column"};
    Type dataType = Type.withNullability(false).STRING;
    UpdateColumnType updateColumnType =
        (UpdateColumnType) TableChange.updateColumnType(fieldNames, dataType);

    assertArrayEquals(fieldNames, updateColumnType.fieldNames());
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

    assertFalse(commentA.equals(null));
    assertFalse(commentA.equals(commentB));
    assertFalse(commentB.equals(commentA));
    assertNotEquals(commentA.hashCode(), commentB.hashCode());
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
  void testColumnUpdateTypeEqualsAndHashCode() {
    String[] nameA = {"First Name"};
    Type dataType = Type.withNullability(false).STRING;
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
    Type dataType = Type.withNullability(false).STRING;
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
    String[] fieldNamesA = {"First Name"};
    ColumnPosition newPosition = TableChange.ColumnPosition.first();
    UpdateColumnPosition changeA =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fieldNamesA, newPosition);
    String[] fieldNamesB = {"First Name"};
    UpdateColumnPosition changeB =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fieldNamesB, newPosition);

    assertTrue(changeA.equals(changeB));
    assertTrue(changeB.equals(changeA));
    assertEquals(changeA.hashCode(), changeB.hashCode());
  }

  @Test
  void testUpdateColumnPositionNotEqualsAndHashCode() {
    String[] fieldNamesA = {"First Name"};
    ColumnPosition newPosition = TableChange.ColumnPosition.first();
    UpdateColumnPosition changeA =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fieldNamesA, newPosition);
    String[] fieldNamesB = {"Last Name"};
    UpdateColumnPosition changeB =
        (UpdateColumnPosition) TableChange.updateColumnPosition(fieldNamesB, newPosition);

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
    String[] fieldNamesA = {"Name"};
    Type dataTypeA = Type.withNullability(false).STRING;
    String commentA = "Person name";
    AddColumn columnA = (AddColumn) TableChange.addColumn(fieldNamesA, dataTypeA, commentA);
    String[] fieldNamesB = {"Name"};
    Type dataTypeB = Type.withNullability(false).STRING;
    String commentB = "Person name";
    AddColumn columnB = (AddColumn) TableChange.addColumn(fieldNamesB, dataTypeB, commentB);

    assertTrue(columnA.equals(columnB));
    assertTrue(columnB.equals(columnA));
    assertEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  void testAddColumnNotEqualsAndHashCode() {
    String[] fieldNamesA = {"Name"};
    Type dataTypeA = Type.withNullability(false).STRING;
    String commentA = "Person name";
    AddColumn columnA = (AddColumn) TableChange.addColumn(fieldNamesA, dataTypeA, commentA);
    String[] fieldNamesB = {"First Name"};
    Type dataTypeB = Type.withNullability(false).STRING;
    String commentB = "Person name";
    AddColumn columnB = (AddColumn) TableChange.addColumn(fieldNamesB, dataTypeB, commentB);

    assertFalse(columnA.equals(null));
    assertFalse(columnA.equals(columnB));
    assertFalse(columnB.equals(columnA));
    assertNotEquals(columnA.hashCode(), columnB.hashCode());
  }
}
