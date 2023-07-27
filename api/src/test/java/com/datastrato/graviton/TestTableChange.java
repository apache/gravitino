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
}
