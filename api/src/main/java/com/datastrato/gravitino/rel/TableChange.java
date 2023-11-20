/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Referred from Apache Spark's connector/catalog implementation
// sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/TableChange.java

package com.datastrato.gravitino.rel;

import io.substrait.type.Type;
import lombok.EqualsAndHashCode;
import lombok.Getter;

public interface TableChange {

  /**
   * Create a TableChange for renaming a table.
   *
   * @param newName The new table name.
   * @return A TableChange for the rename.
   */
  static TableChange rename(String newName) {
    return new RenameTable(newName);
  }

  /**
   * Create a TableChange for updating the comment.
   *
   * @param newComment The new comment.
   * @return A TableChange for the update.
   */
  static TableChange updateComment(String newComment) {
    return new UpdateComment(newComment);
  }

  /**
   * Create a TableChange for setting a table property.
   *
   * <p>If the property already exists, it will be replaced with the new value.
   *
   * @param property The property name.
   * @param value The new property value.
   * @return A TableChange for the addition.
   */
  static TableChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Create a TableChange for removing a table property.
   *
   * <p>If the property does not exist, the change will succeed.
   *
   * @param property The property name.
   * @return A TableChange for the addition.
   */
  static TableChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /**
   * Create a TableChange for adding an optional column.
   *
   * <p>If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames The field names of the new column.
   * @param dataType The new column's data type.
   * @return A TableChange for the addition.
   */
  static TableChange addColumn(String[] fieldNames, Type dataType) {
    return new AddColumn(fieldNames, dataType, null, null);
  }

  /**
   * Create a TableChange for adding a column.
   *
   * <p>If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames The field names of the new column.
   * @param dataType The new column's data type.
   * @param comment The new field's comment string.
   * @return A TableChange for the addition.
   */
  static TableChange addColumn(String[] fieldNames, Type dataType, String comment) {
    return new AddColumn(fieldNames, dataType, comment, null);
  }

  /**
   * Create a TableChange for adding a column.
   *
   * <p>If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames Field names of the new column.
   * @param dataType The new column's data type.
   * @param comment The new field's comment string.
   * @param position The new columns's position.
   * @return A TableChange for the addition.
   */
  static TableChange addColumn(
      String[] fieldNames, Type dataType, String comment, ColumnPosition position) {
    return new AddColumn(fieldNames, dataType, comment, position);
  }

  /**
   * Create a TableChange for renaming a field.
   *
   * <p>The name is used to find the field to rename. The new name will replace the leaf field name.
   * For example, renameColumn(["a", "b", "c"], "x") should produce column a.b.x.
   *
   * <p>If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames The current field names.
   * @param newName The new name.
   * @return A TableChange for the rename.
   */
  static TableChange renameColumn(String[] fieldNames, String newName) {
    return new RenameColumn(fieldNames, newName);
  }

  /**
   * Create a TableChange for updating the type of a field that is nullable.
   *
   * <p>The field names are used to find the field to update.
   *
   * <p>If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames The field names of the column to update.
   * @param newDataType The new data type.
   * @return A TableChange for the update.
   */
  static TableChange updateColumnType(String[] fieldNames, Type newDataType) {
    return new UpdateColumnType(fieldNames, newDataType);
  }

  /**
   * Create a TableChange for updating the comment of a field.
   *
   * <p>The name is used to find the field to update.
   *
   * <p>If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames The field names of the column to update.
   * @param newComment The new comment.
   * @return A TableChange for the update.
   */
  static TableChange updateColumnComment(String[] fieldNames, String newComment) {
    return new UpdateColumnComment(fieldNames, newComment);
  }

  /**
   * Create a TableChange for updating the position of a field.
   *
   * <p>The name is used to find the field to update.
   *
   * <p>If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames The field names of the column to update.
   * @param newPosition The new position.
   * @return A TableChange for the update.
   */
  static TableChange updateColumnPosition(String[] fieldNames, ColumnPosition newPosition) {
    return new UpdateColumnPosition(fieldNames, newPosition);
  }

  /**
   * Create a TableChange for deleting a field.
   *
   * <p>If the field does not exist, the change will result in an {@link IllegalArgumentException}
   * unless {@code ifExists} is true.
   *
   * @param fieldNames Field names of the column to delete.
   * @param ifExists If true, silence the error if column does not exist during drop. Otherwise, an
   *     {@link IllegalArgumentException} will be thrown.
   * @return A TableChange for the delete.
   */
  static TableChange deleteColumn(String[] fieldNames, Boolean ifExists) {
    return new DeleteColumn(fieldNames, ifExists);
  }

  /** A TableChange to rename a table. */
  @EqualsAndHashCode
  @Getter
  final class RenameTable implements TableChange {
    private final String newName;

    private RenameTable(String newName) {
      this.newName = newName;
    }
  }

  /** A TableChange to update a table's comment. */
  @EqualsAndHashCode
  @Getter
  final class UpdateComment implements TableChange {
    private final String newComment;

    private UpdateComment(String newComment) {
      this.newComment = newComment;
    }
  }

  /**
   * A TableChange to set a table property.
   *
   * <p>If the property already exists, it must be replaced with the new value.
   */
  @EqualsAndHashCode
  @Getter
  final class SetProperty implements TableChange {
    private final String property;
    private final String value;

    public SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }
  }

  /**
   * A TableChange to remove a table property.
   *
   * <p>If the property does not exist, the change should succeed.
   */
  @EqualsAndHashCode
  @Getter
  final class RemoveProperty implements TableChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }
  }

  interface ColumnPosition {

    static ColumnPosition first() {
      return First.INSTANCE;
    }

    static ColumnPosition after(String column) {
      return new After(column);
    }
  }

  /**
   * Column position FIRST means the specified column should be the first column. Note that, the
   * specified column may be a nested field, and then FIRST means this field should be the first one
   * within the struct.
   */
  final class First implements ColumnPosition {
    private static final First INSTANCE = new First();

    private First() {}

    @Override
    public String toString() {
      return "FIRST";
    }
  }

  /**
   * Column position AFTER means the specified column should be put after the given `column`. Note
   * that, the specified column may be a nested field, and then the given `column` refers to a field
   * in the same struct.
   */
  @EqualsAndHashCode
  @Getter
  final class After implements ColumnPosition {
    private final String column;

    private After(String column) {
      assert column != null;
      this.column = column;
    }

    @Override
    public String toString() {
      return "AFTER " + column;
    }
  }

  interface ColumnChange extends TableChange {
    String[] fieldNames();
  }

  /**
   * A TableChange to add a field. The implementation may need to back-fill all the existing data to
   * add this new column, or remember the column default value specified here and let the reader
   * fill the column value when reading existing data that do not have this new column.
   *
   * <p>If the field already exists, the change must result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change must
   * result in an {@link IllegalArgumentException}.
   */
  @EqualsAndHashCode
  final class AddColumn implements ColumnChange {
    private final String[] fieldNames;

    @Getter private final Type dataType;

    @Getter private final String comment;

    @Getter private final ColumnPosition position;

    private AddColumn(String[] fieldNames, Type dataType, String comment, ColumnPosition position) {
      this.fieldNames = fieldNames;
      this.dataType = dataType;
      this.comment = comment;
      this.position = position;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }
  }

  /**
   * A TableChange to rename a field.
   *
   * <p>The name is used to find the field to rename. The new name will replace the leaf field name.
   * For example, renameColumn("a.b.c", "x") should produce column a.b.x.
   *
   * <p>If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  @EqualsAndHashCode
  final class RenameColumn implements ColumnChange {
    private final String[] fieldNames;

    @Getter private final String newName;

    private RenameColumn(String[] fieldNames, String newName) {
      this.fieldNames = fieldNames;
      this.newName = newName;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }
  }

  /**
   * A TableChange to update the type of a field.
   *
   * <p>The field names are used to find the field to update.
   *
   * <p>If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  @EqualsAndHashCode
  final class UpdateColumnType implements ColumnChange {
    private final String[] fieldNames;

    @Getter private final Type newDataType;

    private UpdateColumnType(String[] fieldNames, Type newDataType) {
      this.fieldNames = fieldNames;
      this.newDataType = newDataType;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }
  }

  /**
   * A TableChange to update the comment of a field.
   *
   * <p>The field names are used to find the field to update.
   *
   * <p>If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  @EqualsAndHashCode
  final class UpdateColumnComment implements ColumnChange {
    private final String[] fieldNames;

    @Getter private final String newComment;

    private UpdateColumnComment(String[] fieldNames, String newComment) {
      this.fieldNames = fieldNames;
      this.newComment = newComment;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }
  }

  /**
   * A TableChange to update the position of a field.
   *
   * <p>The field names are used to find the field to update.
   *
   * <p>If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  @EqualsAndHashCode
  final class UpdateColumnPosition implements ColumnChange {
    private final String[] fieldNames;

    @Getter private final ColumnPosition position;

    private UpdateColumnPosition(String[] fieldNames, ColumnPosition position) {
      this.fieldNames = fieldNames;
      this.position = position;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }
  }

  /**
   * A TableChange to delete a field.
   *
   * <p>If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  @EqualsAndHashCode
  final class DeleteColumn implements ColumnChange {
    private final String[] fieldNames;

    @Getter private final Boolean ifExists;

    private DeleteColumn(String[] fieldNames, Boolean ifExists) {
      this.fieldNames = fieldNames;
      this.ifExists = ifExists;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }
  }
}
