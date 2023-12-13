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

import com.datastrato.gravitino.rel.types.Type;
import java.util.Arrays;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * The TableChange interface defines the public API for managing tables in a schema. If the catalog
 * implementation supports tables, it must implement this interface.
 */
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
   * @param fieldName The field name of the new column.
   * @param dataType The new column's data type.
   * @return A TableChange for the addition.
   */
  static TableChange addColumn(String[] fieldName, Type dataType) {
    return new AddColumn(fieldName, dataType, null, null, true);
  }

  /**
   * Create a TableChange for adding a column.
   *
   * <p>If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldName The field name of the new column.
   * @param dataType The new column's data type.
   * @param comment The new field's comment string.
   * @return A TableChange for the addition.
   */
  static TableChange addColumn(String[] fieldName, Type dataType, String comment) {
    return new AddColumn(fieldName, dataType, comment, null, true);
  }

  /**
   * Create a TableChange for adding a column.
   *
   * <p>If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldName The field name of the new column.
   * @param dataType The new column's data type.
   * @param position The new column's position.
   * @return A TableChange for the addition.
   */
  static TableChange addColumn(String[] fieldName, Type dataType, ColumnPosition position) {
    return new AddColumn(fieldName, dataType, null, position, true);
  }

  /**
   * Create a TableChange for adding a column.
   *
   * <p>If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldName Field name of the new column.
   * @param dataType The new column's data type.
   * @param comment The new field's comment string.
   * @param position The new column's position.
   * @return A TableChange for the addition.
   */
  static TableChange addColumn(
      String[] fieldName, Type dataType, String comment, ColumnPosition position) {
    return new AddColumn(fieldName, dataType, comment, position, true);
  }

  /**
   * Create a TableChange for adding a column.
   *
   * <p>If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldName Field name of the new column.
   * @param dataType The new column's data type.
   * @param nullable The new column's nullable.
   * @return A TableChange for the addition.
   */
  static TableChange addColumn(String[] fieldName, Type dataType, boolean nullable) {
    return new AddColumn(fieldName, dataType, null, null, nullable);
  }

  /**
   * Create a TableChange for adding a column.
   *
   * @param fieldName Field name of the new column.
   * @param dataType The new column's data type.
   * @param comment The new field's comment string.
   * @param nullable The new column's nullable.
   * @return A TableChange for the addition.
   */
  static TableChange addColumn(
      String[] fieldName, Type dataType, String comment, boolean nullable) {
    return new AddColumn(fieldName, dataType, comment, null, nullable);
  }

  /**
   * Create a TableChange for adding a column.
   *
   * <p>If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldName Field name of the new column.
   * @param dataType The new column's data type.
   * @param comment The new field's comment string.
   * @param position The new column's position.
   * @param nullable The new column's nullable.
   * @return A TableChange for the addition.
   */
  static TableChange addColumn(
      String[] fieldName,
      Type dataType,
      String comment,
      ColumnPosition position,
      boolean nullable) {
    return new AddColumn(fieldName, dataType, comment, position, nullable);
  }

  /**
   * Create a TableChange for renaming a field.
   *
   * <p>The name is used to find the field to rename. The new name will replace the leaf field name.
   * For example, renameColumn(["a", "b", "c"], "x") should produce column a.b.x.
   *
   * <p>If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldName The current field name.
   * @param newName The new name.
   * @return A TableChange for the rename.
   */
  static TableChange renameColumn(String[] fieldName, String newName) {
    return new RenameColumn(fieldName, newName);
  }

  /**
   * Create a TableChange for updating the type of a field that is nullable.
   *
   * <p>The field name are used to find the field to update.
   *
   * <p>If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldName The field name of the column to update.
   * @param newDataType The new data type.
   * @return A TableChange for the update.
   */
  static TableChange updateColumnType(String[] fieldName, Type newDataType) {
    return new UpdateColumnType(fieldName, newDataType);
  }

  /**
   * Create a TableChange for updating the comment of a field.
   *
   * <p>The name is used to find the field to update.
   *
   * <p>If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldName The field name of the column to update.
   * @param newComment The new comment.
   * @return A TableChange for the update.
   */
  static TableChange updateColumnComment(String[] fieldName, String newComment) {
    return new UpdateColumnComment(fieldName, newComment);
  }

  /**
   * Create a TableChange for updating the position of a field.
   *
   * <p>The name is used to find the field to update.
   *
   * <p>If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldName The field name of the column to update.
   * @param newPosition The new position.
   * @return A TableChange for the update.
   */
  static TableChange updateColumnPosition(String[] fieldName, ColumnPosition newPosition) {
    return new UpdateColumnPosition(fieldName, newPosition);
  }

  /**
   * Create a TableChange for deleting a field.
   *
   * <p>If the field does not exist, the change will result in an {@link IllegalArgumentException}
   * unless {@code ifExists} is true.
   *
   * @param fieldName Field name of the column to delete.
   * @param ifExists If true, silence the error if column does not exist during drop. Otherwise, an
   *     {@link IllegalArgumentException} will be thrown.
   * @return A TableChange for the delete.
   */
  static TableChange deleteColumn(String[] fieldName, Boolean ifExists) {
    return new DeleteColumn(fieldName, ifExists);
  }

  /**
   * Create a TableChange for updating the nullability of a field.
   *
   * <p>The name are used to find the field to update.
   *
   * <p>If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldName The field name of the column to update.
   * @param nullable The new nullability.
   * @return A TableChange for the update.
   */
  static TableChange updateColumnNullability(String[] fieldName, boolean nullable) {
    return new UpdateColumnNullability(fieldName, nullable);
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

  /**
   * The interface for all column positions. Column positions are used to specify the position of a
   * column when adding a new column to a table.
   */
  interface ColumnPosition {

    static ColumnPosition first() {
      return First.INSTANCE;
    }

    static ColumnPosition after(String column) {
      return new After(column);
    }

    static ColumnPosition defaultPos() {
      return Default.INSTANCE;
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

  /**
   * Column position DEFAULT means the position of the column was ignored by the user, and should be
   * determined by the catalog implementation.
   */
  final class Default implements ColumnPosition {
    private static final Default INSTANCE = new Default();

    private Default() {}

    @Override
    public String toString() {
      return "DEFAULT";
    }
  }

  /**
   * The interface for all column changes. Column changes are used to modify the schema of a table.
   */
  interface ColumnChange extends TableChange {
    String[] fieldName();
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
    private final String[] fieldName;

    @Getter private final Type dataType;

    @Getter private final String comment;

    @Getter private final ColumnPosition position;

    @Getter private final boolean nullable;

    private AddColumn(
        String[] fieldName,
        Type dataType,
        String comment,
        ColumnPosition position,
        boolean nullable) {
      this.fieldName = fieldName;
      this.dataType = dataType;
      this.comment = comment;
      this.position = position == null ? ColumnPosition.defaultPos() : position;
      this.nullable = nullable;
    }

    @Override
    public String[] fieldName() {
      return fieldName;
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
    private final String[] fieldName;

    @Getter private final String newName;

    private RenameColumn(String[] fieldName, String newName) {
      this.fieldName = fieldName;
      this.newName = newName;
    }

    @Override
    public String[] fieldName() {
      return fieldName;
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
    private final String[] fieldName;

    @Getter private final Type newDataType;

    private UpdateColumnType(String[] fieldName, Type newDataType) {
      this.fieldName = fieldName;
      this.newDataType = newDataType;
    }

    @Override
    public String[] fieldName() {
      return fieldName;
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
    private final String[] fieldName;

    @Getter private final String newComment;

    private UpdateColumnComment(String[] fieldName, String newComment) {
      this.fieldName = fieldName;
      this.newComment = newComment;
    }

    @Override
    public String[] fieldName() {
      return fieldName;
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
    private final String[] fieldName;

    @Getter private final ColumnPosition position;

    private UpdateColumnPosition(String[] fieldName, ColumnPosition position) {
      this.fieldName = fieldName;
      this.position = position;
    }

    @Override
    public String[] fieldName() {
      return fieldName;
    }
  }

  /**
   * A TableChange to delete a field.
   *
   * <p>If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  @EqualsAndHashCode
  final class DeleteColumn implements ColumnChange {
    private final String[] fieldName;

    @Getter private final Boolean ifExists;

    private DeleteColumn(String[] fieldName, Boolean ifExists) {
      this.fieldName = fieldName;
      this.ifExists = ifExists;
    }

    @Override
    public String[] fieldName() {
      return fieldName;
    }
  }

  /**
   * A TableChange to update the nullability of a field.
   *
   * <p>The field names are used to find the field to update.
   *
   * <p>If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  final class UpdateColumnNullability implements ColumnChange {
    private final String[] fieldName;

    private final boolean nullable;

    private UpdateColumnNullability(String[] fieldName, boolean nullable) {
      this.fieldName = fieldName;
      this.nullable = nullable;
    }

    @Override
    public String[] fieldName() {
      return fieldName;
    }

    public boolean nullable() {
      return nullable;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UpdateColumnNullability that = (UpdateColumnNullability) o;
      return nullable == that.nullable && Arrays.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(nullable);
      result = 31 * result + Arrays.hashCode(fieldName);
      return result;
    }
  }
}
