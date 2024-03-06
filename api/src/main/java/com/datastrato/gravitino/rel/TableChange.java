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

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.types.Type;
import java.util.Arrays;
import java.util.Objects;

/**
 * The TableChange interface defines the public API for managing tables in a schema. If the catalog
 * implementation supports tables, it must implement this interface.
 */
@Evolving
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
    return new AddColumn(fieldName, dataType, null, null, true, false);
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
    return new AddColumn(fieldName, dataType, comment, null, true, false);
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
    return new AddColumn(fieldName, dataType, null, position, true, false);
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
    return new AddColumn(fieldName, dataType, comment, position, true, false);
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
    return new AddColumn(fieldName, dataType, null, null, nullable, false);
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
    return new AddColumn(fieldName, dataType, comment, null, nullable, false);
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
    return new AddColumn(fieldName, dataType, comment, position, nullable, false);
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
   * @param autoIncrement The new column's autoIncrement.
   * @return A TableChange for the addition.
   */
  static TableChange addColumn(
      String[] fieldName,
      Type dataType,
      String comment,
      ColumnPosition position,
      boolean nullable,
      boolean autoIncrement) {
    return new AddColumn(fieldName, dataType, comment, position, nullable, autoIncrement);
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

  /**
   * Create a TableChange for adding an index.
   *
   * @param type The type of the index.
   * @param name The name of the index.
   * @param fieldNames The field names of the index.
   * @return A TableChange for the add index.
   */
  static TableChange addIndex(Index.IndexType type, String name, String[][] fieldNames) {
    return new AddIndex(type, name, fieldNames);
  }

  /**
   * Create a TableChange for deleting an index.
   *
   * @param name The name of the index to be dropped.
   * @param ifExists If true, silence the error if column does not exist during drop. Otherwise, an
   *     {@link IllegalArgumentException} will be thrown.
   * @return A TableChange for the delete index.
   */
  static TableChange deleteIndex(String name, Boolean ifExists) {
    return new DeleteIndex(name, ifExists);
  }

  /**
   * Create a TableChange for updating the autoIncrement of a field.
   *
   * @param fieldName The field name of the column to update.
   * @param autoIncrement Whether the column is auto-incremented.
   * @return A TableChange for the update.
   */
  static TableChange updateColumnAutoIncrement(String[] fieldName, boolean autoIncrement) {
    return new UpdateColumnAutoIncrement(fieldName, autoIncrement);
  }

  /** A TableChange to rename a table. */
  final class RenameTable implements TableChange {
    private final String newName;

    private RenameTable(String newName) {
      this.newName = newName;
    }

    /**
     * Retrieves the new name for the table.
     *
     * @return The new name of the table.
     */
    public String getNewName() {
      return newName;
    }

    /**
     * Compares this RenameTable instance with another object for equality. The comparison is based
     * on the new name of the table.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same table renaming; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RenameTable that = (RenameTable) o;
      return newName.equals(that.newName);
    }

    /**
     * Generates a hash code for this RenameTable instance. The hash code is based on the new name
     * of the table.
     *
     * @return A hash code value for this table renaming operation.
     */
    @Override
    public int hashCode() {
      return newName.hashCode();
    }

    /**
     * Returns a string representation of the RenameTable instance. This string format includes the
     * class name followed by the property name to be renamed.
     *
     * @return A string summary of the property rename instance.
     */
    @Override
    public String toString() {
      return "RENAMETABLE " + newName;
    }
  }

  /** A TableChange to update a table's comment. */
  final class UpdateComment implements TableChange {
    private final String newComment;

    private UpdateComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Retrieves the new comment for the table.
     *
     * @return The new comment of the table.
     */
    public String getNewComment() {
      return newComment;
    }

    /**
     * Compares this UpdateComment instance with another object for equality. The comparison is
     * based on the new comment of the table.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same table comment update; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateComment that = (UpdateComment) o;
      return newComment.equals(that.newComment);
    }

    /**
     * Generates a hash code for this UpdateComment instance. The hash code is based on the new
     * comment of the table.
     *
     * @return A hash code value for this table comment update operation.
     */
    @Override
    public int hashCode() {
      return newComment.hashCode();
    }

    /**
     * Returns a string representation of the UpdateComment instance. This string format includes
     * the class name followed by the property name to be updated.
     *
     * @return A string representation of the UpdateComment instance.
     */
    @Override
    public String toString() {
      return "UPDATECOMMENT " + newComment;
    }
  }

  /**
   * A TableChange to set a table property.
   *
   * <p>If the property already exists, it must be replaced with the new value.
   */
  final class SetProperty implements TableChange {
    private final String property;
    private final String value;

    /**
     * Creates a new SetProperty instance.
     *
     * @param property The name of the property to be set.
     * @param value The new value of the property.
     */
    public SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * Retrieves the name of the property.
     *
     * @return The name of the property.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Retrieves the value of the property.
     *
     * @return The value of the property.
     */
    public String getValue() {
      return value;
    }

    /**
     * Compares this SetProperty instance with another object for equality. The comparison is based
     * on both the property name and its value.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same property setting; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SetProperty that = (SetProperty) o;
      return property.equals(that.property) && value.equals(that.value);
    }

    /**
     * Generates a hash code for this SetProperty instance. The hash code is based on both the
     * property name and its value.
     *
     * @return A hash code value for this property setting.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

    /**
     * Returns a string representation of the SetProperty instance. This string format includes the
     * class name followed by the property name and value to be set.
     *
     * @return A string representation of the SetProperty instance.
     */
    @Override
    public String toString() {
      return "SETPROPERTY " + property + " " + value;
    }
  }

  /**
   * A TableChange to remove a table property.
   *
   * <p>If the property does not exist, the change should succeed.
   */
  final class RemoveProperty implements TableChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * Retrieves the name of the property to be removed from the table.
     *
     * @return The name of the property scheduled for removal.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Compares this RemoveProperty instance with another object for equality. Two instances are
     * considered equal if they target the same property for removal from the table.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same property removal; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoveProperty that = (RemoveProperty) o;
      return property.equals(that.property);
    }

    /**
     * Generates a hash code for this RemoveProperty instance. The hash code is based on the
     * property name that is to be removed from the table.
     *
     * @return A hash code value for this property removal operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property);
    }

    /**
     * Provides a string representation of the RemoveProperty instance. This string format includes
     * the class name followed by the property name to be removed.
     *
     * @return A string summary of the property removal operation.
     */
    @Override
    public String toString() {
      return "REMOVEPROPERTY " + property;
    }
  }

  /**
   * A TableChange to add an index. Add an index key based on the type and field name passed in as
   * well as the name.
   */
  final class AddIndex implements TableChange {

    private final Index.IndexType type;
    private final String name;

    private final String[][] fieldNames;

    /**
     * @param type The type of the index.
     * @param name The name of the index.
     * @param fieldNames The field names of the index.
     */
    public AddIndex(Index.IndexType type, String name, String[][] fieldNames) {
      this.type = type;
      this.name = name;
      this.fieldNames = fieldNames;
    }

    /** @return The type of the index. */
    public Index.IndexType getType() {
      return type;
    }

    /** @return The name of the index. */
    public String getName() {
      return name;
    }

    /** @return The field names of the index. */
    public String[][] getFieldNames() {
      return fieldNames;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AddIndex addIndex = (AddIndex) o;
      return type == addIndex.type
          && Objects.equals(name, addIndex.name)
          && Arrays.deepEquals(fieldNames, addIndex.fieldNames);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(type, name);
      result = 31 * result + Arrays.hashCode(fieldNames);
      return result;
    }
  }

  /**
   * A TableChange to delete an index.
   *
   * <p>If the index does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  final class DeleteIndex implements TableChange {
    private final String name;
    private final boolean ifExists;

    /**
     * @param name name of the index.
     * @param ifExists If true, silence the error if index does not exist during drop.
     */
    public DeleteIndex(String name, boolean ifExists) {
      this.name = name;
      this.ifExists = ifExists;
    }

    /** @return The name of the index to be deleted. */
    public String getName() {
      return name;
    }

    /** @return If true, silence the error if index does not exist during drop. */
    public boolean isIfExists() {
      return ifExists;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DeleteIndex that = (DeleteIndex) o;
      return Objects.equals(name, that.name) && Objects.equals(ifExists, that.ifExists);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, ifExists);
    }
  }

  /**
   * The interface for all column positions. Column positions are used to specify the position of a
   * column when adding a new column to a table.
   */
  interface ColumnPosition {

    /** @return The first position of ColumnPosition instance. */
    static ColumnPosition first() {
      return First.INSTANCE;
    }

    /**
     * Returns the position after the given column.
     *
     * @param column The name of the reference column to place the new column after.
     * @return The position after the given column.
     */
    static ColumnPosition after(String column) {
      return new After(column);
    }

    /** @return The default position of ColumnPosition instance. */
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
  final class After implements ColumnPosition {
    private final String column;

    private After(String column) {
      if (null == column) {
        throw new IllegalArgumentException("column can not be null.");
      }
      this.column = column;
    }

    /**
     * Retrieves the name of the reference column after which the specified column will be placed.
     *
     * @return The name of the reference column.
     */
    public String getColumn() {
      return column;
    }

    /**
     * Compares this After instance with another object for equality. Two instances are considered
     * equal if they refer to the same column name.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object refers to the same column; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      After after = (After) o;
      return column.equals(after.column);
    }

    /**
     * Generates a hash code for this After instance. The hash code is based on the column name.
     *
     * @return A hash code value for this column positioning operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(column);
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

    /**
     * Retrieves the field name of the column to be modified.
     *
     * @return An array of strings representing the field name.
     */
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
  final class AddColumn implements ColumnChange {
    private final String[] fieldName;
    private final Type dataType;
    private final String comment;
    private final ColumnPosition position;
    private final boolean nullable;

    private final boolean autoIncrement;

    private AddColumn(
        String[] fieldName,
        Type dataType,
        String comment,
        ColumnPosition position,
        boolean nullable,
        boolean autoIncrement) {
      this.fieldName = fieldName;
      this.dataType = dataType;
      this.comment = comment;
      this.position = position == null ? ColumnPosition.defaultPos() : position;
      this.nullable = nullable;
      this.autoIncrement = autoIncrement;
    }

    /**
     * Retrieves the field name of the new column.
     *
     * @return An array of strings representing the field name.
     */
    public String[] getFieldName() {
      return fieldName;
    }

    /**
     * Retrieves the data type of the new column.
     *
     * @return The data type of the column.
     */
    public Type getDataType() {
      return dataType;
    }

    /**
     * Retrieves the comment for the new column.
     *
     * @return The comment associated with the column.
     */
    public String getComment() {
      return comment;
    }

    /**
     * Retrieves the position where the new column should be added.
     *
     * @return The position of the column.
     */
    public ColumnPosition getPosition() {
      return position;
    }

    /**
     * Checks if the new column is nullable.
     *
     * @return true if the column is nullable; false otherwise.
     */
    public boolean isNullable() {
      return nullable;
    }

    /**
     * Checks if the new column is autoIncrement.
     *
     * @return true if the column is autoIncrement; false otherwise.
     */
    public boolean isAutoIncrement() {
      return autoIncrement;
    }

    /**
     * Compares this AddColumn instance with another object for equality. The comparison is based on
     * the field name, data type, comment, position, and nullability.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same column addition; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AddColumn addColumn = (AddColumn) o;
      return nullable == addColumn.nullable
          && autoIncrement == addColumn.autoIncrement
          && Arrays.equals(fieldName, addColumn.fieldName)
          && Objects.equals(dataType, addColumn.dataType)
          && Objects.equals(comment, addColumn.comment)
          && Objects.equals(position, addColumn.position);
    }

    /**
     * Generates a hash code for this AddColumn instance. This hash code is based on the field name,
     * data type, comment, position, nullability, and autoIncrement.
     *
     * @return A hash code value for this column addition operation.
     */
    @Override
    public int hashCode() {
      int result = Objects.hash(dataType, comment, position, nullable, autoIncrement);
      result = 31 * result + Arrays.hashCode(fieldName);
      return result;
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
  final class RenameColumn implements ColumnChange {
    private final String[] fieldName;
    private final String newName;

    private RenameColumn(String[] fieldName, String newName) {
      this.fieldName = fieldName;
      this.newName = newName;
    }

    /**
     * Retrieves the hierarchical field name of the column to be renamed.
     *
     * @return An array of strings representing the field name.
     */
    public String[] getFieldName() {
      return fieldName;
    }

    /**
     * Retrieves the new name for the column.
     *
     * @return The new name of the column.
     */
    public String getNewName() {
      return newName;
    }

    /**
     * Compares this RenameColumn instance with another object for equality. The comparison is based
     * on the field name array and the new name.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same column renaming; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RenameColumn that = (RenameColumn) o;
      return Arrays.equals(fieldName, that.fieldName) && Objects.equals(newName, that.newName);
    }

    /**
     * Generates a hash code for this RenameColumn instance. This hash code is based on both the
     * hierarchical field name and the new name.
     *
     * @return A hash code value for this column renaming operation.
     */
    @Override
    public int hashCode() {
      int result = Objects.hash(newName);
      result = 31 * result + Arrays.hashCode(fieldName);
      return result;
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
  final class UpdateColumnType implements ColumnChange {
    private final String[] fieldName;
    private final Type newDataType;

    private UpdateColumnType(String[] fieldName, Type newDataType) {
      this.fieldName = fieldName;
      this.newDataType = newDataType;
    }

    /**
     * Retrieves the field name of the column whose data type is being updated.
     *
     * @return An array of strings representing the field name.
     */
    public String[] getFieldName() {
      return fieldName;
    }

    /**
     * Retrieves the new data type for the column.
     *
     * @return The new data type of the column.
     */
    public Type getNewDataType() {
      return newDataType;
    }

    /**
     * Compares this UpdateColumnType instance with another object for equality. The comparison is
     * based on the field name array and the new data type.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same data type update; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateColumnType that = (UpdateColumnType) o;
      return Arrays.equals(fieldName, that.fieldName)
          && Objects.equals(newDataType, that.newDataType);
    }

    /**
     * Generates a hash code for this UpdateColumnType instance. The hash code is based on both the
     * hierarchical field name and the new data type.
     *
     * @return A hash code value for this data type update operation.
     */
    @Override
    public int hashCode() {
      int result = Objects.hash(newDataType);
      result = 31 * result + Arrays.hashCode(fieldName);
      return result;
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
  final class UpdateColumnComment implements ColumnChange {
    private final String[] fieldName;
    private final String newComment;

    private UpdateColumnComment(String[] fieldName, String newComment) {
      this.fieldName = fieldName;
      this.newComment = newComment;
    }

    /**
     * Retrieves the field name of the column whose comment is being updated.
     *
     * @return An array of strings representing the field name.
     */
    public String[] getFieldName() {
      return fieldName;
    }

    /**
     * Retrieves the new comment for the column.
     *
     * @return The new comment of the column.
     */
    public String getNewComment() {
      return newComment;
    }

    /**
     * Compares this UpdateColumnComment instance with another object for equality. The comparison
     * is based on the field name array and the new comment.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same comment update; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateColumnComment that = (UpdateColumnComment) o;
      return Arrays.equals(fieldName, that.fieldName)
          && Objects.equals(newComment, that.newComment);
    }

    /**
     * Generates a hash code for this UpdateColumnComment instance. The hash code is based on both
     * the hierarchical field name and the new comment.
     *
     * @return A hash code value for this comment update operation.
     */
    @Override
    public int hashCode() {
      int result = Objects.hash(newComment);
      result = 31 * result + Arrays.hashCode(fieldName);
      return result;
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
  final class UpdateColumnPosition implements ColumnChange {
    private final String[] fieldName;
    private final ColumnPosition position;

    private UpdateColumnPosition(String[] fieldName, ColumnPosition position) {
      this.fieldName = fieldName;
      this.position = position;
    }

    /**
     * Retrieves the field name of the column whose position is being updated.
     *
     * @return An array of strings representing the field name.
     */
    public String[] getFieldName() {
      return fieldName;
    }

    /**
     * Retrieves the new position for the column.
     *
     * @return The new position of the column.
     */
    public ColumnPosition getPosition() {
      return position;
    }

    /**
     * Compares this UpdateColumnPosition instance with another object for equality. The comparison
     * is based on the field name array and the new position.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same position update; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateColumnPosition that = (UpdateColumnPosition) o;
      return Arrays.equals(fieldName, that.fieldName) && Objects.equals(position, that.position);
    }

    /**
     * Generates a hash code for this UpdateColumnPosition instance. The hash code is based on both
     * the hierarchical field name and the new position.
     *
     * @return A hash code value for this position update operation.
     */
    @Override
    public int hashCode() {
      int result = Objects.hash(position);
      result = 31 * result + Arrays.hashCode(fieldName);
      return result;
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
  final class DeleteColumn implements ColumnChange {
    private final String[] fieldName;
    private final Boolean ifExists;

    private DeleteColumn(String[] fieldName, Boolean ifExists) {
      this.fieldName = fieldName;
      this.ifExists = ifExists;
    }

    /**
     * Retrieves the field name of the column to be deleted.
     *
     * @return An array of strings representing the field name.
     */
    public String[] getFieldName() {
      return fieldName;
    }

    /**
     * Checks if the field should be deleted only if it exists.
     *
     * @return true if the field should be deleted only if it exists; false otherwise.
     */
    public Boolean getIfExists() {
      return ifExists;
    }

    /**
     * Compares this DeleteColumn instance with another object for equality. The comparison is based
     * on the field name array and the 'ifExists' flag.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same field deletion; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DeleteColumn that = (DeleteColumn) o;
      return Arrays.equals(fieldName, that.fieldName) && Objects.equals(ifExists, that.ifExists);
    }

    /**
     * Generates a hash code for this DeleteColumn instance. The hash code is based on both the
     * hierarchical field name and the 'ifExists' flag.
     *
     * @return A hash code value for this field deletion operation.
     */
    @Override
    public int hashCode() {
      int result = Objects.hash(ifExists);
      result = 31 * result + Arrays.hashCode(fieldName);
      return result;
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

    /**
     * Retrieves the field name of the column whose nullability is being updated.
     *
     * @return An array of strings representing the field name.
     */
    @Override
    public String[] fieldName() {
      return fieldName;
    }

    /**
     * The nullable flag of the column.
     *
     * @return true if the column is nullable; false otherwise.
     */
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

  /**
   * A TableChange to update the autoIncrement of a field. True is to add autoIncrement, false is to
   * delete autoIncrement.
   */
  final class UpdateColumnAutoIncrement implements ColumnChange {
    private final String[] fieldName;

    private final boolean autoIncrement;

    /**
     * Creates a new UpdateColumnAutoIncrement instance.
     *
     * @param fieldName The name of the field to be updated.
     * @param autoIncrement The new autoIncrement flag of the field.
     */
    public UpdateColumnAutoIncrement(String[] fieldName, boolean autoIncrement) {
      this.fieldName = fieldName;
      this.autoIncrement = autoIncrement;
    }

    /**
     * Retrieves the field name of the column whose autoIncrement is being updated.
     *
     * @return An array of strings representing the field name.
     */
    @Override
    public String[] fieldName() {
      return fieldName;
    }

    /**
     * The autoIncrement flag of the column.
     *
     * @return true if the column is autoIncrement; false otherwise.
     */
    public boolean isAutoIncrement() {
      return autoIncrement;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateColumnAutoIncrement that = (UpdateColumnAutoIncrement) o;
      return autoIncrement == that.autoIncrement && Arrays.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(autoIncrement);
      result = 31 * result + Arrays.hashCode(fieldName);
      return result;
    }
  }
}
