/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.utils;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.TypeUtils.toPaimonType;
import static org.apache.paimon.schema.SchemaChange.addColumn;
import static org.apache.paimon.schema.SchemaChange.dropColumn;
import static org.apache.paimon.schema.SchemaChange.removeOption;
import static org.apache.paimon.schema.SchemaChange.renameColumn;
import static org.apache.paimon.schema.SchemaChange.setOption;
import static org.apache.paimon.schema.SchemaChange.updateColumnComment;
import static org.apache.paimon.schema.SchemaChange.updateColumnNullability;
import static org.apache.paimon.schema.SchemaChange.updateColumnPosition;
import static org.apache.paimon.schema.SchemaChange.updateColumnType;
import static org.apache.paimon.schema.SchemaChange.updateComment;

import com.datastrato.gravitino.catalog.lakehouse.paimon.ops.PaimonTableOps;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.TableChange.AddColumn;
import com.datastrato.gravitino.rel.TableChange.After;
import com.datastrato.gravitino.rel.TableChange.ColumnChange;
import com.datastrato.gravitino.rel.TableChange.ColumnPosition;
import com.datastrato.gravitino.rel.TableChange.Default;
import com.datastrato.gravitino.rel.TableChange.DeleteColumn;
import com.datastrato.gravitino.rel.TableChange.First;
import com.datastrato.gravitino.rel.TableChange.RemoveProperty;
import com.datastrato.gravitino.rel.TableChange.RenameColumn;
import com.datastrato.gravitino.rel.TableChange.SetProperty;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnComment;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnNullability;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnPosition;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnType;
import com.datastrato.gravitino.rel.TableChange.UpdateComment;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaChange.Move;

/** Utilities of {@link PaimonTableOps} to support table operation. */
public class TableOpsUtils {

  public static final Joiner DOT = Joiner.on(".");

  /**
   * Builds {@link SchemaChange} schema changes with given {@link TableChange} table changes.
   *
   * @param tableChanges The {@link TableChange} table changes.
   * @return The built {@link SchemaChange} schema changes.
   * @throws UnsupportedOperationException If the provided table change does not support.
   */
  public static List<SchemaChange> buildSchemaChanges(TableChange... tableChanges)
      throws UnsupportedOperationException {
    List<SchemaChange> schemaChanges = new ArrayList<>();
    for (TableChange tableChange : tableChanges) {
      schemaChanges.add(buildSchemaChange(tableChange));
    }
    return schemaChanges;
  }

  /**
   * Builds {@link SchemaChange} schema change with given {@link TableChange} table change.
   *
   * @param tableChange The {@link TableChange} table change.
   * @return The built {@link SchemaChange} schema change.
   * @throws UnsupportedOperationException If the provided table change does not support.
   */
  public static SchemaChange buildSchemaChange(TableChange tableChange)
      throws UnsupportedOperationException {
    if (tableChange instanceof ColumnChange) {
      String[] fieldNames = ((ColumnChange) tableChange).fieldName();
      Preconditions.checkArgument(
          fieldNames.length == 1,
          String.format(
              "Paimon does not support update non-primitive type column. Illegal column: %s.",
              fieldName(fieldNames)));
      if (tableChange instanceof AddColumn) {
        AddColumn addColumn = (AddColumn) tableChange;
        String fieldName = fieldName(addColumn);
        checkColumn(fieldName, addColumn.getDefaultValue(), addColumn.isAutoIncrement());
        return addColumn(
            fieldName,
            toPaimonType(addColumn.getDataType()).copy(addColumn.isNullable()),
            addColumn.getComment(),
            move(fieldName, addColumn.getPosition()));
      } else if (tableChange instanceof DeleteColumn) {
        return dropColumn(fieldName((DeleteColumn) tableChange));
      } else if (tableChange instanceof RenameColumn) {
        RenameColumn renameColumn = ((RenameColumn) tableChange);
        return renameColumn(fieldName(renameColumn), renameColumn.getNewName());
      } else if (tableChange instanceof UpdateColumnComment) {
        UpdateColumnComment updateColumnComment = (UpdateColumnComment) tableChange;
        return updateColumnComment(
            fieldName(updateColumnComment), updateColumnComment.getNewComment());
      } else if (tableChange instanceof UpdateColumnNullability) {
        UpdateColumnNullability updateColumnNullability = (UpdateColumnNullability) tableChange;
        return updateColumnNullability(
            fieldName(updateColumnNullability), updateColumnNullability.nullable());
      } else if (tableChange instanceof UpdateColumnPosition) {
        UpdateColumnPosition updateColumnPosition = (UpdateColumnPosition) tableChange;
        return updateColumnPosition(
            move(fieldName(updateColumnPosition), updateColumnPosition.getPosition()));
      } else if (tableChange instanceof UpdateColumnType) {
        UpdateColumnType updateColumnType = (UpdateColumnType) tableChange;
        return updateColumnType(
            fieldName(updateColumnType), toPaimonType(updateColumnType.getNewDataType()));
      }
    } else if (tableChange instanceof UpdateComment) {
      return updateComment(((UpdateComment) tableChange).getNewComment());
    } else if (tableChange instanceof SetProperty) {
      SetProperty setProperty = ((SetProperty) tableChange);
      return setOption(setProperty.getProperty(), setProperty.getValue());
    } else if (tableChange instanceof RemoveProperty) {
      return removeOption(((RemoveProperty) tableChange).getProperty());
    }
    throw new UnsupportedOperationException(
        String.format(
            "Paimon does not support %s table change.", tableChange.getClass().getSimpleName()));
  }

  public static void checkColumn(String fieldName, Expression defaultValue, boolean autoIncrement) {
    checkColumnDefaultValue(fieldName, defaultValue);
    checkColumnAutoIncrement(fieldName, autoIncrement);
  }

  public static String fieldName(String[] fieldName) {
    return DOT.join(fieldName);
  }

  public static String[] fieldName(String fieldName) {
    return new String[] {fieldName};
  }

  private static String fieldName(ColumnChange columnChange) {
    return fieldName(columnChange.fieldName());
  }

  private static void checkColumnDefaultValue(String fieldName, Expression defaultValue) {
    Preconditions.checkArgument(
        defaultValue.equals(Column.DEFAULT_VALUE_NOT_SET),
        String.format(
            "Paimon does not support column default value. Illegal column: %s.", fieldName));
  }

  private static void checkColumnAutoIncrement(String fieldName, boolean autoIncrement) {
    Preconditions.checkArgument(
        !autoIncrement,
        String.format(
            "Paimon does not support auto increment column. Illegal column: %s.", fieldName));
  }

  private static Move move(String fieldName, ColumnPosition columnPosition)
      throws UnsupportedOperationException {
    if (columnPosition instanceof After) {
      return Move.after(fieldName, ((After) columnPosition).getColumn());
    } else if (columnPosition instanceof Default) {
      return null;
    } else if (columnPosition instanceof First) {
      return Move.first(fieldName);
    }
    throw new UnsupportedOperationException(
        String.format(
            "Paimon does not support %s column position.",
            columnPosition.getClass().getSimpleName()));
  }
}
