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

import static org.apache.gravitino.catalog.lakehouse.paimon.utils.TypeUtils.toPaimonType;
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.catalog.lakehouse.paimon.ops.PaimonCatalogOps;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.TableChange.AddColumn;
import org.apache.gravitino.rel.TableChange.After;
import org.apache.gravitino.rel.TableChange.ColumnChange;
import org.apache.gravitino.rel.TableChange.ColumnPosition;
import org.apache.gravitino.rel.TableChange.Default;
import org.apache.gravitino.rel.TableChange.DeleteColumn;
import org.apache.gravitino.rel.TableChange.First;
import org.apache.gravitino.rel.TableChange.RemoveProperty;
import org.apache.gravitino.rel.TableChange.RenameColumn;
import org.apache.gravitino.rel.TableChange.SetProperty;
import org.apache.gravitino.rel.TableChange.UpdateColumnComment;
import org.apache.gravitino.rel.TableChange.UpdateColumnNullability;
import org.apache.gravitino.rel.TableChange.UpdateColumnPosition;
import org.apache.gravitino.rel.TableChange.UpdateColumnType;
import org.apache.gravitino.rel.TableChange.UpdateComment;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaChange.Move;

/** Utilities of {@link PaimonCatalogOps} to support table operation. */
public class TableOpsUtils {

  public static final Joiner DOT = Joiner.on(".");

  public static void checkColumnCapability(
      String fieldName, Expression defaultValue, boolean autoIncrement) {
    checkColumnDefaultValue(fieldName, defaultValue);
    checkColumnAutoIncrement(fieldName, autoIncrement);
  }

  public static List<SchemaChange> buildSchemaChanges(TableChange... tableChanges)
      throws UnsupportedOperationException {
    List<SchemaChange> schemaChanges = new ArrayList<>();
    for (TableChange tableChange : tableChanges) {
      schemaChanges.add(buildSchemaChange(tableChange));
    }
    return schemaChanges;
  }

  public static SchemaChange buildSchemaChange(TableChange tableChange)
      throws UnsupportedOperationException {
    if (tableChange instanceof ColumnChange) {
      if (tableChange instanceof AddColumn) {
        AddColumn addColumn = (AddColumn) tableChange;
        String fieldName = getfieldName(addColumn);
        checkColumnCapability(fieldName, addColumn.getDefaultValue(), addColumn.isAutoIncrement());
        return addColumn(
            fieldName,
            toPaimonType(addColumn.getDataType()).copy(addColumn.isNullable()),
            addColumn.getComment(),
            move(fieldName, addColumn.getPosition()));
      } else if (tableChange instanceof DeleteColumn) {
        return dropColumn(getfieldName((DeleteColumn) tableChange));
      } else if (tableChange instanceof RenameColumn) {
        RenameColumn renameColumn = ((RenameColumn) tableChange);
        return renameColumn(getfieldName(renameColumn), renameColumn.getNewName());
      } else if (tableChange instanceof UpdateColumnComment) {
        UpdateColumnComment updateColumnComment = (UpdateColumnComment) tableChange;
        return updateColumnComment(
            getfieldName(updateColumnComment), updateColumnComment.getNewComment());
      } else if (tableChange instanceof UpdateColumnNullability) {
        UpdateColumnNullability updateColumnNullability = (UpdateColumnNullability) tableChange;
        return updateColumnNullability(
            getfieldName(updateColumnNullability), updateColumnNullability.nullable());
      } else if (tableChange instanceof UpdateColumnPosition) {
        UpdateColumnPosition updateColumnPosition = (UpdateColumnPosition) tableChange;
        Preconditions.checkArgument(
            !(updateColumnPosition.getPosition() instanceof Default),
            "Default position is not supported for Paimon update column position.");
        return updateColumnPosition(
            move(getfieldName(updateColumnPosition), updateColumnPosition.getPosition()));
      } else if (tableChange instanceof UpdateColumnType) {
        UpdateColumnType updateColumnType = (UpdateColumnType) tableChange;
        return updateColumnType(
            getfieldName(updateColumnType), toPaimonType(updateColumnType.getNewDataType()));
      }
    } else if (tableChange instanceof UpdateComment) {
      return updateComment(((UpdateComment) tableChange).getNewComment());
    } else if (tableChange instanceof SetProperty) {
      SetProperty setProperty = ((SetProperty) tableChange);
      return setOption(setProperty.getProperty(), setProperty.getValue());
    } else if (tableChange instanceof RemoveProperty) {
      RemoveProperty removeProperty = (RemoveProperty) tableChange;
      return removeOption(removeProperty.getProperty());
    }
    throw new UnsupportedOperationException(
        String.format(
            "Paimon does not support %s table change.", tableChange.getClass().getSimpleName()));
  }

  private static void checkColumnDefaultValue(String fieldName, Expression defaultValue) {
    Preconditions.checkArgument(
        defaultValue.equals(Column.DEFAULT_VALUE_NOT_SET),
        String.format(
            "Paimon set column default value through table properties instead of column info. Illegal column: %s.",
            fieldName));
  }

  private static void checkColumnAutoIncrement(String fieldName, boolean autoIncrement) {
    Preconditions.checkArgument(
        !autoIncrement,
        String.format(
            "Paimon does not support auto increment column. Illegal column: %s.", fieldName));
  }

  private static void checkNestedColumn(String[] fieldNames) {
    Preconditions.checkArgument(
        fieldNames.length == 1,
        String.format(
            "Paimon does not support update nested column. Illegal column: %s.",
            getfieldName(fieldNames)));
  }

  public static String[] getfieldName(String fieldName) {
    return new String[] {fieldName};
  }

  public static String getfieldName(String[] fieldName) {
    return DOT.join(fieldName);
  }

  private static String getfieldName(ColumnChange columnChange) {
    return getfieldName(columnChange.fieldName());
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
