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
package org.apache.gravitino.spark.connector;

import com.google.common.base.Preconditions;
import org.apache.spark.sql.connector.catalog.TableChange;

public class SparkTableChangeConverter {
  private SparkTypeConverter sparkTypeConverter;

  public SparkTableChangeConverter(SparkTypeConverter sparkTypeConverter) {
    this.sparkTypeConverter = sparkTypeConverter;
  }

  public org.apache.gravitino.rel.TableChange toGravitinoTableChange(TableChange change) {
    if (change instanceof TableChange.SetProperty) {
      TableChange.SetProperty setProperty = (TableChange.SetProperty) change;
      if (ConnectorConstants.COMMENT.equals(setProperty.property())) {
        return org.apache.gravitino.rel.TableChange.updateComment(setProperty.value());
      } else {
        return org.apache.gravitino.rel.TableChange.setProperty(
            setProperty.property(), setProperty.value());
      }
    } else if (change instanceof TableChange.RemoveProperty) {
      TableChange.RemoveProperty removeProperty = (TableChange.RemoveProperty) change;
      Preconditions.checkArgument(
          !ConnectorConstants.COMMENT.equals(removeProperty.property()),
          "Gravitino doesn't support remove table comment yet");
      return org.apache.gravitino.rel.TableChange.removeProperty(removeProperty.property());
    } else if (change instanceof TableChange.AddColumn) {
      TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
      return org.apache.gravitino.rel.TableChange.addColumn(
          addColumn.fieldNames(),
          sparkTypeConverter.toGravitinoType(addColumn.dataType()),
          addColumn.comment(),
          transformColumnPosition(addColumn.position()),
          addColumn.isNullable());
    } else if (change instanceof TableChange.DeleteColumn) {
      TableChange.DeleteColumn deleteColumn = (TableChange.DeleteColumn) change;
      return org.apache.gravitino.rel.TableChange.deleteColumn(
          deleteColumn.fieldNames(), deleteColumn.ifExists());
    } else if (change instanceof TableChange.UpdateColumnType) {
      TableChange.UpdateColumnType updateColumnType = (TableChange.UpdateColumnType) change;
      return org.apache.gravitino.rel.TableChange.updateColumnType(
          updateColumnType.fieldNames(),
          sparkTypeConverter.toGravitinoType(updateColumnType.newDataType()));
    } else if (change instanceof TableChange.RenameColumn) {
      TableChange.RenameColumn renameColumn = (TableChange.RenameColumn) change;
      return org.apache.gravitino.rel.TableChange.renameColumn(
          renameColumn.fieldNames(), renameColumn.newName());
    } else if (change instanceof TableChange.UpdateColumnPosition) {
      TableChange.UpdateColumnPosition sparkUpdateColumnPosition =
          (TableChange.UpdateColumnPosition) change;
      org.apache.gravitino.rel.TableChange.UpdateColumnPosition gravitinoUpdateColumnPosition =
          (org.apache.gravitino.rel.TableChange.UpdateColumnPosition)
              org.apache.gravitino.rel.TableChange.updateColumnPosition(
                  sparkUpdateColumnPosition.fieldNames(),
                  transformColumnPosition(sparkUpdateColumnPosition.position()));
      Preconditions.checkArgument(
          !(gravitinoUpdateColumnPosition.getPosition()
              instanceof org.apache.gravitino.rel.TableChange.Default),
          "Doesn't support alter column position without specifying position");
      return gravitinoUpdateColumnPosition;
    } else if (change instanceof TableChange.UpdateColumnComment) {
      TableChange.UpdateColumnComment updateColumnComment =
          (TableChange.UpdateColumnComment) change;
      return org.apache.gravitino.rel.TableChange.updateColumnComment(
          updateColumnComment.fieldNames(), updateColumnComment.newComment());
    } else if (change instanceof TableChange.UpdateColumnNullability) {
      TableChange.UpdateColumnNullability updateColumnNullability =
          (TableChange.UpdateColumnNullability) change;
      return org.apache.gravitino.rel.TableChange.updateColumnNullability(
          updateColumnNullability.fieldNames(), updateColumnNullability.nullable());
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported table change %s", change.getClass().getName()));
    }
  }

  private org.apache.gravitino.rel.TableChange.ColumnPosition transformColumnPosition(
      TableChange.ColumnPosition columnPosition) {
    if (null == columnPosition) {
      return org.apache.gravitino.rel.TableChange.ColumnPosition.defaultPos();
    } else if (columnPosition instanceof TableChange.First) {
      return org.apache.gravitino.rel.TableChange.ColumnPosition.first();
    } else if (columnPosition instanceof TableChange.After) {
      TableChange.After after = (TableChange.After) columnPosition;
      return org.apache.gravitino.rel.TableChange.ColumnPosition.after(after.column());
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported table column position %s", columnPosition.getClass().getName()));
    }
  }
}
