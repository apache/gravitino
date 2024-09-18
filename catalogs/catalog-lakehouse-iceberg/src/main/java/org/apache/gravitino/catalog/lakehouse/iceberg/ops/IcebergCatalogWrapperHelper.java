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

package org.apache.gravitino.catalog.lakehouse.iceberg.ops;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.IcebergDataTypeConverter;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper.IcebergTableChange;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.TableChange.AddColumn;
import org.apache.gravitino.rel.TableChange.After;
import org.apache.gravitino.rel.TableChange.ColumnChange;
import org.apache.gravitino.rel.TableChange.ColumnPosition;
import org.apache.gravitino.rel.TableChange.DeleteColumn;
import org.apache.gravitino.rel.TableChange.RemoveProperty;
import org.apache.gravitino.rel.TableChange.RenameColumn;
import org.apache.gravitino.rel.TableChange.RenameTable;
import org.apache.gravitino.rel.TableChange.SetProperty;
import org.apache.gravitino.rel.TableChange.UpdateColumnComment;
import org.apache.gravitino.rel.TableChange.UpdateColumnPosition;
import org.apache.gravitino.rel.TableChange.UpdateColumnType;
import org.apache.gravitino.rel.TableChange.UpdateComment;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types.NestedField;

public class IcebergCatalogWrapperHelper {
  @VisibleForTesting public static final Joiner DOT = Joiner.on(".");
  private static final Set<String> IcebergReservedProperties =
      ImmutableSet.of(
          "location",
          "comment",
          "current-snapshot-id",
          "cherry-pick-snapshot-id",
          "sort-order",
          "identifier-fields");

  private Catalog icebergCatalog;

  public IcebergCatalogWrapperHelper(Catalog icebergCatalog) {
    this.icebergCatalog = icebergCatalog;
  }

  private void doDeleteColumn(
      UpdateSchema icebergUpdateSchema, DeleteColumn deleteColumn, Schema icebergTableSchema) {
    NestedField deleteField = icebergTableSchema.findField(DOT.join(deleteColumn.fieldName()));
    if (deleteField == null) {
      if (deleteColumn.getIfExists()) {
        return;
      } else {
        throw new IllegalArgumentException(
            "delete column not exists: " + DOT.join(deleteColumn.fieldName()));
      }
    }
    icebergUpdateSchema.deleteColumn(DOT.join(deleteColumn.fieldName()));
  }

  private void doUpdateColumnComment(
      UpdateSchema icebergUpdateSchema, UpdateColumnComment updateColumnComment) {
    icebergUpdateSchema.updateColumnDoc(
        DOT.join(updateColumnComment.fieldName()), updateColumnComment.getNewComment());
  }

  private void doUpdateColumnNullability(
      UpdateSchema icebergUpdateSchema,
      TableChange.UpdateColumnNullability updateColumnNullability) {
    if (updateColumnNullability.nullable()) {
      icebergUpdateSchema.makeColumnOptional(DOT.join(updateColumnNullability.fieldName()));
    } else {
      // TODO: figure out how to enable users to make column required
      // icebergUpdateSchema.allowIncompatibleChanges();
      icebergUpdateSchema.requireColumn(DOT.join(updateColumnNullability.fieldName()));
    }
  }

  private void doSetProperty(UpdateProperties icebergUpdateProperties, SetProperty setProperty) {
    icebergUpdateProperties.set(setProperty.getProperty(), setProperty.getValue());
  }

  private void doRemoveProperty(
      UpdateProperties icebergUpdateProperties, RemoveProperty removeProperty) {
    icebergUpdateProperties.remove(removeProperty.getProperty());
  }

  private void doRenameColumn(UpdateSchema icebergUpdateSchema, RenameColumn renameColumn) {
    icebergUpdateSchema.renameColumn(DOT.join(renameColumn.fieldName()), renameColumn.getNewName());
  }

  private void doMoveColumn(
      UpdateSchema icebergUpdateSchema, String[] fieldName, ColumnPosition columnPosition) {
    if (columnPosition instanceof TableChange.After) {
      After after = (After) columnPosition;
      String peerName = getSiblingName(fieldName, after.getColumn());
      icebergUpdateSchema.moveAfter(DOT.join(fieldName), peerName);
    } else if (columnPosition instanceof TableChange.First) {
      icebergUpdateSchema.moveFirst(DOT.join(fieldName));
    } else {
      throw new UnsupportedOperationException(
          "Iceberg doesn't support column position: " + columnPosition.getClass().getSimpleName());
    }
  }

  private void doUpdateColumnPosition(
      UpdateSchema icebergUpdateSchema, UpdateColumnPosition updateColumnPosition) {
    doMoveColumn(
        icebergUpdateSchema, updateColumnPosition.fieldName(), updateColumnPosition.getPosition());
  }

  private void doUpdateColumnType(
      UpdateSchema icebergUpdateSchema,
      UpdateColumnType updateColumnType,
      Schema icebergTableSchema) {
    String fieldName = DOT.join(updateColumnType.fieldName());
    Preconditions.checkArgument(
        icebergTableSchema.findField(fieldName) != null,
        "Cannot update missing field: %s",
        fieldName);

    icebergTableSchema.findField(fieldName).isOptional();
    org.apache.iceberg.types.Type type =
        IcebergDataTypeConverter.CONVERTER.fromGravitino(updateColumnType.getNewDataType());
    Preconditions.checkArgument(
        type.isPrimitiveType(), "Cannot update %s, not a primitive type: %s", fieldName, type);
    icebergUpdateSchema.updateColumn(fieldName, (PrimitiveType) type);
  }

  private void doAddColumn(UpdateSchema icebergUpdateSchema, AddColumn addColumn) {
    if (addColumn.isAutoIncrement()) {
      throw new IllegalArgumentException("Iceberg doesn't support auto increment column");
    }

    if (addColumn.isNullable()) {
      icebergUpdateSchema.addColumn(
          getParentName(addColumn.fieldName()),
          getLeafName(addColumn.fieldName()),
          IcebergDataTypeConverter.CONVERTER.fromGravitino(addColumn.getDataType()),
          addColumn.getComment());
    } else {
      // TODO: figure out how to enable users to add required columns
      // icebergUpdateSchema.allowIncompatibleChanges();
      icebergUpdateSchema.addRequiredColumn(
          getParentName(addColumn.fieldName()),
          getLeafName(addColumn.fieldName()),
          IcebergDataTypeConverter.CONVERTER.fromGravitino(addColumn.getDataType()),
          addColumn.getComment());
    }

    if (!ColumnPosition.defaultPos().equals(addColumn.getPosition())) {
      doMoveColumn(icebergUpdateSchema, addColumn.fieldName(), addColumn.getPosition());
    }
  }

  private void alterTableProperty(
      UpdateProperties icebergUpdateProperties, List<TableChange> propertyChanges) {
    for (TableChange change : propertyChanges) {
      if (change instanceof RemoveProperty) {
        doRemoveProperty(icebergUpdateProperties, (RemoveProperty) change);
      } else if (change instanceof SetProperty) {
        doSetProperty(icebergUpdateProperties, (SetProperty) change);
      } else {
        throw new UnsupportedOperationException(
            "Iceberg doesn't support table change: "
                + change.getClass().getSimpleName()
                + " for now");
      }
    }
    icebergUpdateProperties.commit();
  }

  private void alterTableColumn(
      UpdateSchema icebergUpdateSchema,
      List<ColumnChange> columnChanges,
      Schema icebergTableSchema) {
    for (ColumnChange change : columnChanges) {
      if (change instanceof AddColumn) {
        doAddColumn(icebergUpdateSchema, (AddColumn) change);
      } else if (change instanceof DeleteColumn) {
        doDeleteColumn(icebergUpdateSchema, (DeleteColumn) change, icebergTableSchema);
      } else if (change instanceof UpdateColumnPosition) {
        doUpdateColumnPosition(icebergUpdateSchema, (UpdateColumnPosition) change);
      } else if (change instanceof RenameColumn) {
        doRenameColumn(icebergUpdateSchema, (RenameColumn) change);
      } else if (change instanceof UpdateColumnType) {
        doUpdateColumnType(icebergUpdateSchema, (UpdateColumnType) change, icebergTableSchema);
      } else if (change instanceof UpdateColumnComment) {
        doUpdateColumnComment(icebergUpdateSchema, (UpdateColumnComment) change);
      } else if (change instanceof TableChange.UpdateColumnNullability) {
        doUpdateColumnNullability(
            icebergUpdateSchema, (TableChange.UpdateColumnNullability) change);
      } else if (change instanceof TableChange.UpdateColumnAutoIncrement) {
        throw new IllegalArgumentException("Iceberg doesn't support auto increment column");
      } else {
        throw new UnsupportedOperationException(
            "Iceberg doesn't support " + change.getClass().getSimpleName() + " for now");
      }
    }
    icebergUpdateSchema.commit();
  }

  public IcebergTableChange buildIcebergTableChanges(
      NameIdentifier gravitinoNameIdentifier, TableChange... tableChanges) {

    TableIdentifier icebergTableIdentifier =
        TableIdentifier.of(
            Namespace.of(gravitinoNameIdentifier.namespace().levels()),
            gravitinoNameIdentifier.name());

    List<ColumnChange> gravitinoColumnChanges = Lists.newArrayList();
    List<TableChange> gravitinoPropertyChanges = Lists.newArrayList();
    for (TableChange change : tableChanges) {
      if (change instanceof ColumnChange) {
        gravitinoColumnChanges.add((ColumnChange) change);
      } else if (change instanceof UpdateComment) {
        UpdateComment updateComment = (UpdateComment) change;
        gravitinoPropertyChanges.add(new SetProperty("comment", updateComment.getNewComment()));
      } else if (change instanceof RemoveProperty) {
        RemoveProperty removeProperty = (RemoveProperty) change;
        Preconditions.checkArgument(
            !IcebergReservedProperties.contains(removeProperty.getProperty()),
            removeProperty.getProperty() + " is not allowed to remove properties");
        gravitinoPropertyChanges.add(removeProperty);
      } else if (change instanceof SetProperty) {
        SetProperty setProperty = (SetProperty) change;
        Preconditions.checkArgument(
            !IcebergReservedProperties.contains(setProperty.getProperty()),
            setProperty.getProperty() + " is not allowed to Set properties");
        gravitinoPropertyChanges.add(setProperty);
      } else if (change instanceof RenameTable) {
        throw new RuntimeException("RenameTable shouldn't use tableUpdate interface");
      } else {
        throw new UnsupportedOperationException(
            "Iceberg doesn't support " + change.getClass() + "for now");
      }
    }

    Table icebergBaseTable = icebergCatalog.loadTable(icebergTableIdentifier);
    Transaction transaction = icebergBaseTable.newTransaction();
    IcebergTableChange icebergTableChange =
        new IcebergTableChange(icebergTableIdentifier, transaction);
    if (!gravitinoColumnChanges.isEmpty()) {
      alterTableColumn(
          transaction.updateSchema(), gravitinoColumnChanges, icebergBaseTable.schema());
    }

    if (!gravitinoPropertyChanges.isEmpty()) {
      alterTableProperty(transaction.updateProperties(), gravitinoPropertyChanges);
    }

    return icebergTableChange;
  }

  /**
   * Gravitino only supports single-level namespace storage management, which differs from Iceberg.
   * Therefore, we need to handle this difference here.
   *
   * @param namespace GravitinoNamespace
   * @return Iceberg Namespace
   */
  public static Namespace getIcebergNamespace(org.apache.gravitino.Namespace namespace) {
    return getIcebergNamespace(namespace.level(namespace.length() - 1));
  }

  public static Namespace getIcebergNamespace(String... level) {
    return Namespace.of(level);
  }

  /**
   * Gravitino only supports tables managed with a single level hierarchy, such as
   * `{namespace}.{table}`, so we need to perform truncation here.
   *
   * @param namespace The Gravitino name space
   * @param name The table name
   * @return Iceberg TableIdentifier
   */
  public static TableIdentifier buildIcebergTableIdentifier(
      org.apache.gravitino.Namespace namespace, String name) {
    String[] levels = namespace.levels();
    return TableIdentifier.of(levels[levels.length - 1], name);
  }

  /**
   * Gravitino only supports tables managed with a single level hierarchy, such as
   * `{namespace}.{table}`, so we need to perform truncation here.
   *
   * @param nameIdentifier GravitinoNameIdentifier
   * @return Iceberg TableIdentifier
   */
  public static TableIdentifier buildIcebergTableIdentifier(NameIdentifier nameIdentifier) {
    String[] levels = nameIdentifier.namespace().levels();
    return TableIdentifier.of(levels[levels.length - 1], nameIdentifier.name());
  }

  @VisibleForTesting
  static String getParentName(String[] fields) {
    if (fields.length > 1) {
      return DOT.join(Arrays.copyOfRange(fields, 0, fields.length - 1));
    }
    return null;
  }

  @VisibleForTesting
  static String getLeafName(String[] fields) {
    Preconditions.checkArgument(
        fields.length > 0, "Invalid field name: at least one name is required");
    return fields[fields.length - 1];
  }

  @VisibleForTesting
  static String getSiblingName(String[] originalField, String targetField) {
    if (originalField.length > 1) {
      String[] peerNames = Arrays.copyOf(originalField, originalField.length);
      peerNames[originalField.length - 1] = targetField;
      return DOT.join(peerNames);
    }
    return targetField;
  }

  @VisibleForTesting
  static Set<String> getIcebergReservedProperties() {
    return IcebergReservedProperties;
  }
}
