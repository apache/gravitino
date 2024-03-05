/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.ops;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.ConvertUtil;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.TableChange.AddColumn;
import com.datastrato.gravitino.rel.TableChange.After;
import com.datastrato.gravitino.rel.TableChange.ColumnChange;
import com.datastrato.gravitino.rel.TableChange.ColumnPosition;
import com.datastrato.gravitino.rel.TableChange.DeleteColumn;
import com.datastrato.gravitino.rel.TableChange.RemoveProperty;
import com.datastrato.gravitino.rel.TableChange.RenameColumn;
import com.datastrato.gravitino.rel.TableChange.RenameTable;
import com.datastrato.gravitino.rel.TableChange.SetProperty;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnComment;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnPosition;
import com.datastrato.gravitino.rel.TableChange.UpdateColumnType;
import com.datastrato.gravitino.rel.TableChange.UpdateComment;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.ws.rs.NotSupportedException;
import lombok.Getter;
import lombok.Setter;
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
import org.apache.iceberg.types.Types.StructType;

public class IcebergTableOpsHelper {

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

  public IcebergTableOpsHelper(Catalog icebergCatalog) {
    this.icebergCatalog = icebergCatalog;
  }

  @Getter
  @Setter
  public static final class IcebergTableChange {
    private TableIdentifier tableIdentifier;
    private Transaction transaction;

    IcebergTableChange(TableIdentifier tableIdentifier, Transaction transaction) {
      this.tableIdentifier = tableIdentifier;
      this.transaction = transaction;
    }
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
      throw new NotSupportedException(
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

    boolean nullable = icebergTableSchema.findField(fieldName).isOptional();
    org.apache.iceberg.types.Type type =
        ConvertUtil.toIcebergType(nullable, updateColumnType.getNewDataType());
    Preconditions.checkArgument(
        type.isPrimitiveType(), "Cannot update %s, not a primitive type: %s", fieldName, type);
    icebergUpdateSchema.updateColumn(fieldName, (PrimitiveType) type);
  }

  private ColumnPosition getAddColumnPosition(StructType parent, ColumnPosition columnPosition) {
    if (!(columnPosition instanceof TableChange.Default)) {
      return columnPosition;
    }

    List<NestedField> fields = parent.fields();
    // no column, add to first
    if (fields.isEmpty()) {
      return ColumnPosition.first();
    }

    NestedField last = fields.get(fields.size() - 1);
    return ColumnPosition.after(last.name());
  }

  private void doAddColumn(
      UpdateSchema icebergUpdateSchema, AddColumn addColumn, Schema icebergTableSchema) {
    String parentName = getParentName(addColumn.fieldName());
    StructType parentStruct;
    if (parentName != null) {
      org.apache.iceberg.types.Type parent = icebergTableSchema.findType(parentName);
      Preconditions.checkArgument(
          parent != null, "Couldn't find parent field: " + parentName + " in iceberg table");
      Preconditions.checkArgument(
          parent instanceof StructType,
          "Couldn't add column to non-struct field, name:"
              + parentName
              + ", type:"
              + parent.getClass().getSimpleName());
      parentStruct = (StructType) parent;
    } else {
      parentStruct = icebergTableSchema.asStruct();
    }

    if (addColumn.isAutoIncrement()) {
      throw new IllegalArgumentException("Iceberg doesn't support auto increment column");
    }

    if (addColumn.isNullable()) {
      icebergUpdateSchema.addColumn(
          getParentName(addColumn.fieldName()),
          getLeafName(addColumn.fieldName()),
          ConvertUtil.toIcebergType(addColumn.isNullable(), addColumn.getDataType()),
          addColumn.getComment());
    } else {
      // TODO: figure out how to enable users to add required columns
      // icebergUpdateSchema.allowIncompatibleChanges();
      icebergUpdateSchema.addRequiredColumn(
          getParentName(addColumn.fieldName()),
          getLeafName(addColumn.fieldName()),
          ConvertUtil.toIcebergType(addColumn.isNullable(), addColumn.getDataType()),
          addColumn.getComment());
    }

    ColumnPosition position = getAddColumnPosition(parentStruct, addColumn.getPosition());
    doMoveColumn(icebergUpdateSchema, addColumn.fieldName(), position);
  }

  private void alterTableProperty(
      UpdateProperties icebergUpdateProperties, List<TableChange> propertyChanges) {
    for (TableChange change : propertyChanges) {
      if (change instanceof RemoveProperty) {
        doRemoveProperty(icebergUpdateProperties, (RemoveProperty) change);
      } else if (change instanceof SetProperty) {
        doSetProperty(icebergUpdateProperties, (SetProperty) change);
      } else {
        throw new NotSupportedException(
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
        doAddColumn(icebergUpdateSchema, (AddColumn) change, icebergTableSchema);
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
        throw new NotSupportedException(
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
        throw new NotSupportedException("Iceberg doesn't support " + change.getClass() + "for now");
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
  public static Namespace getIcebergNamespace(com.datastrato.gravitino.Namespace namespace) {
    return getIcebergNamespace(namespace.level(namespace.length() - 1));
  }

  public static Namespace getIcebergNamespace(String... level) {
    return Namespace.of(level);
  }

  /**
   * Gravitino only supports tables managed with a single level hierarchy, such as
   * `{namespace}.{table}`, so we need to perform truncation here.
   *
   * @param namespace
   * @param name
   * @return Iceberg TableIdentifier
   */
  public static TableIdentifier buildIcebergTableIdentifier(
      com.datastrato.gravitino.Namespace namespace, String name) {
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
