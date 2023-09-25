/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.lakehouse.iceberg.ops;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.rel.TableChange.AddColumn;
import com.datastrato.graviton.rel.TableChange.After;
import com.datastrato.graviton.rel.TableChange.ColumnChange;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.substrait.type.Type;
import io.substrait.type.Type.Binary;
import io.substrait.type.Type.I32;
import io.substrait.type.Type.I64;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.ws.rs.NotSupportedException;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
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

  // todo, just for pass the updateTable test, @yunqing will provide a new implement
  private static org.apache.iceberg.types.Type convertType(Type gravitonType) {
    if (gravitonType instanceof I32) {
      return IntegerType.get();
    } else if (gravitonType instanceof I64) {
      return LongType.get();
    } else if (gravitonType instanceof Binary) {
      return StringType.get();
    }
    return StringType.get();
  }

  private void doDeleteColumn(
      UpdateSchema icebergUpdateSchema, DeleteColumn deleteColumn, Schema icebergTableSchema) {
    NestedField deleteField = icebergTableSchema.findField(DOT.join(deleteColumn.fieldNames()));
    if (deleteField == null) {
      if (deleteColumn.getIfExists()) {
        return;
      } else {
        throw new IllegalArgumentException(
            "delete column not exists: " + DOT.join(deleteColumn.fieldNames()));
      }
    }
    icebergUpdateSchema.deleteColumn(DOT.join(deleteColumn.fieldNames()));
  }

  private void doUpdateColumnComment(
      UpdateSchema icebergUpdateSchema, UpdateColumnComment updateColumnComment) {
    icebergUpdateSchema.updateColumnDoc(
        DOT.join(updateColumnComment.fieldNames()), updateColumnComment.getNewComment());
  }

  private void doSetProperty(UpdateProperties icebergUpdateProperties, SetProperty setProperty) {
    icebergUpdateProperties.set(setProperty.getProperty(), setProperty.getValue());
  }

  private void doRemoveProperty(
      UpdateProperties icebergUpdateProperties, RemoveProperty removeProperty) {
    icebergUpdateProperties.remove(removeProperty.getProperty());
  }

  private void doRenameColumn(UpdateSchema icebergUpdateSchema, RenameColumn renameColumn) {
    icebergUpdateSchema.renameColumn(
        DOT.join(renameColumn.fieldNames()), renameColumn.getNewName());
  }

  private void doMoveColumn(
      UpdateSchema icebergUpdateSchema, String[] fieldNames, ColumnPosition columnPosition) {
    if (columnPosition instanceof TableChange.After) {
      After after = (After) columnPosition;
      String peerName = getSiblingName(fieldNames, after.getColumn());
      icebergUpdateSchema.moveAfter(DOT.join(fieldNames), peerName);
    } else if (columnPosition instanceof TableChange.First) {
      icebergUpdateSchema.moveFirst(DOT.join(fieldNames));
    } else {
      throw new NotSupportedException(
          "Iceberg doesn't support column position: " + columnPosition.getClass().getSimpleName());
    }
  }

  private void doUpdateColumnPosition(
      UpdateSchema icebergUpdateSchema, UpdateColumnPosition updateColumnPosition) {
    doMoveColumn(
        icebergUpdateSchema, updateColumnPosition.fieldNames(), updateColumnPosition.getPosition());
  }

  private void doUpdateColumnType(
      UpdateSchema icebergUpdateSchema, UpdateColumnType updateColumnType) {
    org.apache.iceberg.types.Type type = convertType(updateColumnType.getNewDataType());
    Preconditions.checkArgument(
        type.isPrimitiveType(),
        "Cannot update %s, not a primitive type: %s",
        DOT.join(updateColumnType.fieldNames()),
        type);
    icebergUpdateSchema.updateColumn(DOT.join(updateColumnType.fieldNames()), (PrimitiveType) type);
  }

  private ColumnPosition getAddColumnPosition(StructType parent, ColumnPosition columnPosition) {
    if (columnPosition != null) {
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
    // todo(xiaojing) check new column is nullable
    String parentName = getParentName(addColumn.fieldNames());
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

    icebergUpdateSchema.addColumn(
        getParentName(addColumn.fieldNames()),
        getLeafName(addColumn.fieldNames()),
        convertType(addColumn.getDataType()),
        addColumn.getComment());

    ColumnPosition position = getAddColumnPosition(parentStruct, addColumn.getPosition());
    doMoveColumn(icebergUpdateSchema, addColumn.fieldNames(), position);
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
        doUpdateColumnType(icebergUpdateSchema, (UpdateColumnType) change);
      } else if (change instanceof UpdateColumnComment) {
        doUpdateColumnComment(icebergUpdateSchema, (UpdateColumnComment) change);
      } else {
        throw new NotSupportedException(
            "Iceberg doesn't support " + change.getClass().getSimpleName() + " for now");
      }
    }
    icebergUpdateSchema.commit();
  }

  public IcebergTableChange buildIcebergTableChanges(
      NameIdentifier gravitonNameIdentifier, TableChange... tableChanges) {

    TableIdentifier icebergTableIdentifier =
        TableIdentifier.of(
            Namespace.of(gravitonNameIdentifier.namespace().levels()),
            gravitonNameIdentifier.name());

    List<ColumnChange> gravitonColumnChanges = Lists.newArrayList();
    List<TableChange> gravitonPropertyChanges = Lists.newArrayList();
    for (TableChange change : tableChanges) {
      if (change instanceof ColumnChange) {
        gravitonColumnChanges.add((ColumnChange) change);
      } else if (change instanceof UpdateComment) {
        UpdateComment updateComment = (UpdateComment) change;
        gravitonPropertyChanges.add(new SetProperty("comment", updateComment.getNewComment()));
      } else if (change instanceof RemoveProperty) {
        RemoveProperty removeProperty = (RemoveProperty) change;
        Preconditions.checkArgument(
            !IcebergReservedProperties.contains(removeProperty.getProperty()),
            removeProperty.getProperty() + " is not allowed to remove properties");
        gravitonPropertyChanges.add(removeProperty);
      } else if (change instanceof SetProperty) {
        SetProperty setProperty = (SetProperty) change;
        Preconditions.checkArgument(
            !IcebergReservedProperties.contains(setProperty.getProperty()),
            setProperty.getProperty() + " is not allowed to Set properties");
        gravitonPropertyChanges.add(setProperty);
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
    if (!gravitonColumnChanges.isEmpty()) {
      alterTableColumn(
          transaction.updateSchema(), gravitonColumnChanges, icebergBaseTable.schema());
    }

    if (!gravitonPropertyChanges.isEmpty()) {
      alterTableProperty(transaction.updateProperties(), gravitonPropertyChanges);
    }

    return icebergTableChange;
  }

  public static Namespace getIcebergNamespace(NameIdentifier ident) {
    return getIcebergNamespace(ArrayUtils.add(ident.namespace().levels(), ident.name()));
  }

  public static Namespace getIcebergNamespace(String... level) {
    return Namespace.of(level);
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
  static String getSiblingName(String[] fieldNames, String fieldName) {
    if (fieldNames.length > 1) {
      String[] peerNames = Arrays.copyOf(fieldNames, fieldNames.length);
      peerNames[fieldNames.length - 1] = fieldName;
      return DOT.join(peerNames);
    }
    return fieldName;
  }

  @VisibleForTesting
  static Set<String> getIcebergReservedProperties() {
    return IcebergReservedProperties;
  }
}
