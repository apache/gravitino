/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.ops;

import com.datastrato.graviton.catalog.lakehouse.iceberg.utils.IcebergCatalogUtil;
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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.NotSupportedException;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableOps {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableOps.class);

  protected Catalog catalog;
  private SupportsNamespaces asNamespaceCatalog;
  private final String DEFAULT_ICEBERG_CATALOG_TYPE = "memory";

  private Joiner Dot = Joiner.on(".");

  @VisibleForTesting
  private static final Set<String> IcebergBanedProperties =
      ImmutableSet.of(
          "location",
          "comment",
          "current-snapshot-id",
          "cherry-pick-snapshot-id",
          "sort-order",
          "identifier-fields");

  public IcebergTableOps() {
    catalog = IcebergCatalogUtil.loadIcebergCatalog(DEFAULT_ICEBERG_CATALOG_TYPE);
    if (catalog instanceof SupportsNamespaces) {
      asNamespaceCatalog = (SupportsNamespaces) catalog;
    }
  }

  private void validateNamespace(Optional<Namespace> namespace) {
    namespace.ifPresent(
        n ->
            Preconditions.checkArgument(
                n.toString().isEmpty() == false, "Namespace couldn't be empty"));
    if (asNamespaceCatalog == null) {
      throw new NotSupportedException("The underlying catalog doesn't support namespace operation");
    }
  }

  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    validateNamespace(Optional.of(request.namespace()));
    return CatalogHandlers.createNamespace(asNamespaceCatalog, request);
  }

  public void dropNamespace(Namespace namespace) {
    validateNamespace(Optional.of(namespace));
    CatalogHandlers.dropNamespace(asNamespaceCatalog, namespace);
  }

  public GetNamespaceResponse loadNamespace(Namespace namespace) {
    validateNamespace(Optional.of(namespace));
    return CatalogHandlers.loadNamespace(asNamespaceCatalog, namespace);
  }

  public ListNamespacesResponse listNamespace(Namespace parent) {
    validateNamespace(Optional.empty());
    return CatalogHandlers.listNamespaces(asNamespaceCatalog, parent);
  }

  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest) {
    validateNamespace(Optional.of(namespace));
    return CatalogHandlers.updateNamespaceProperties(
        asNamespaceCatalog, namespace, updateNamespacePropertiesRequest);
  }

  public LoadTableResponse createTable(Namespace namespace, CreateTableRequest request) {
    request.validate();
    if (request.stageCreate() == false) {
      return CatalogHandlers.createTable(catalog, namespace, request);
    } else {
      return CatalogHandlers.stageTableCreate(catalog, namespace, request);
    }
  }

  public void dropTable(TableIdentifier tableIdentifier) {
    CatalogHandlers.dropTable(catalog, tableIdentifier);
  }

  public void purgeTable(TableIdentifier tableIdentifier) {
    CatalogHandlers.purgeTable(catalog, tableIdentifier);
  }

  public LoadTableResponse loadTable(TableIdentifier tableIdentifier) {
    return CatalogHandlers.loadTable(catalog, tableIdentifier);
  }

  public boolean tableExists(TableIdentifier tableIdentifier) {
    return catalog.tableExists(tableIdentifier);
  }

  public ListTablesResponse listTable(Namespace namespace) {
    return CatalogHandlers.listTables(catalog, namespace);
  }

  public void renameTable(RenameTableRequest renameTableRequest) {
    CatalogHandlers.renameTable(catalog, renameTableRequest);
  }

  public LoadTableResponse updateTable(
      TableIdentifier tableIdentifier, UpdateTableRequest updateTableRequest) {
    return CatalogHandlers.updateTable(catalog, tableIdentifier, updateTableRequest);
  }

  // to use correct convert types
  private static org.apache.iceberg.types.Type convertType(Type gravitonType) {
    if (gravitonType instanceof I32) {
      return IntegerType.get();
    } else if (gravitonType instanceof Binary) {
      return StringType.get();
    }
    return StringType.get();
  }

  // todo handle struct columns
  private void doDeleteColumn(
      UpdateSchema updateSchema, DeleteColumn deleteColumn, List<String> schemas) {
    boolean isExists = schemas.contains(deleteColumn.fieldNames()[0]);
    if (!isExists) {
      if (deleteColumn.getIfExists()) {
        return;
      } else {
        throw new IllegalArgumentException(
            "delete column not exists: " + Dot.join(deleteColumn.fieldNames()));
      }
    }
    updateSchema.deleteColumn(Dot.join(deleteColumn.fieldNames()));
  }

  private void doUpdateColumnComment(
      UpdateSchema updateSchema, UpdateColumnComment updateColumnComment) {
    updateSchema.updateColumnDoc(
        Dot.join(updateColumnComment.fieldNames()), updateColumnComment.getNewComment());
  }

  private void doSetProperty(UpdateProperties updateProperties, SetProperty setProperty) {
    updateProperties.set(setProperty.getProperty(), setProperty.getValue());
  }

  private void doRemoveProperty(UpdateProperties updateProperties, RemoveProperty removeProperty) {
    updateProperties.remove(removeProperty.getProperty());
  }

  private void doRenameTable(TableIdentifier tableIdentifier, RenameTable renameTable) {
    RenameTableRequest renameTableRequest =
        RenameTableRequest.builder()
            .withSource(tableIdentifier)
            .withDestination(
                TableIdentifier.of(tableIdentifier.namespace(), renameTable.getNewName()))
            .build();
    renameTable(renameTableRequest);
  }

  private void doRenameColumn(UpdateSchema updateSchema, RenameColumn renameColumn) {
    updateSchema.renameColumn(Dot.join(renameColumn.fieldNames()), renameColumn.getNewName());
  }

  private void doMoveColumn(
      UpdateSchema updateSchema, String columnName, ColumnPosition columnPosition) {
    if (columnPosition instanceof TableChange.After) {
      After after = (After) columnPosition;
      updateSchema.moveAfter(columnName, after.getColumn());
    } else if (columnPosition instanceof TableChange.First) {
      updateSchema.moveFirst(columnName);
    } else {
      throw new NotSupportedException("");
    }
  }

  private void doUpdateColumnPosition(
      UpdateSchema updateSchema, UpdateColumnPosition updateColumnPosition) {
    doMoveColumn(
        updateSchema,
        Dot.join(updateColumnPosition.fieldNames()),
        updateColumnPosition.getPosition());
  }

  private void doUpdateColumnType(UpdateSchema updateSchema, UpdateColumnType updateColumnType) {
    org.apache.iceberg.types.Type type = convertType(updateColumnType.getNewDataType());
    Preconditions.checkArgument(
        type.isPrimitiveType(),
        "can't update %s, not a primitive type: %s",
        Dot.join(updateColumnType.fieldNames()),
        type);
    updateSchema.updateColumn(Dot.join(updateColumnType.fieldNames()), (PrimitiveType) type);
  }

  private void doAddColumn(UpdateSchema updateSchema, AddColumn addColumn, List<String> columns) {
    // todo check new column is nullable
    updateSchema.addColumn(
        addColumn.fieldNames()[0], convertType(addColumn.getDataType()), addColumn.getComment());

    if (addColumn.getPosition() == null) {
      if (columns.size() == 0) {
        updateSchema.moveFirst(Dot.join(addColumn.fieldNames()));
      } else {
        updateSchema.moveAfter(Dot.join(addColumn.fieldNames()), columns.get(columns.size() - 1));
      }
    } else if (addColumn.getPosition() instanceof TableChange.After) {
      After after = (After) addColumn.getPosition();
      updateSchema.moveAfter(Dot.join(addColumn.fieldNames()), after.getColumn());
    } else if (addColumn.getPosition() instanceof TableChange.First) {
      updateSchema.moveFirst(Dot.join(addColumn.fieldNames()));
    } else {
      throw new NotSupportedException(
          "Iceberg not support addColumn position: "
              + addColumn.getPosition().getClass().getSimpleName()
              + " for now");
    }
  }

  private void alterTableProperty(
      UpdateProperties updateProperties, List<TableChange> propertyChanges) {
    for (TableChange change : propertyChanges) {
      if (change instanceof RemoveProperty) {
        doRemoveProperty(updateProperties, (RemoveProperty) change);
      } else if (change instanceof SetProperty) {
        doSetProperty(updateProperties, (SetProperty) change);
      } else {
        throw new NotSupportedException(
            "Iceberg not support table change: " + change.getClass() + "for now");
      }
    }
    updateProperties.commit();
  }

  private void alterTableColumn(
      UpdateSchema updateSchema, List<ColumnChange> columnChanges, List<String> columns) {
    for (ColumnChange change : columnChanges) {
      if (change instanceof AddColumn) {
        doAddColumn(updateSchema, (AddColumn) change, columns);
      } else if (change instanceof DeleteColumn) {
        doDeleteColumn(updateSchema, (DeleteColumn) change, columns);
      } else if (change instanceof UpdateColumnPosition) {
        doUpdateColumnPosition(updateSchema, (UpdateColumnPosition) change);
      } else if (change instanceof RenameColumn) {
        doRenameColumn(updateSchema, (RenameColumn) change);
      } else if (change instanceof UpdateColumnType) {
        doUpdateColumnType(updateSchema, (UpdateColumnType) change);
      } else if (change instanceof UpdateColumnComment) {
        doUpdateColumnComment(updateSchema, (UpdateColumnComment) change);
      } else {
        throw new NotSupportedException("Iceberg not support " + change.getClass() + "for now");
      }
    }

    updateSchema.commit();
  }

  public LoadTableResponse alterTable(
      TableIdentifier tableIdentifier, TableChange... tableChanges) {

    List<ColumnChange> columnChanges = Lists.newArrayList();
    List<TableChange> propertyChanges = Lists.newArrayList();
    List<TableChange> otherChanges = Lists.newArrayList();
    for (TableChange change : tableChanges) {
      if (change instanceof ColumnChange) {
        columnChanges.add((ColumnChange) change);
      } else if (change instanceof UpdateComment) {
        UpdateComment updateComment = (UpdateComment) change;
        propertyChanges.add(new SetProperty("comment", updateComment.getNewComment()));
      } else if (change instanceof RemoveProperty) {
        RemoveProperty removeProperty = (RemoveProperty) change;
        Preconditions.checkArgument(
            !IcebergBanedProperties.contains(removeProperty.getProperty()),
            removeProperty.getProperty() + " is not allowed to remove properties");
        propertyChanges.add(removeProperty);
      } else if (change instanceof SetProperty) {
        SetProperty setProperty = (SetProperty) change;
        Preconditions.checkArgument(
            !IcebergBanedProperties.contains(setProperty.getProperty()),
            setProperty.getProperty() + " is not allowed to Set properties");
        propertyChanges.add(setProperty);
      } else if (change instanceof RenameTable) {
        otherChanges.add(change);
      } else {
        throw new NotSupportedException("Iceberg not support " + change.getClass() + "for now");
      }
    }

    for (TableChange change : otherChanges) {
      if (change instanceof RenameTable) {
        RenameTable renameTable = (RenameTable) change;
        doRenameTable(tableIdentifier, renameTable);
        return loadTable(TableIdentifier.of(tableIdentifier.namespace(), renameTable.getNewName()));
      } else {
        throw new NotSupportedException("Iceberg not support " + change.getClass() + "for now");
      }
    }

    if (columnChanges.isEmpty() && propertyChanges.isEmpty()) {
      return loadTable(tableIdentifier);
    }

    Table table = catalog.loadTable(tableIdentifier);
    Transaction transaction = table.newTransaction();

    if (!columnChanges.isEmpty()) {
      List<String> tableColumns =
          table.schema().columns().stream().map(NestedField::name).collect(Collectors.toList());
      alterTableColumn(transaction.updateSchema(), columnChanges, tableColumns);
    }

    if (!propertyChanges.isEmpty()) {
      alterTableProperty(transaction.updateProperties(), propertyChanges);
    }

    transaction.commitTransaction();

    return loadTable(tableIdentifier);
  }

  @VisibleForTesting
  public static Set<String> getIcebergBanedProperties() {
    return IcebergBanedProperties;
  }
}
