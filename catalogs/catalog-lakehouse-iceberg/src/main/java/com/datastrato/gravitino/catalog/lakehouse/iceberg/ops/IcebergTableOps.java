/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.ops;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.ops.IcebergTableOpsHelper.IcebergTableChange;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.utils.IcebergCatalogUtil;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Optional;
import javax.ws.rs.NotSupportedException;
import org.apache.iceberg.Transaction;
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

public class IcebergTableOps implements AutoCloseable {

  protected Catalog catalog;
  private SupportsNamespaces asNamespaceCatalog;

  public IcebergTableOps(IcebergConfig icebergConfig) {
    String catalogType = icebergConfig.get(IcebergConfig.CATALOG_BACKEND);
    if (!IcebergCatalogBackend.MEMORY.name().equalsIgnoreCase(catalogType)) {
      icebergConfig.get(IcebergConfig.CATALOG_WAREHOUSE);
      icebergConfig.get(IcebergConfig.CATALOG_URI);
    }
    catalog = IcebergCatalogUtil.loadCatalogBackend(catalogType, icebergConfig.getAllConfig());
    if (catalog instanceof SupportsNamespaces) {
      asNamespaceCatalog = (SupportsNamespaces) catalog;
    }
  }

  public IcebergTableOps() {
    this(new IcebergConfig(Collections.emptyMap()));
  }

  public IcebergTableOpsHelper createIcebergTableOpsHelper() {
    return new IcebergTableOpsHelper(catalog);
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
    if (request.stageCreate()) {
      return CatalogHandlers.stageTableCreate(catalog, namespace, request);
    }
    return CatalogHandlers.createTable(catalog, namespace, request);
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

  public LoadTableResponse updateTable(IcebergTableChange icebergTableChange) {
    Transaction transaction = icebergTableChange.getTransaction();
    transaction.commitTransaction();
    return loadTable(icebergTableChange.getTableIdentifier());
  }

  @Override
  public void close() throws Exception {
    if (catalog instanceof AutoCloseable) {
      // JdbcCatalog need close.
      ((AutoCloseable) catalog).close();
    }
  }
}
