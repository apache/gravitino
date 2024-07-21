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

import com.google.common.base.Preconditions;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.Optional;
import javax.ws.rs.NotSupportedException;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import org.apache.gravitino.catalog.lakehouse.iceberg.ops.IcebergTableOpsHelper.IcebergTableChange;
import org.apache.gravitino.catalog.lakehouse.iceberg.utils.IcebergCatalogUtil;
import org.apache.gravitino.utils.IsolatedClassLoader;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableOps implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(IcebergTableOps.class);

  protected Catalog catalog;
  private SupportsNamespaces asNamespaceCatalog;
  private final String catalogType;
  private String catalogUri = null;

  public IcebergTableOps(IcebergConfig icebergConfig) {
    this.catalogType = icebergConfig.get(IcebergConfig.CATALOG_BACKEND);
    if (!IcebergCatalogBackend.MEMORY.name().equalsIgnoreCase(catalogType)) {
      icebergConfig.get(IcebergConfig.CATALOG_WAREHOUSE);
      this.catalogUri = icebergConfig.get(IcebergConfig.CATALOG_URI);
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
      // JdbcCatalog and WrappedHiveCatalog need close.
      ((AutoCloseable) catalog).close();
    }

    // Because each catalog in Gravitino has its own classloader, after a catalog is no longer used
    // for a long time or dropped, the instance of classloader needs to be released. In order to
    // let JVM GC remove the classloader, we need to release the resources of the classloader. The
    // resources include the driver of the catalog backend and the
    // AbandonedConnectionCleanupThread of MySQL. For more information about
    // AbandonedConnectionCleanupThread, please refer to the corresponding java doc of MySQL
    // driver.
    if (catalogUri != null && catalogUri.contains("mysql")) {
      closeMySQLCatalogResource();
    } else if (catalogUri != null && catalogUri.contains("postgresql")) {
      closePostgreSQLCatalogResource();
    } else if (catalogType.equalsIgnoreCase(IcebergCatalogBackend.HIVE.name())) {
      // TODO(yuqi) add close for other catalog types such Hive catalog, for more, please refer to
      // https://github.com/apache/gravitino/pull/2548/commits/ab876b69b7e094bbd8c174d48a2365a18ed5176d
    }
  }

  private void closeMySQLCatalogResource() {
    try {
      // Close thread AbandonedConnectionCleanupThread if we are using `com.mysql.cj.jdbc.Driver`,
      // for driver `com.mysql.jdbc.Driver` (deprecated), the daemon thead maybe not this one.
      Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread")
          .getMethod("uncheckedShutdown")
          .invoke(null);
      LOG.info("AbandonedConnectionCleanupThread has been shutdown...");

      // Unload the MySQL driver, only Unload the driver if it is loaded by
      // IsolatedClassLoader.
      closeDriverLoadedByIsolatedClassLoader(catalogUri);
    } catch (Exception e) {
      LOG.warn("Failed to shutdown AbandonedConnectionCleanupThread or deregister MySQL driver", e);
    }
  }

  private void closeDriverLoadedByIsolatedClassLoader(String uri) {
    try {
      Driver driver = DriverManager.getDriver(uri);
      if (driver.getClass().getClassLoader().getClass()
          == IsolatedClassLoader.CUSTOM_CLASS_LOADER_CLASS) {
        DriverManager.deregisterDriver(driver);
        LOG.info("Driver {} has been deregistered...", driver);
      }
    } catch (Exception e) {
      LOG.warn("Failed to deregister driver", e);
    }
  }

  private void closePostgreSQLCatalogResource() {
    closeDriverLoadedByIsolatedClassLoader(catalogUri);
  }
}
