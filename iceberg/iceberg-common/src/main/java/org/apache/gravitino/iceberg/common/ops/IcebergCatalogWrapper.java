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
package org.apache.gravitino.iceberg.common.ops;

import com.google.common.base.Preconditions;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.gravitino.iceberg.common.cache.TableMetadataCache;
import org.apache.gravitino.iceberg.common.utils.IcebergCatalogUtil;
import org.apache.gravitino.utils.ClassUtils;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.jdbc.JdbcCatalogWithMetadataLocationSupport;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for Iceberg catalog backend, provides the common interface for Iceberg REST server and
 * Gravitino Iceberg catalog.
 */
public class IcebergCatalogWrapper implements AutoCloseable {

  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapper.class);

  private final Object initializationLock = new Object();
  private volatile Catalog catalog;
  private volatile SupportsNamespaces asNamespaceCatalog;
  private final IcebergCatalogBackend catalogBackend;
  private final IcebergConfig icebergConfig;
  private String catalogUri = null;
  private volatile TableMetadataCache metadataCache;
  private final Configuration configuration;

  public IcebergCatalogWrapper(IcebergConfig icebergConfig) {
    this.icebergConfig = icebergConfig;
    this.catalogBackend =
        IcebergCatalogBackend.valueOf(
            icebergConfig.get(IcebergConfig.CATALOG_BACKEND).toUpperCase(Locale.ROOT));
    if (!IcebergCatalogBackend.MEMORY.equals(catalogBackend)
        && !IcebergCatalogBackend.REST.equals(catalogBackend)) {
      // check whether IcebergConfig.CATALOG_WAREHOUSE exists
      if (StringUtils.isBlank(icebergConfig.get(IcebergConfig.CATALOG_WAREHOUSE))) {
        throw new IllegalArgumentException("The 'warehouse' parameter must have a value.");
      }
    }
    if (!IcebergCatalogBackend.MEMORY.equals(catalogBackend)) {
      this.catalogUri = icebergConfig.get(IcebergConfig.CATALOG_URI);
    }
    Map<String, String> catalogPropertiesMap = icebergConfig.getIcebergCatalogProperties();
    this.configuration = FileSystemUtils.createConfiguration(null, catalogPropertiesMap);
  }

  public Catalog getCatalog() {
    Catalog loadedCatalog = catalog;
    if (loadedCatalog != null) {
      return loadedCatalog;
    }

    synchronized (initializationLock) {
      if (catalog == null) {
        catalog = IcebergCatalogUtil.loadCatalogBackend(catalogBackend, icebergConfig);
      }
      return catalog;
    }
  }

  public IcebergConfig getIcebergConfig() {
    return icebergConfig;
  }

  private SupportsNamespaces getNamespaceCatalog() {
    SupportsNamespaces namespaceCatalog = asNamespaceCatalog;
    if (namespaceCatalog != null) {
      return namespaceCatalog;
    }

    synchronized (initializationLock) {
      if (asNamespaceCatalog == null) {
        Catalog loadedCatalog = getCatalog();
        if (loadedCatalog instanceof SupportsNamespaces) {
          asNamespaceCatalog = (SupportsNamespaces) loadedCatalog;
        }
      }
      return asNamespaceCatalog;
    }
  }

  private TableMetadataCache getMetadataCache() {
    TableMetadataCache cache = metadataCache;
    if (cache != null) {
      return cache;
    }

    synchronized (initializationLock) {
      if (metadataCache == null) {
        metadataCache = loadTableMetadataCache(icebergConfig, getCatalog());
      }
      return metadataCache;
    }
  }

  private void validateNamespace(Optional<Namespace> namespace) {
    namespace.ifPresent(
        n -> Preconditions.checkArgument(!n.toString().isEmpty(), "Namespace couldn't be empty"));
    if (getNamespaceCatalog() == null) {
      throw new UnsupportedOperationException(
          "The underlying catalog doesn't support namespace operation");
    }
  }

  private ViewCatalog getViewCatalog() {
    Catalog loadedCatalog = getCatalog();
    if (!(loadedCatalog instanceof ViewCatalog)) {
      throw new UnsupportedOperationException(
          "The underlying catalog '" + loadedCatalog.name() + "' does not support view operations");
    }
    return (ViewCatalog) loadedCatalog;
  }

  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    validateNamespace(Optional.of(request.namespace()));
    return CatalogHandlers.createNamespace(getNamespaceCatalog(), request);
  }

  public void dropNamespace(Namespace namespace) {
    validateNamespace(Optional.of(namespace));
    CatalogHandlers.dropNamespace(getNamespaceCatalog(), namespace);
  }

  public GetNamespaceResponse loadNamespace(Namespace namespace) {
    validateNamespace(Optional.of(namespace));
    return CatalogHandlers.loadNamespace(getNamespaceCatalog(), namespace);
  }

  public boolean namespaceExists(Namespace namespace) {
    validateNamespace(Optional.of(namespace));
    return getNamespaceCatalog().namespaceExists(namespace);
  }

  public ListNamespacesResponse listNamespace(Namespace parent) {
    validateNamespace(Optional.empty());
    return CatalogHandlers.listNamespaces(getNamespaceCatalog(), parent);
  }

  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest) {
    validateNamespace(Optional.of(namespace));
    return CatalogHandlers.updateNamespaceProperties(
        getNamespaceCatalog(), namespace, updateNamespacePropertiesRequest);
  }

  public LoadTableResponse registerTable(Namespace namespace, RegisterTableRequest request) {
    return CatalogHandlers.registerTable(getCatalog(), namespace, request);
  }

  /**
   * Reload hadoop configuration, this is useful when the hadoop configuration UserGroupInformation
   * is shared by multiple threads. UserGroupInformation#authenticationMethod was first initialized
   * in KerberosClient, however, when switching to iceberg-rest thread,
   * UserGroupInformation#authenticationMethod will be reset to the default value; we need to
   * reinitialize it again.
   */
  public void reloadHadoopConf() {
    UserGroupInformation.setConfiguration(configuration);
  }

  public LoadTableResponse createTable(Namespace namespace, CreateTableRequest request) {
    request.validate();
    Catalog loadedCatalog = getCatalog();
    if (request.stageCreate()) {
      return CatalogHandlers.stageTableCreate(loadedCatalog, namespace, request);
    }
    LoadTableResponse loadTableResponse =
        CatalogHandlers.createTable(loadedCatalog, namespace, request);
    if (loadTableResponse != null) {
      TableIdentifier tableIdentifier = TableIdentifier.of(namespace, request.name());
      getMetadataCache().updateTableMetadata(tableIdentifier, loadTableResponse.tableMetadata());
    }
    return loadTableResponse;
  }

  public void dropTable(TableIdentifier tableIdentifier) {
    getMetadataCache().invalidate(tableIdentifier);
    CatalogHandlers.dropTable(getCatalog(), tableIdentifier);
  }

  public void purgeTable(TableIdentifier tableIdentifier) {
    getMetadataCache().invalidate(tableIdentifier);
    CatalogHandlers.purgeTable(getCatalog(), tableIdentifier);
  }

  public LoadTableResponse loadTable(TableIdentifier tableIdentifier) {
    Optional<TableMetadata> tableMetadataOptional =
        getMetadataCache().getTableMetadata(tableIdentifier);
    if (tableMetadataOptional.isPresent()) {
      return LoadTableResponse.builder().withTableMetadata(tableMetadataOptional.get()).build();
    }

    LoadTableResponse loadTableResponse = CatalogHandlers.loadTable(getCatalog(), tableIdentifier);
    if (loadTableResponse != null) {
      getMetadataCache().updateTableMetadata(tableIdentifier, loadTableResponse.tableMetadata());
    }
    return loadTableResponse;
  }

  /**
   * Retrieves the metadata file location for the specified table without loading full table
   * metadata. This is an optional fast path for catalogs that implement {@link
   * SupportsMetadataLocation}.
   *
   * @param tableIdentifier the table identifier
   * @return an Optional containing the metadata file location, or empty if the catalog doesn't
   *     support this operation
   */
  public Optional<String> getTableMetadataLocation(TableIdentifier tableIdentifier) {
    Catalog loadedCatalog = getCatalog();
    if (loadedCatalog instanceof SupportsMetadataLocation) {
      return Optional.ofNullable(
          ((SupportsMetadataLocation) loadedCatalog).metadataLocation(tableIdentifier));
    }
    return Optional.empty();
  }

  public boolean tableExists(TableIdentifier tableIdentifier) {
    return getCatalog().tableExists(tableIdentifier);
  }

  public ListTablesResponse listTable(Namespace namespace) {
    return CatalogHandlers.listTables(getCatalog(), namespace);
  }

  public void renameTable(RenameTableRequest renameTableRequest) {
    getMetadataCache().invalidate(renameTableRequest.source());
    CatalogHandlers.renameTable(getCatalog(), renameTableRequest);
  }

  public LoadTableResponse updateTable(
      TableIdentifier tableIdentifier, UpdateTableRequest updateTableRequest) {
    getMetadataCache().invalidate(tableIdentifier);
    LoadTableResponse loadTableResponse =
        CatalogHandlers.updateTable(getCatalog(), tableIdentifier, updateTableRequest);
    if (loadTableResponse != null) {
      getMetadataCache().updateTableMetadata(tableIdentifier, loadTableResponse.tableMetadata());
    }
    return loadTableResponse;
  }

  public LoadTableResponse updateTable(IcebergTableChange icebergTableChange) {
    Transaction transaction = icebergTableChange.getTransaction();
    transaction.commitTransaction();
    return loadTable(icebergTableChange.getTableIdentifier());
  }

  /**
   * Commits multiple table updates in a best-effort atomic manner using a two-phase
   * validate-then-commit approach:
   *
   * <ol>
   *   <li>Phase 1: Load all tables and validate ALL requirements against current metadata. If any
   *       requirement fails, the entire transaction is rejected before any commit is made.
   *   <li>Phase 2: Apply metadata updates and commit each table. Once phase 1 succeeds, commits are
   *       applied sequentially.
   * </ol>
   *
   * <p>True cross-table atomicity (rollback of already-committed tables) depends on whether the
   * underlying catalog backend supports it. Most Iceberg catalog backends do not provide
   * cross-table rollback, but this two-phase approach ensures that no table is modified if any
   * pre-condition check fails.
   *
   * <p>Each {@link UpdateTableRequest} in the request must include a {@link TableIdentifier}.
   *
   * @param commitTransactionRequest The request containing all table changes to apply.
   */
  public void commitTransaction(CommitTransactionRequest commitTransactionRequest) {
    commitTransactionRequest.validate();
    List<UpdateTableRequest> tableChanges = commitTransactionRequest.tableChanges();

    // Phase 1: validate ALL requirements before committing anything
    List<org.apache.iceberg.TableOperations> allOps = new ArrayList<>(tableChanges.size());
    List<TableMetadata> allBase = new ArrayList<>(tableChanges.size());
    List<TableMetadata> allUpdated = new ArrayList<>(tableChanges.size());

    for (UpdateTableRequest change : tableChanges) {
      Preconditions.checkArgument(
          change.identifier() != null,
          "Invalid table change in transaction: missing table identifier");
      Table table = catalog.loadTable(change.identifier());
      Preconditions.checkArgument(
          table instanceof HasTableOperations,
          "Table does not support operations required for transaction commit: %s",
          change.identifier());

      org.apache.iceberg.TableOperations ops = ((HasTableOperations) table).operations();
      TableMetadata base = ops.current();

      // Validate all requirements against the current metadata — fail fast before any commit
      change.requirements().forEach(req -> req.validate(base));

      // Build the new metadata by applying all updates
      TableMetadata.Builder builder = TableMetadata.buildFrom(base);
      for (MetadataUpdate update : change.updates()) {
        update.applyTo(builder);
      }
      TableMetadata updated = builder.build();

      allOps.add(ops);
      allBase.add(base);
      allUpdated.add(updated);
    }

    // Phase 2: all requirements passed — now commit each table
    for (int i = 0; i < allOps.size(); i++) {
      allOps.get(i).commit(allBase.get(i), allUpdated.get(i));
      metadataCache.updateTableMetadata(tableChanges.get(i).identifier(), allUpdated.get(i));
    }
  }

  public LoadViewResponse createView(Namespace namespace, CreateViewRequest request) {
    request.validate();
    return CatalogHandlers.createView(getViewCatalog(), namespace, request);
  }

  public LoadViewResponse updateView(TableIdentifier viewIdentifier, UpdateTableRequest request) {
    request.validate();
    return CatalogHandlers.updateView(getViewCatalog(), viewIdentifier, request);
  }

  public LoadViewResponse loadView(TableIdentifier viewIdentifier) {
    return CatalogHandlers.loadView(getViewCatalog(), viewIdentifier);
  }

  public void dropView(TableIdentifier viewIdentifier) {
    CatalogHandlers.dropView(getViewCatalog(), viewIdentifier);
  }

  public void renameView(RenameTableRequest request) {
    request.validate();
    CatalogHandlers.renameView(getViewCatalog(), request);
  }

  public boolean viewExists(TableIdentifier viewIdentifier) {
    return getViewCatalog().viewExists(viewIdentifier);
  }

  public ListTablesResponse listView(Namespace namespace) {
    return CatalogHandlers.listViews(getViewCatalog(), namespace);
  }

  public boolean supportsViewOperations() {
    Catalog loadedCatalog = getCatalog();
    if (!(loadedCatalog instanceof ViewCatalog)) {
      return false;
    }

    // JDBC catalog only supports view operations from v1 schema version
    if (loadedCatalog instanceof JdbcCatalogWithMetadataLocationSupport) {
      JdbcCatalogWithMetadataLocationSupport jdbcCatalog =
          (JdbcCatalogWithMetadataLocationSupport) loadedCatalog;
      return jdbcCatalog.supportsViewsWithSchemaVersion();
    }

    return true;
  }

  @Override
  public void close() throws Exception {
    Catalog loadedCatalog = catalog;
    if (loadedCatalog != null) {
      LOG.info("Closing IcebergCatalogWrapper for catalog: {}", loadedCatalog.name());
    } else {
      LOG.info("Closing IcebergCatalogWrapper before catalog is initialized");
    }
    if (loadedCatalog instanceof AutoCloseable) {
      // JdbcCatalog and ClosableHiveCatalog implement AutoCloseable and will handle their own
      // cleanup
      ((AutoCloseable) loadedCatalog).close();
    }
    TableMetadataCache cache = metadataCache;
    if (cache != null) {
      cache.close();
    }

    // For Iceberg REST server which use the same classloader when recreating catalog wrapper, the
    // Driver couldn't be reloaded after deregister()
    if (useDifferentClassLoader()) {
      closeJdbcDriverResources();
    }
  }

  /**
   * Whether the wrapper is recreated with a different classloader.
   *
   * <p>Returning {@code true} allows JDBC drivers loaded by an isolated classloader to be
   * deregistered when the wrapper closes so the classloader can be garbage collected. Implementors
   * that intentionally reuse the same classloader (for example, an Iceberg REST server instance)
   * should override and return {@code false} to skip deregistration.
   */
  protected boolean useDifferentClassLoader() {
    return true;
  }

  private void closeJdbcDriverResources() {
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
    }
  }

  private void closeMySQLCatalogResource() {
    try {
      // Close thread AbandonedConnectionCleanupThread if we are using `com.mysql.cj.jdbc.Driver`,
      // for driver `com.mysql.jdbc.Driver` (deprecated), the daemon thread maybe not this one.
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

  @Getter
  @Setter
  public static final class IcebergTableChange {

    private TableIdentifier tableIdentifier;
    private Transaction transaction;

    public IcebergTableChange(TableIdentifier tableIdentifier, Transaction transaction) {
      this.tableIdentifier = tableIdentifier;
      this.transaction = transaction;
    }
  }

  private TableMetadataCache loadTableMetadataCache(IcebergConfig config, Catalog catalog) {
    String impl = config.get(IcebergConfig.TABLE_METADATA_CACHE_IMPL);
    if (StringUtils.isBlank(impl)) {
      return TableMetadataCache.DUMMY;
    }

    Preconditions.checkArgument(
        catalog instanceof SupportsMetadataLocation,
        "You shouldn't enable Iceberg metadata cache for the catalog %s,"
            + " because the catalog impl does not support get metadata location.",
        catalog.name());

    TableMetadataCache cache =
        ClassUtils.loadAndGetInstance(impl, Thread.currentThread().getContextClassLoader());
    int capacity = config.get(IcebergConfig.TABLE_METADATA_CACHE_CAPACITY);
    int expireMinutes = config.get(IcebergConfig.TABLE_METADATA_CACHE_EXPIRE_MINUTES);
    cache.initialize(
        capacity, expireMinutes, config.getAllConfig(), (SupportsMetadataLocation) catalog);
    LOG.info(
        "Load Iceberg table metadata cache for catalog: {}, impl:{}, capacity: {}, expire minutes: {}",
        catalog.name(),
        impl,
        capacity,
        expireMinutes);
    return cache;
  }
}
