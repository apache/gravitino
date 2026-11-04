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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.gravitino.iceberg.common.cache.TableMetadataCache;
import org.apache.gravitino.iceberg.common.utils.IcebergCatalogUtil;
import org.apache.gravitino.utils.ClassUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.jdbc.JdbcCatalogWithMetadataLocationSupport;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogProperties.SnapshotMode;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ImmutableLoadViewResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for Iceberg catalog backend, provides the common interface for Iceberg REST server and
 * Gravitino Iceberg catalog.
 */
public class IcebergCatalogWrapper implements AutoCloseable {

  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapper.class);

  /** Format versions a table can be created with: the versions the Iceberg table spec defines. */
  private static final int MIN_FORMAT_VERSION = 1;

  private static final int MAX_FORMAT_VERSION = 3;

  private final Object initializationLock = new Object();
  private volatile Catalog catalog;
  private volatile SupportsNamespaces asNamespaceCatalog;
  private final IcebergCatalogBackend catalogBackend;
  private final IcebergConfig icebergConfig;
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

  /**
   * Invalidates cached metadata before a subclass changes or removes a table.
   *
   * @param tableIdentifier the table whose cached metadata must be invalidated
   */
  protected void invalidateTableMetadata(TableIdentifier tableIdentifier) {
    getMetadataCache().invalidate(tableIdentifier);
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
    try {
      return CatalogHandlers.registerTable(getCatalog(), namespace, request);
    } catch (NotFoundException e) {
      // The metadata location comes from the request, so a file that does not exist is a client
      // error. 404 is reserved by the REST spec for a namespace that does not exist.
      throw new BadRequestException(
          "Cannot register table %s: metadata location %s does not exist",
          request.name(), request.metadataLocation());
    }
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
    validateTableDefinition(request);
    Catalog loadedCatalog = getCatalog();
    LoadTableResponse loadTableResponse;
    try {
      if (request.stageCreate()) {
        return CatalogHandlers.stageTableCreate(loadedCatalog, namespace, request);
      }
      loadTableResponse = CatalogHandlers.createTable(loadedCatalog, namespace, request);
    } catch (UnsupportedOperationException e) {
      // Iceberg reports an invalid table definition, such as a transform the column type does
      // not support, as UnsupportedOperationException. createTable has no 406 response.
      throw new BadRequestException("Invalid table definition: %s", e.getMessage());
    } catch (IllegalStateException e) {
      // With the format version taken from the catalog defaults, a column type that version does
      // not support is only detected while Iceberg builds the table metadata.
      if (e.getMessage() != null && e.getMessage().startsWith("Invalid schema for v")) {
        throw new BadRequestException("%s", e.getMessage());
      }
      throw e;
    }
    if (loadTableResponse != null) {
      TableIdentifier tableIdentifier = TableIdentifier.of(namespace, request.name());
      getMetadataCache().updateTableMetadata(tableIdentifier, loadTableResponse.tableMetadata());
    }
    return loadTableResponse;
  }

  public void dropTable(TableIdentifier tableIdentifier) {
    invalidateTableMetadata(tableIdentifier);
    CatalogHandlers.dropTable(getCatalog(), tableIdentifier);
  }

  public void purgeTable(TableIdentifier tableIdentifier) {
    invalidateTableMetadata(tableIdentifier);
    Table table = getCatalog().loadTable(tableIdentifier);
    if (!(table instanceof HasTableOperations)) {
      CatalogHandlers.purgeTable(getCatalog(), tableIdentifier);
      return;
    }
    // Iceberg drops the catalog entry first and then deletes the files, giving up on the first
    // file it cannot read (for example a missing manifest list). The request then fails although
    // the table is gone, and no request is left that could delete the files. Read the metadata
    // first, drop the entry, then delete every file that can still be reached.
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata metadata = ops.current();
    FileIO io = ops.io();
    if (!getCatalog().dropTable(tableIdentifier, false)) {
      throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
    }
    try {
      CatalogUtil.dropTableData(io, metadata);
    } catch (RuntimeException e) {
      LOG.warn(
          "Purge of {} could not read all table files ({}); deleting the files that can be read",
          tableIdentifier,
          e.getMessage());
      IcebergPurgeUtil.deleteReachableFiles(io, metadata);
    }
  }

  /**
   * Loads current {@link TableMetadata}, bypassing the response cache. Used by the async cleanup
   * path to snapshot the metadata location before dropping the catalog entry.
   *
   * @param tableIdentifier the table
   * @return its metadata
   */
  public TableMetadata loadTableMetadata(TableIdentifier tableIdentifier) {
    return ((BaseTable) getCatalog().loadTable(tableIdentifier)).operations().current();
  }

  /**
   * Loads table metadata directly from a metadata file location.
   *
   * @param metadataLocation metadata file location
   * @return parsed table metadata
   */
  public TableMetadata loadTableMetadataFromLocation(String metadataLocation) {
    try (FileIO fileIO = CatalogUtil.loadFileIO(fileIOImpl(), fileIOProperties(), null)) {
      return TableMetadataParser.read(fileIO, metadataLocation);
    }
  }

  /**
   * Returns the FileIO implementation configured for this catalog.
   *
   * @return the {@code io-impl} class, or the Iceberg default when unset
   */
  public String fileIOImpl() {
    String impl = icebergConfig.get(IcebergConfig.IO_IMPL);
    return StringUtils.isNotBlank(impl) ? impl : ResolvingFileIO.class.getName();
  }

  /**
   * Returns catalog properties used to reconstruct FileIO in a cleanup worker.
   *
   * @return catalog properties snapshotted at enqueue time
   */
  public Map<String, String> fileIOProperties() {
    return getIcebergConfig().getIcebergCatalogProperties();
  }

  public LoadTableResponse loadTable(TableIdentifier tableIdentifier) {
    Optional<TableMetadata> tableMetadataOptional =
        getMetadataCache().getTableMetadata(tableIdentifier);
    if (tableMetadataOptional.isPresent()) {
      return LoadTableResponse.builder().withTableMetadata(tableMetadataOptional.get()).build();
    }

    LoadTableResponse loadTableResponse =
        CatalogHandlers.loadTable(getCatalog(), tableIdentifier, SnapshotMode.ALL);
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

  /**
   * Returns whether the identifier resolves to a metadata table rather than to a table with that
   * name. For example, {@code db.t.files} resolves to the files table of {@code db.t} when the
   * namespace {@code db.t} has no table {@code files}.
   *
   * @param tableIdentifier the table identifier
   * @return true if loading the identifier returns a metadata table
   * @throws NoSuchTableException if neither exists
   */
  public boolean resolvesToMetadataTable(TableIdentifier tableIdentifier) {
    return getCatalog().loadTable(tableIdentifier) instanceof BaseMetadataTable;
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
    validateDefaultSpecAndSortOrder(tableIdentifier, updateTableRequest);
    LoadTableResponse loadTableResponse;
    try {
      loadTableResponse =
          CatalogHandlers.updateTable(getCatalog(), tableIdentifier, updateTableRequest);
    } catch (UnsupportedOperationException e) {
      // As in createTable: an invalid update, not an unsupported operation. updateTable has no 406.
      throw new BadRequestException("Invalid table update: %s", e.getMessage());
    }
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

  public LoadViewResponse createView(Namespace namespace, CreateViewRequest request) {
    request.validate();
    return CatalogHandlers.createView(getViewCatalog(), namespace, request);
  }

  public LoadViewResponse registerView(Namespace namespace, RegisterViewRequest request) {
    request.validate();
    return CatalogHandlers.registerView(getViewCatalog(), namespace, request);
  }

  public LoadViewResponse updateView(TableIdentifier viewIdentifier, UpdateTableRequest request) {
    request.validate();
    return CatalogHandlers.updateView(getViewCatalog(), viewIdentifier, request);
  }

  /**
   * Loads a view response from one metadata snapshot.
   *
   * @param viewIdentifier the view to load
   * @return the view metadata and its location
   * @throws NoSuchViewException if the view does not exist or disappears during loading
   */
  public LoadViewResponse loadView(TableIdentifier viewIdentifier) {
    View view = getViewCatalog().loadView(viewIdentifier);
    Preconditions.checkState(
        view instanceof BaseView, "Cannot wrap catalog that does not produce BaseView");
    ViewMetadata metadata = ((BaseView) view).operations().current();
    if (metadata == null) {
      throw new NoSuchViewException("View does not exist: %s", viewIdentifier);
    }
    // CatalogHandlers' private viewResponse helper assumes non-null metadata. A native rename
    // can remove the view after loadView's existence check, so build the same response from the
    // checked snapshot rather than loading it again through the handler.
    return ImmutableLoadViewResponse.builder()
        .metadata(metadata)
        .metadataLocation(metadata.metadataFileLocation())
        .build();
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
    boolean internedMemoryCatalog =
        catalogBackend == IcebergCatalogBackend.MEMORY
            && icebergConfig.getAllConfig().containsKey(IcebergConstants.CATALOG_UUID);
    if (!internedMemoryCatalog && loadedCatalog instanceof AutoCloseable) {
      // JdbcCatalog and ClosableHiveCatalog implement AutoCloseable and will handle their own
      // cleanup
      ((AutoCloseable) loadedCatalog).close();
    }
    TableMetadataCache cache = metadataCache;
    if (cache != null) {
      cache.close();
    }
  }

  public boolean isRESTCatalog() {
    return getCatalog() instanceof RESTCatalog;
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

    if (!(catalog instanceof SupportsMetadataLocation)) {
      LOG.warn(
          "Catalog '{}' does not support the table metadata cache, because the catalog impl does not "
              + "support get metadata location. The cache is disabled. Set '{}' to an empty string "
              + "(\"\") in the configuration before the next restart.",
          catalog.name(),
          IcebergConfig.TABLE_METADATA_CACHE_IMPL.getKey());
      return TableMetadataCache.DUMMY;
    }

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

  /**
   * Rejects a table definition Iceberg would otherwise accept or fail on with a server error: a
   * format version that is not a number or not a defined version, and column types the format
   * version does not support.
   */
  @VisibleForTesting
  static void validateTableDefinition(CreateTableRequest request) {
    Map<String, String> properties = request.properties();
    String value = properties == null ? null : properties.get(TableProperties.FORMAT_VERSION);
    if (value == null) {
      // The catalog's table defaults decide the version; Iceberg checks the schema against it.
      return;
    }
    int formatVersion;
    try {
      formatVersion = Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new BadRequestException(
          "Invalid %s '%s': must be an integer", TableProperties.FORMAT_VERSION, value);
    }
    if (formatVersion < MIN_FORMAT_VERSION || formatVersion > MAX_FORMAT_VERSION) {
      throw new BadRequestException(
          "Unsupported %s %s: must be between %s and %s",
          TableProperties.FORMAT_VERSION, formatVersion, MIN_FORMAT_VERSION, MAX_FORMAT_VERSION);
    }
    if (request.schema() != null) {
      try {
        Schema.checkCompatibility(request.schema(), formatVersion);
      } catch (IllegalStateException | IllegalArgumentException e) {
        throw new BadRequestException("%s", e.getMessage());
      }
    }
  }

  /**
   * Rejects set-default-spec and set-default-sort-order updates naming an id the table does not
   * have; Iceberg applies them unchecked and then fails with a NullPointerException.
   */
  private void validateDefaultSpecAndSortOrder(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {
    if (request == null || request.updates() == null) {
      return;
    }
    Set<Integer> specIds = new HashSet<>();
    Set<Integer> sortOrderIds = new HashSet<>();
    boolean addsSortOrder = false;
    for (MetadataUpdate update : request.updates()) {
      if (update instanceof MetadataUpdate.SetDefaultPartitionSpec) {
        specIds.add(((MetadataUpdate.SetDefaultPartitionSpec) update).specId());
      } else if (update instanceof MetadataUpdate.SetDefaultSortOrder) {
        sortOrderIds.add(((MetadataUpdate.SetDefaultSortOrder) update).sortOrderId());
      } else if (update instanceof MetadataUpdate.AddSortOrder) {
        addsSortOrder = true;
      }
    }
    // -1 means "the spec or sort order added last in this request".
    specIds.remove(-1);
    sortOrderIds.remove(-1);
    if (specIds.isEmpty() && sortOrderIds.isEmpty()) {
      return;
    }
    for (MetadataUpdate update : request.updates()) {
      if (update instanceof MetadataUpdate.AddPartitionSpec) {
        specIds.remove(((MetadataUpdate.AddPartitionSpec) update).spec().specId());
      }
    }
    Table table;
    try {
      table = getCatalog().loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      // A create-table transaction commits to a table that does not exist yet; Iceberg checks its
      // requirements itself.
      return;
    }
    for (Integer id : specIds) {
      if (!table.specs().containsKey(id)) {
        throw new BadRequestException(
            "Cannot set default partition spec to %s: the table has no spec with that id", id);
      }
    }
    // A sort order added in the same request gets its id when it is applied, so only ids the
    // table already has can be checked here.
    if (!addsSortOrder) {
      for (Integer id : sortOrderIds) {
        if (!table.sortOrders().containsKey(id)) {
          throw new BadRequestException(
              "Cannot set default sort order to %s: the table has no sort order with that id", id);
        }
      }
    }
  }
}
