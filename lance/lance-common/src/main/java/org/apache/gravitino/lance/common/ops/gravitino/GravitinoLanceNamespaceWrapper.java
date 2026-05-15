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
package org.apache.gravitino.lance.common.ops.gravitino;

import static org.apache.gravitino.lance.common.config.LanceConfig.METALAKE_NAME;
import static org.apache.gravitino.lance.common.config.LanceConfig.NAMESPACE_BACKEND_URI;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoClient.ClientBuilder;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.apache.gravitino.lance.common.ops.LanceNamespaceOperations;
import org.apache.gravitino.lance.common.ops.LanceTableOperations;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoLanceNamespaceWrapper extends NamespaceWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoLanceNamespaceWrapper.class);

  private String metalakeName;
  private CatalogFetcher catalogFetcher;
  private LanceNamespaceOperations namespaceOperations;
  private LanceTableOperations tableOperations;

  @VisibleForTesting
  GravitinoLanceNamespaceWrapper() {
    super(null);
  }

  public GravitinoLanceNamespaceWrapper(LanceConfig config) {
    super(config);
  }

  @Override
  protected void initialize() {
    metalakeName = config().get(METALAKE_NAME);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "Metalake name must be provided for Lance Gravitino namespace backend");

    this.catalogFetcher = createCatalogFetcher(metalakeName);

    this.namespaceOperations = new GravitinoLanceNameSpaceOperations(this);
    this.tableOperations = new GravitinoLanceTableOperations(this);
  }

  @Override
  public LanceNamespaceOperations newNamespaceOps() {
    return namespaceOperations;
  }

  @Override
  protected LanceTableOperations newTableOps() {
    return tableOperations;
  }

  @Override
  public void close() {
    if (catalogFetcher != null) {
      try {
        catalogFetcher.close();
      } catch (Exception e) {
        LOG.warn("Error closing Lance catalog fetcher", e);
      }
    }
  }

  Catalog[] listCatalogsInfo() throws NoSuchMetalakeException {
    return catalogFetcher.listCatalogsInfo();
  }

  Catalog loadCatalog(String catalogName) throws NoSuchCatalogException {
    return catalogFetcher.loadCatalog(catalogName);
  }

  Catalog createCatalog(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    return catalogFetcher.createCatalog(catalogName, type, provider, comment, properties);
  }

  Catalog alterCatalog(String catalogName, CatalogChange... changes) throws NoSuchCatalogException {
    return catalogFetcher.alterCatalog(catalogName, changes);
  }

  boolean dropCatalog(String catalogName, boolean force)
      throws NonEmptyEntityException, CatalogInUseException {
    return catalogFetcher.dropCatalog(catalogName, force);
  }

  public boolean isLakehouseCatalog(Catalog catalog) {
    return catalog.type().equals(Catalog.Type.RELATIONAL)
        && "lakehouse-generic".equals(catalog.provider());
  }

  public Catalog loadAndValidateLakehouseCatalog(String catalogName) {
    Catalog catalog;
    try {
      catalog = loadCatalog(catalogName);
    } catch (NoSuchCatalogException e) {
      throw new NamespaceNotFoundException(
          "Catalog not found: " + catalogName, CommonUtil.formatCurrentStackTrace(), catalogName);
    }
    if (!isLakehouseCatalog(catalog)) {
      throw new NamespaceNotFoundException(
          "Catalog is not a lakehouse catalog: " + catalogName,
          CommonUtil.formatCurrentStackTrace(),
          catalogName);
    }
    return catalog;
  }

  String[] listSchemas(Catalog catalog) throws NoSuchCatalogException {
    SchemaDispatcher schemaDispatcher = currentSchemaDispatcher();
    if (schemaDispatcher != null) {
      return java.util.Arrays.stream(
              schemaDispatcher.listSchemas(Namespace.of(metalakeName, catalog.name())))
          .map(NameIdentifier::name)
          .toArray(String[]::new);
    }

    if (catalog instanceof BaseCatalog) {
      CatalogOperations ops = ((BaseCatalog<?>) catalog).ops();
      if (ops instanceof org.apache.gravitino.connector.SupportsSchemas) {
        return java.util.Arrays.stream(
                ((org.apache.gravitino.connector.SupportsSchemas) ops).listSchemas(Namespace.of()))
            .map(NameIdentifier::name)
            .toArray(String[]::new);
      }
    }

    return catalog.asSchemas().listSchemas();
  }

  boolean schemaExists(Catalog catalog, String schemaName) {
    SchemaDispatcher schemaDispatcher = currentSchemaDispatcher();
    if (schemaDispatcher != null) {
      return schemaDispatcher.schemaExists(schemaIdent(catalog.name(), schemaName));
    }

    if (catalog instanceof BaseCatalog) {
      CatalogOperations ops = ((BaseCatalog<?>) catalog).ops();
      if (ops instanceof org.apache.gravitino.connector.SupportsSchemas) {
        return ((org.apache.gravitino.connector.SupportsSchemas) ops)
            .schemaExists(NameIdentifier.of(schemaName));
      }
    }

    return catalog.asSchemas().schemaExists(schemaName);
  }

  Schema loadSchema(Catalog catalog, String schemaName) {
    SchemaDispatcher schemaDispatcher = currentSchemaDispatcher();
    if (schemaDispatcher != null) {
      return schemaDispatcher.loadSchema(schemaIdent(catalog.name(), schemaName));
    }

    if (catalog instanceof BaseCatalog) {
      CatalogOperations ops = ((BaseCatalog<?>) catalog).ops();
      if (ops instanceof org.apache.gravitino.connector.SupportsSchemas) {
        return ((org.apache.gravitino.connector.SupportsSchemas) ops)
            .loadSchema(NameIdentifier.of(schemaName));
      }
    }

    return catalog.asSchemas().loadSchema(schemaName);
  }

  Schema createSchema(
      Catalog catalog, String schemaName, String comment, Map<String, String> properties) {
    SchemaDispatcher schemaDispatcher = currentSchemaDispatcher();
    if (schemaDispatcher != null) {
      return schemaDispatcher.createSchema(
          schemaIdent(catalog.name(), schemaName), comment, properties);
    }

    if (catalog instanceof BaseCatalog) {
      CatalogOperations ops = ((BaseCatalog<?>) catalog).ops();
      if (ops instanceof org.apache.gravitino.connector.SupportsSchemas) {
        return ((org.apache.gravitino.connector.SupportsSchemas) ops)
            .createSchema(NameIdentifier.of(schemaName), comment, properties);
      }
    }

    return catalog.asSchemas().createSchema(schemaName, comment, properties);
  }

  Schema alterSchema(Catalog catalog, String schemaName, SchemaChange... changes) {
    SchemaDispatcher schemaDispatcher = currentSchemaDispatcher();
    if (schemaDispatcher != null) {
      return schemaDispatcher.alterSchema(schemaIdent(catalog.name(), schemaName), changes);
    }

    if (catalog instanceof BaseCatalog) {
      CatalogOperations ops = ((BaseCatalog<?>) catalog).ops();
      if (ops instanceof org.apache.gravitino.connector.SupportsSchemas) {
        return ((org.apache.gravitino.connector.SupportsSchemas) ops)
            .alterSchema(NameIdentifier.of(schemaName), changes);
      }
    }

    return catalog.asSchemas().alterSchema(schemaName, changes);
  }

  boolean dropSchema(Catalog catalog, String schemaName, boolean cascade) {
    SchemaDispatcher schemaDispatcher = currentSchemaDispatcher();
    if (schemaDispatcher != null) {
      return schemaDispatcher.dropSchema(schemaIdent(catalog.name(), schemaName), cascade);
    }

    if (catalog instanceof BaseCatalog) {
      CatalogOperations ops = ((BaseCatalog<?>) catalog).ops();
      if (ops instanceof org.apache.gravitino.connector.SupportsSchemas) {
        return ((org.apache.gravitino.connector.SupportsSchemas) ops)
            .dropSchema(NameIdentifier.of(schemaName), cascade);
      }
    }

    return catalog.asSchemas().dropSchema(schemaName, cascade);
  }

  TableCatalog asTableCatalog(Catalog catalog) {
    TableDispatcher tableDispatcher = currentTableDispatcher();
    if (tableDispatcher != null) {
      return new InternalTableCatalogAdapter(catalog.name(), tableDispatcher);
    }

    if (catalog instanceof BaseCatalog) {
      CatalogOperations ops = ((BaseCatalog<?>) catalog).ops();
      if (ops instanceof TableCatalog) {
        return (TableCatalog) ops;
      }
    }

    return catalog.asTableCatalog();
  }

  private NameIdentifier schemaIdent(String catalogName, String schemaName) {
    return NameIdentifierUtil.ofSchema(metalakeName, catalogName, schemaName);
  }

  private NameIdentifier tableIdent(String catalogName, NameIdentifier ident) {
    return NameIdentifierUtil.ofTable(
        metalakeName, catalogName, ident.namespace().level(0), ident.name());
  }

  private Namespace tableNamespace(String catalogName, Namespace namespace) {
    return Namespace.of(metalakeName, catalogName, namespace.level(0));
  }

  private SchemaDispatcher currentSchemaDispatcher() {
    if (!config().isAuxMode()) {
      return null;
    }

    return GravitinoEnv.getInstance().schemaDispatcher();
  }

  private TableDispatcher currentTableDispatcher() {
    if (!config().isAuxMode()) {
      return null;
    }

    return GravitinoEnv.getInstance().tableDispatcher();
  }

  @VisibleForTesting
  CatalogFetcher createCatalogFetcher(String metalakeName) {
    return config().isAuxMode()
        ? new InternalCatalogFetcher(metalakeName)
        : new HttpCatalogFetcher(
            config().get(NAMESPACE_BACKEND_URI), metalakeName, config(), extractClientProperties());
  }

  @VisibleForTesting
  void setCatalogFetcher(CatalogFetcher catalogFetcher) {
    this.catalogFetcher = catalogFetcher;
  }

  private Map<String, String> extractClientProperties() {
    Map<String, String> clientProperties = new HashMap<>();
    config()
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith("gravitino.client.")) {
                clientProperties.put(key, value);
                LOG.info("Applying client config: {} = {}", key, value);
              }
            });
    return clientProperties;
  }

  @VisibleForTesting
  static GravitinoClient createGravitinoClient(
      String uri, String metalakeName, LanceConfig config, Map<String, String> clientProperties) {
    ClientBuilder builder = GravitinoClient.builder(uri).withMetalake(metalakeName);
    builder.withClientConfig(clientProperties);
    return builder.build();
  }

  interface CatalogFetcher extends Closeable {

    Catalog[] listCatalogsInfo() throws NoSuchMetalakeException;

    Catalog loadCatalog(String catalogName) throws NoSuchCatalogException;

    Catalog createCatalog(
        String catalogName,
        Catalog.Type type,
        String provider,
        String comment,
        Map<String, String> properties)
        throws NoSuchMetalakeException, CatalogAlreadyExistsException;

    Catalog alterCatalog(String catalogName, CatalogChange... changes)
        throws NoSuchCatalogException;

    boolean dropCatalog(String catalogName, boolean force)
        throws NonEmptyEntityException, CatalogInUseException;

    @Override
    default void close() {}
  }

  private class InternalTableCatalogAdapter implements TableCatalog {
    private final String catalogName;
    private final TableDispatcher dispatcher;

    private InternalTableCatalogAdapter(String catalogName, TableDispatcher dispatcher) {
      this.catalogName = catalogName;
      this.dispatcher = dispatcher;
    }

    @Override
    public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
      return dispatcher.listTables(tableNamespace(catalogName, namespace));
    }

    @Override
    public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
      return dispatcher.loadTable(tableIdent(catalogName, ident));
    }

    @Override
    public Table createTable(
        NameIdentifier ident,
        Column[] columns,
        String comment,
        Map<String, String> properties,
        Transform[] partitions,
        Distribution distribution,
        SortOrder[] sortOrders,
        Index[] indexes)
        throws NoSuchSchemaException, TableAlreadyExistsException {
      return dispatcher.createTable(
          tableIdent(catalogName, ident),
          columns,
          comment,
          properties,
          partitions,
          distribution,
          sortOrders,
          indexes);
    }

    @Override
    public Table alterTable(NameIdentifier ident, TableChange... changes)
        throws NoSuchTableException, IllegalArgumentException {
      return dispatcher.alterTable(tableIdent(catalogName, ident), changes);
    }

    @Override
    public boolean dropTable(NameIdentifier ident) {
      return dispatcher.dropTable(tableIdent(catalogName, ident));
    }

    @Override
    public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
      return dispatcher.purgeTable(tableIdent(catalogName, ident));
    }

    @Override
    public boolean tableExists(NameIdentifier ident) {
      return dispatcher.tableExists(tableIdent(catalogName, ident));
    }
  }

  private static class InternalCatalogFetcher implements CatalogFetcher {
    private final String metalakeName;
    private final CatalogDispatcher catalogDispatcher;
    private final CatalogManager catalogManager;

    private InternalCatalogFetcher(String metalakeName) {
      this.metalakeName = metalakeName;
      CatalogDispatcher dispatcher = GravitinoEnv.getInstance().catalogDispatcher();
      Preconditions.checkState(
          dispatcher != null,
          "CatalogDispatcher is not available. Internal catalog fetcher requires Gravitino server mode.");
      this.catalogDispatcher = dispatcher;
      this.catalogManager = GravitinoEnv.getInstance().catalogManager();
    }

    @Override
    public Catalog[] listCatalogsInfo() throws NoSuchMetalakeException {
      return catalogDispatcher.listCatalogsInfo(Namespace.of(metalakeName));
    }

    @Override
    public Catalog loadCatalog(String catalogName) throws NoSuchCatalogException {
      NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(metalakeName, catalogName);
      if (catalogManager != null) {
        return catalogManager.loadCatalog(catalogIdent);
      }

      return catalogDispatcher.loadCatalog(catalogIdent);
    }

    @Override
    public Catalog createCatalog(
        String catalogName,
        Catalog.Type type,
        String provider,
        String comment,
        Map<String, String> properties)
        throws NoSuchMetalakeException, CatalogAlreadyExistsException {
      return catalogDispatcher.createCatalog(
          NameIdentifierUtil.ofCatalog(metalakeName, catalogName),
          type,
          provider,
          comment,
          properties);
    }

    @Override
    public Catalog alterCatalog(String catalogName, CatalogChange... changes)
        throws NoSuchCatalogException {
      return catalogDispatcher.alterCatalog(
          NameIdentifierUtil.ofCatalog(metalakeName, catalogName), changes);
    }

    @Override
    public boolean dropCatalog(String catalogName, boolean force)
        throws NonEmptyEntityException, CatalogInUseException {
      return catalogDispatcher.dropCatalog(
          NameIdentifierUtil.ofCatalog(metalakeName, catalogName), force);
    }
  }

  private static class HttpCatalogFetcher implements CatalogFetcher {
    private final String uri;
    private final String metalakeName;
    private final LanceConfig config;
    private final Map<String, String> clientProperties;
    private volatile GravitinoClient client;

    private HttpCatalogFetcher(
        String uri, String metalakeName, LanceConfig config, Map<String, String> clientProperties) {
      this.uri = uri;
      this.metalakeName = metalakeName;
      this.config = config;
      this.clientProperties = clientProperties;
    }

    @Override
    public Catalog[] listCatalogsInfo() throws NoSuchMetalakeException {
      return getClient().listCatalogsInfo();
    }

    @Override
    public Catalog loadCatalog(String catalogName) throws NoSuchCatalogException {
      return getClient().loadCatalog(catalogName);
    }

    @Override
    public Catalog createCatalog(
        String catalogName,
        Catalog.Type type,
        String provider,
        String comment,
        Map<String, String> properties)
        throws NoSuchMetalakeException, CatalogAlreadyExistsException {
      return getClient().createCatalog(catalogName, type, provider, comment, properties);
    }

    @Override
    public Catalog alterCatalog(String catalogName, CatalogChange... changes)
        throws NoSuchCatalogException {
      return getClient().alterCatalog(catalogName, changes);
    }

    @Override
    public boolean dropCatalog(String catalogName, boolean force)
        throws NonEmptyEntityException, CatalogInUseException {
      return getClient().dropCatalog(catalogName, force);
    }

    @Override
    public void close() {
      Optional.ofNullable(client).ifPresent(GravitinoClient::close);
    }

    private GravitinoClient getClient() {
      if (client != null) {
        return client;
      }

      synchronized (this) {
        if (client == null) {
          client = createGravitinoClient(uri, metalakeName, config, clientProperties);
          LOG.info(
              "GravitinoClient initialized with {} client properties for metalake: {}",
              clientProperties.size(),
              metalakeName);
        }
      }

      return client;
    }
  }
}
