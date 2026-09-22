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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoClient.ClientBuilder;
import org.apache.gravitino.config.ConfigEntry;
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
import org.apache.gravitino.secret.SecretPropertyOperationDispatcher;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoLanceNamespaceWrapper extends NamespaceWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoLanceNamespaceWrapper.class);

  private String metalakeName;
  private CatalogOperator catalogOperator;
  private LanceNamespaceOperations namespaceOperations;
  private LanceTableOperations tableOperations;

  @VisibleForTesting
  GravitinoLanceNamespaceWrapper() {
    super(null);
  }

  public GravitinoLanceNamespaceWrapper(LanceConfig config, boolean auxMode) {
    super(config, auxMode);
  }

  @Override
  protected void initialize() {
    metalakeName = config().get(METALAKE_NAME);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "Metalake name must be provided for Lance Gravitino namespace backend");

    this.catalogOperator = createCatalogOperator(metalakeName);

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
    if (catalogOperator != null) {
      try {
        catalogOperator.close();
      } catch (Exception e) {
        LOG.warn("Error closing Lance catalog operator", e);
      }
    }
  }

  Catalog[] listCatalogsInfo() throws NoSuchMetalakeException {
    return catalogOperator.listCatalogsInfo();
  }

  Catalog loadCatalog(String catalogName) throws NoSuchCatalogException {
    return catalogOperator.loadCatalog(catalogName);
  }

  Map<String, String> propsWithSecrets(Catalog catalog) {
    Map<String, String> props = copyProps(catalog.properties());
    props.putAll(catalogSecrets(catalog.name()));
    return props;
  }

  Map<String, String> schemaPropsWithSecrets(Catalog catalog, String schemaName) {
    Schema schema = loadSchema(catalog, schemaName);
    Map<String, String> props = copyProps(schema.properties());
    props.putAll(schemaSecrets(catalog.name(), schemaName));
    return props;
  }

  private Map<String, String> catalogSecrets(String catalogName) {
    SecretPropertyOperationDispatcher dispatcher = secretDispatcher();
    if (dispatcher != null) {
      return dispatcher.getSecrets(
          NameIdentifierUtil.ofCatalog(metalakeName, catalogName), Entity.EntityType.CATALOG);
    }
    return loadCatalog(catalogName).supportsSecrets().getSecrets();
  }

  private Map<String, String> schemaSecrets(String catalogName, String schemaName) {
    SecretPropertyOperationDispatcher dispatcher = secretDispatcher();
    if (dispatcher != null) {
      return dispatcher.getSecrets(schemaIdent(catalogName, schemaName), Entity.EntityType.SCHEMA);
    }
    return loadSchema(loadCatalog(catalogName), schemaName).supportsSecrets().getSecrets();
  }

  private SecretPropertyOperationDispatcher secretDispatcher() {
    try {
      return GravitinoEnv.getInstance().secretPropertyOperationDispatcher();
    } catch (Exception e) {
      return null;
    }
  }

  private static Map<String, String> copyProps(Map<String, String> props) {
    return new HashMap<>(props == null ? Map.of() : props);
  }

  Catalog createCatalog(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    return catalogOperator.createCatalog(catalogName, type, provider, comment, properties);
  }

  Catalog alterCatalog(String catalogName, CatalogChange... changes) throws NoSuchCatalogException {
    return catalogOperator.alterCatalog(catalogName, changes);
  }

  boolean dropCatalog(String catalogName, boolean force)
      throws NonEmptyEntityException, CatalogInUseException {
    return catalogOperator.dropCatalog(catalogName, force);
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
      return Arrays.stream(schemaDispatcher.listSchemas(Namespace.of(metalakeName, catalog.name())))
          .map(NameIdentifier::name)
          .toArray(String[]::new);
    }

    return catalog.asSchemas().listSchemas();
  }

  boolean schemaExists(Catalog catalog, String schemaName) {
    SchemaDispatcher schemaDispatcher = currentSchemaDispatcher();
    if (schemaDispatcher != null) {
      return schemaDispatcher.schemaExists(schemaIdent(catalog.name(), schemaName));
    }

    return catalog.asSchemas().schemaExists(schemaName);
  }

  Schema loadSchema(Catalog catalog, String schemaName) {
    SchemaDispatcher schemaDispatcher = currentSchemaDispatcher();
    if (schemaDispatcher != null) {
      return schemaDispatcher.loadSchema(schemaIdent(catalog.name(), schemaName));
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

    return catalog.asSchemas().createSchema(schemaName, comment, properties);
  }

  Schema alterSchema(Catalog catalog, String schemaName, SchemaChange... changes) {
    SchemaDispatcher schemaDispatcher = currentSchemaDispatcher();
    if (schemaDispatcher != null) {
      return schemaDispatcher.alterSchema(schemaIdent(catalog.name(), schemaName), changes);
    }

    return catalog.asSchemas().alterSchema(schemaName, changes);
  }

  boolean dropSchema(Catalog catalog, String schemaName, boolean cascade) {
    SchemaDispatcher schemaDispatcher = currentSchemaDispatcher();
    if (schemaDispatcher != null) {
      return schemaDispatcher.dropSchema(schemaIdent(catalog.name(), schemaName), cascade);
    }

    return catalog.asSchemas().dropSchema(schemaName, cascade);
  }

  TableCatalog asTableCatalog(Catalog catalog) {
    TableDispatcher tableDispatcher = currentTableDispatcher();
    if (tableDispatcher != null) {
      return new InternalTableCatalogAdapter(catalog.name(), tableDispatcher);
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
    if (!isAuxMode()) {
      return null;
    }

    return GravitinoEnv.getInstance().schemaDispatcher();
  }

  private TableDispatcher currentTableDispatcher() {
    if (!isAuxMode()) {
      return null;
    }

    return GravitinoEnv.getInstance().tableDispatcher();
  }

  @VisibleForTesting
  CatalogOperator createCatalogOperator(String metalakeName) {
    return isAuxMode()
        ? new InternalCatalogOperator(metalakeName)
        : new HttpCatalogOperator(
            config().get(NAMESPACE_BACKEND_URI), metalakeName, config(), extractClientProperties());
  }

  @VisibleForTesting
  void setCatalogOperator(CatalogOperator catalogOperator) {
    this.catalogOperator = catalogOperator;
  }

  private Map<String, String> extractClientProperties() {
    Map<String, String> clientProperties = new HashMap<>();
    config()
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith("gravitino.client.")) {
                clientProperties.put(key, value);
                LOG.debug("Applying Gravitino client config key: {}", key);
              }
            });
    return clientProperties;
  }

  interface CatalogOperator extends Closeable {

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

  private static class InternalCatalogOperator implements CatalogOperator {
    private final String metalakeName;
    private final CatalogDispatcher catalogDispatcher;

    private InternalCatalogOperator(String metalakeName) {
      this.metalakeName = metalakeName;
      CatalogDispatcher dispatcher = GravitinoEnv.getInstance().catalogDispatcher();
      Preconditions.checkState(
          dispatcher != null,
          "CatalogDispatcher is not available. Internal catalog operator requires Gravitino server mode.");
      this.catalogDispatcher = dispatcher;
    }

    @Override
    public Catalog[] listCatalogsInfo() throws NoSuchMetalakeException {
      return catalogDispatcher.listCatalogsInfo(Namespace.of(metalakeName));
    }

    @Override
    public Catalog loadCatalog(String catalogName) throws NoSuchCatalogException {
      return catalogDispatcher.loadCatalog(NameIdentifierUtil.ofCatalog(metalakeName, catalogName));
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

  private static class HttpCatalogOperator implements CatalogOperator {
    private final String uri;
    private final String metalakeName;
    private final LanceConfig config;
    private final Map<String, String> clientProperties;
    private volatile GravitinoClient client;

    private HttpCatalogOperator(
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
          client = createGravitinoClient(uri, metalakeName, clientProperties, config);
          LOG.info(
              "GravitinoClient initialized with auth type {} and {} client properties for metalake: {}",
              config.getGravitinoAuthType(),
              clientProperties.size(),
              metalakeName);
        }
      }

      return client;
    }
  }

  static GravitinoClient createGravitinoClient(
      String uri, String metalake, Map<String, String> clientProperties, LanceConfig config) {
    return newClientBuilder(uri, metalake, clientProperties, config).build();
  }

  /**
   * Builds and configures the client builder, including the credentials the Lance REST service
   * presents to the Gravitino server. Separated from {@link #createGravitinoClient} so that the
   * configuration can be exercised without contacting a server.
   *
   * @param uri the Gravitino server URI
   * @param metalake the metalake name
   * @param clientProperties additional client properties, such as connection pool settings
   * @param config the Lance REST service configuration holding the auth settings
   * @return a configured client builder
   */
  @VisibleForTesting
  static ClientBuilder newClientBuilder(
      String uri, String metalake, Map<String, String> clientProperties, LanceConfig config) {
    ClientBuilder builder = GravitinoClient.builder(uri).withMetalake(metalake);
    builder.withClientConfig(clientProperties);
    String authType = config.getGravitinoAuthType();
    if (AuthProperties.isSimple(authType)) {
      builder.withSimpleAuth(config.get(LanceConfig.GRAVITINO_SIMPLE_USERNAME));
    } else if (AuthProperties.isOAuth2(authType)) {
      DefaultOAuth2TokenProvider tokenProvider =
          DefaultOAuth2TokenProvider.builder()
              .withUri(requireConfig(config, LanceConfig.GRAVITINO_OAUTH2_SERVER_URI, "server-uri"))
              .withCredential(
                  requireConfig(config, LanceConfig.GRAVITINO_OAUTH2_CREDENTIAL, "credential"))
              .withPath(
                  requireConfig(config, LanceConfig.GRAVITINO_OAUTH2_TOKEN_PATH, "token-path"))
              .withScope(requireConfig(config, LanceConfig.GRAVITINO_OAUTH2_SCOPE, "scope"))
              .build();
      builder.withOAuth(tokenProvider);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported value for %sgravitino-%s: %s. Supported values are %s and %s.",
              LanceConfig.LANCE_CONFIG_PREFIX,
              LanceConfig.CONFIG_AUTH_TYPE,
              authType,
              AuthProperties.SIMPLE_AUTH_TYPE,
              AuthProperties.OAUTH2_AUTH_TYPE));
    }
    return builder;
  }

  private static String requireConfig(LanceConfig config, ConfigEntry<String> entry, String name) {
    String value = config.get(entry);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(value),
        "%sgravitino-oauth2.%s must be set when %sgravitino-%s is %s",
        LanceConfig.LANCE_CONFIG_PREFIX,
        name,
        LanceConfig.LANCE_CONFIG_PREFIX,
        LanceConfig.CONFIG_AUTH_TYPE,
        AuthProperties.OAUTH2_AUTH_TYPE);
    return value;
  }
}
