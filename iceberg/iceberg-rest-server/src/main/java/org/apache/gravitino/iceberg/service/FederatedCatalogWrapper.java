/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;

/**
 * A {@link CatalogWrapperForREST} for a federated Iceberg REST catalog (the underlying catalog is a
 * {@link RESTCatalog}).
 *
 * <p>Federation-specific behavior is expressed through polymorphic overrides instead of {@code
 * instanceof RESTCatalog} checks scattered across the base class. Table load, create and register
 * use authenticated REST calls so {@code X-Iceberg-Access-Delegation} can be forwarded when the
 * client requested credential vending. Update still uses the Iceberg Catalog API.
 *
 * <p>By default the wrapper passes through the storage credentials the remote catalog vends and
 * never injects Gravitino-generated credentials. With {@code
 * rest-backend-storage-credential-source=local}, it never asks the remote catalog for credentials
 * and instead issues them through this catalog's own credential providers, scoped to the table
 * location and to the caller's privilege, exactly as for a local catalog.
 *
 * <p>Portions of the table update handling are derived from Apache Iceberg's {@code
 * org.apache.iceberg.rest.CatalogHandlers}:
 * https://github.com/apache/iceberg/blob/2abac79fcae94b5ad039bd09f7235be191b0761e/core/src/main/java/org/apache/iceberg/rest/CatalogHandlers.java
 */
public class FederatedCatalogWrapper extends CatalogWrapperForREST {

  private static final String FORMAT_VERSION = "format-version";
  private static final Schema EMPTY_SCHEMA = new Schema();
  private static final String X_ICEBERG_ACCESS_DELEGATION = "X-Iceberg-Access-Delegation";
  private static final String VENDED_CREDENTIALS = "vended-credentials";

  /**
   * Config key prefixes that carry storage credentials. With Gravitino credential vending, keys
   * with these prefixes are dropped from remote responses so remote credentials never reach
   * clients.
   */
  private static final List<String> REMOTE_CREDENTIAL_KEY_PREFIXES =
      ImmutableList.of(
          "s3.access-key-id",
          "s3.secret-access-key",
          "s3.session-token",
          "s3.remote-signing-enabled",
          "gcs.oauth2.",
          "adls.sas-token",
          "client.access-key-id",
          "client.access-key-secret",
          "client.security-token",
          "client.refresh-credentials-",
          "s3.refresh-credentials-",
          "s3.sse.key",
          "adls.token",
          "adls.auth.shared-key.",
          "gcs.client-lib-token",
          "gcs.encryption-key",
          "gcs.decryption-key",
          "token",
          "urn:ietf:params:oauth:token-type:");

  /**
   * Caches whether the remote catalog advertises the scan-plan endpoint. Only successful lookups
   * are cached, so a failure to reach the remote can be retried. Races just repeat an idempotent
   * lookup.
   */
  private volatile Boolean remoteSupportsScanPlan;

  /** Whether storage credentials come from this catalog's providers rather than the remote. */
  private final boolean vendCredentialsLocally;

  /**
   * Creates a federated wrapper.
   *
   * @param catalogName the catalog name.
   * @param config the Iceberg catalog configuration (backend {@code rest}).
   */
  public FederatedCatalogWrapper(String catalogName, IcebergConfig config) {
    super(catalogName, config);
    // A service account's credentials reflect what the service account may do, not the caller,
    // so a catalog connected as a service account never passes the remote's credentials through.
    this.vendCredentialsLocally =
        IcebergConstants.REST_BACKEND_STORAGE_CREDENTIAL_SOURCE_LOCAL.equals(
                config.get(IcebergConfig.REST_BACKEND_STORAGE_CREDENTIAL_SOURCE))
            || config.isRestBackendServiceAccount();
  }

  /**
   * Requires local authorization when the remote catalog is accessed as a service account.
   *
   * @param serverContext the REST server context
   */
  @Override
  public void validateServerContext(IcebergRESTServerContext serverContext) {
    if (getIcebergConfig().isRestBackendServiceAccount()
        && !serverContext.isAuthorizationEnabled()) {
      throw new IllegalArgumentException(
          String.format(
              "Catalog %s connects to its remote catalog as a service account (%s=%s), which"
                  + " requires authorization to be enabled",
              catalogCredentialManager.catalogName(),
              IcebergConstants.REST_BACKEND_AUTH_TYPE,
              IcebergConstants.REST_BACKEND_AUTH_TYPE_SERVICE_ACCOUNT));
    }
  }

  /**
   * Reports whether the remote catalog receives the calling user's access token.
   *
   * @return {@code true} for a user-token connection, or {@code false} for a service account
   */
  @Override
  public boolean forwardsUserToken() {
    return !getIcebergConfig().isRestBackendServiceAccount();
  }

  /**
   * Keeps cleanup of the remote catalog's files on the remote server.
   *
   * @return {@code false}, since this server must forward purge requests to the remote catalog
   */
  @Override
  public boolean supportsAsyncPurge() {
    return false;
  }

  /**
   * Forwards a purge to the remote catalog, which owns deletion of the table's files.
   *
   * @param tableIdentifier the table to purge
   */
  @Override
  public void purgeTable(TableIdentifier tableIdentifier) {
    invalidateTableMetadata(tableIdentifier);
    CatalogHandlers.purgeTable(getCatalog(), tableIdentifier);
  }

  /**
   * Creates a table on the remote REST catalog.
   *
   * <p>Always uses a dedicated REST POST, including staged create, rather than Iceberg's Catalog
   * API. When credential vending is requested the {@code X-Iceberg-Access-Delegation:
   * vended-credentials} header is forwarded so the remote catalog returns {@code
   * storage-credentials} inline. Upstream credential refresh endpoints are rewritten to this IRC
   * catalog.
   *
   * @param namespace the namespace that will own the table.
   * @param request the create-table request.
   * @param requestCredential whether the client requested vended credentials.
   * @return the create response, including rewritten remote credentials when requested.
   */
  @Override
  public LoadTableResponse createTable(
      Namespace namespace, CreateTableRequest request, boolean requestCredential) {
    if (!vendCredentialsLocally) {
      return createTableViaREST(namespace, request, requestCredential);
    }
    // The creator owns the new table, so it receives write credentials, as for a local catalog.
    return withLocalCredentials(
        TableIdentifier.of(namespace, request.name()),
        createTableViaREST(namespace, request, false),
        requestCredential,
        CredentialPrivilege.WRITE);
  }

  /**
   * Loads a table from the remote REST catalog.
   *
   * <p>Always uses a dedicated REST GET rather than Iceberg's {@link RESTCatalog#loadTable}, which
   * cannot send {@code X-Iceberg-Access-Delegation}. When credential vending is requested the
   * header is forwarded so the remote catalog returns {@code storage-credentials} inline. Upstream
   * credential refresh endpoints are rewritten to this IRC catalog. With Gravitino credential
   * vending, the remote is not asked for credentials and this catalog's providers issue them for
   * the caller's {@code privilege}; otherwise the remote catalog decides what to vend.
   *
   * @param identifier the table identifier.
   * @param requestCredential whether the client requested vended credentials.
   * @param privilege the caller's privilege, used only with Gravitino credential vending.
   * @return the load-table response, including rewritten remote credentials when requested.
   */
  @Override
  public LoadTableResponse loadTable(
      TableIdentifier identifier, boolean requestCredential, CredentialPrivilege privilege) {
    if (!vendCredentialsLocally) {
      return loadTableViaREST(identifier, requestCredential);
    }
    return withLocalCredentials(
        identifier, loadTableViaREST(identifier, false), requestCredential, privilege);
  }

  /**
   * Registers a table on the remote REST catalog.
   *
   * <p>Always uses a dedicated REST POST rather than Iceberg's Catalog API. When credential vending
   * is requested the {@code X-Iceberg-Access-Delegation: vended-credentials} header is forwarded so
   * the remote catalog returns {@code storage-credentials} inline. Upstream credential refresh
   * endpoints are rewritten to this IRC catalog.
   *
   * @param namespace the namespace that will own the table.
   * @param request the register-table request.
   * @param requestCredential whether the client requested vended credentials.
   * @return the register response, including rewritten remote credentials when requested.
   */
  @Override
  public LoadTableResponse registerTable(
      Namespace namespace, RegisterTableRequest request, boolean requestCredential) {
    if (!vendCredentialsLocally) {
      return registerTableViaREST(namespace, request, requestCredential);
    }
    // The registering user owns the table, so it receives write credentials, as for a local
    // catalog.
    return withLocalCredentials(
        TableIdentifier.of(namespace, request.name()),
        registerTableViaREST(namespace, request, false),
        requestCredential,
        CredentialPrivilege.WRITE);
  }

  @Override
  public LoadTableResponse updateTable(
      TableIdentifier tableIdentifier, UpdateTableRequest updateTableRequest) {
    return tableUpdateInternal(tableIdentifier, updateTableRequest);
  }

  @Override
  Map<String, String> buildCatalogConfigToClients() {
    Map<String, String> merged = ((RESTCatalog) getCatalog()).properties();
    return filterCatalogConfigForClients(merged != null ? merged : Collections.emptyMap());
  }

  /**
   * Fetches table credentials from the remote REST catalog, or, with Gravitino credential vending,
   * issues them through this catalog's credential providers for the caller's {@code privilege}.
   *
   * <p>The upstream credentials are rewritten via {@link IcebergRESTUtils#rewriteTableCredentials}
   * so their {@code refresh-credentials-endpoint} points at this IRC catalog rather than the remote
   * catalog, consistent with the {@link #loadTable}/{@code createTable} federation paths.
   *
   * @param identifier the table identifier.
   * @param privilege the caller's privilege, used only with Gravitino credential vending.
   * @return the remote credentials with IRC-local refresh endpoints.
   */
  @Override
  public LoadCredentialsResponse getTableCredentials(
      TableIdentifier identifier, CredentialPrivilege privilege) {
    if (vendCredentialsLocally) {
      return super.getTableCredentials(identifier, privilege);
    }
    LoadCredentialsResponse upstream =
        getRESTTableCredentials((RESTCatalog) getCatalog(), identifier);
    return IcebergRESTUtils.rewriteTableCredentials(
        catalogCredentialManager.catalogName(), identifier, upstream);
  }

  /**
   * Delegates scan planning to the remote REST catalog instead of executing it locally.
   *
   * <p>When credential vending is requested, the {@code X-Iceberg-Access-Delegation:
   * vended-credentials} header is forwarded so the remote catalog returns credentials inline. Any
   * upstream credential refresh endpoints are rewritten to point at this IRC instance.
   *
   * @param tableIdentifier the table to scan.
   * @param scanRequest the scan request parameters.
   * @param requestCredentialVending whether the client requested vended credentials.
   * @param privilege the caller's privilege, used only with Gravitino credential vending.
   * @return the scan response from the remote catalog, with rewritten credential refresh endpoints.
   */
  @Override
  public PlanTableScanResponse planTableScan(
      TableIdentifier tableIdentifier,
      PlanTableScanRequest scanRequest,
      boolean requestCredentialVending,
      CredentialPrivilege privilege) {
    Table table = getCatalog().loadTable(tableIdentifier);
    if (vendCredentialsLocally) {
      // Drop anything the remote returned unasked, so its credentials never reach the client.
      PlanTableScanResponse response =
          IcebergRESTUtils.copyWithCredentials(
              getRESTTablePlanScan(
                  (RESTCatalog) getCatalog(), tableIdentifier, scanRequest, false, table.specs()),
              Collections.emptyList());
      return requestCredentialVending && !isLocalOrHdfsLocation(table.location())
          ? injectScanCredentials(tableIdentifier, table, response, privilege)
          : response;
    }
    PlanTableScanResponse response =
        getRESTTablePlanScan(
            (RESTCatalog) getCatalog(),
            tableIdentifier,
            scanRequest,
            requestCredentialVending,
            table.specs());
    return IcebergRESTUtils.rewriteScanPlanCredentials(
        catalogCredentialManager.catalogName(), tableIdentifier, response);
  }

  /**
   * Reports whether the remote catalog advertises the scan-plan endpoint, since {@link
   * #planTableScan} delegates planning to it rather than planning locally.
   *
   * <p>The answer comes from the remote catalog's own {@code /v1/config} response and is cached for
   * the lifetime of this wrapper, so the remote is queried at most once rather than on every local
   * {@code /v1/config} call.
   *
   * <p>A remote that omits {@code endpoints} is treated as not supporting scan planning. That
   * matches the Iceberg client, which falls back to a default endpoint set that predates scan
   * planning when the field is absent.
   *
   * <p>If the remote cannot be reached the result is not cached and the endpoint is not advertised,
   * so a later call can still resolve it once the remote recovers. Not advertising is the safe
   * direction here: the endpoint would fail anyway while the remote is unreachable.
   *
   * @return {@code true} if the remote catalog advertises {@code V1_SUBMIT_TABLE_SCAN_PLAN}.
   */
  @Override
  public boolean supportsScanPlanOperations() {
    Boolean cached = remoteSupportsScanPlan;
    if (cached != null) {
      return cached;
    }

    try {
      boolean supported =
          fetchRemoteConfig().endpoints().contains(Endpoint.V1_SUBMIT_TABLE_SCAN_PLAN);
      remoteSupportsScanPlan = supported;
      return supported;
    } catch (Exception e) {
      LOG.warn(
          "Failed to read the endpoints advertised by the remote catalog of {}; not advertising the"
              + " scan plan endpoint",
          catalogCredentialManager.catalogName(),
          e);
      return false;
    }
  }

  /**
   * Fetches the remote catalog's {@code /v1/config} response.
   *
   * <p>The {@code warehouse} query parameter is forwarded when configured, so a remote serving
   * several warehouses returns the endpoint set for the one this catalog federates.
   *
   * <p>{@code RESTCatalog} already fetched this at init but keeps the endpoint set private, so it
   * has to be re-fetched here.
   *
   * @return the remote catalog's config response.
   */
  private ConfigResponse fetchRemoteConfig() {
    RESTCatalog restCatalog = (RESTCatalog) getCatalog();
    String warehouse = restCatalog.properties().get(CatalogProperties.WAREHOUSE_LOCATION);
    Map<String, String> queryParams =
        warehouse == null || warehouse.isEmpty()
            ? Collections.emptyMap()
            : ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

    return callRemoteCatalog(
        restCatalog,
        "reading the remote catalog config",
        client ->
            client.get(
                ResourcePaths.config(),
                queryParams,
                ConfigResponse.class,
                Collections.emptyMap(),
                ErrorHandlers.configErrorHandler()));
  }

  private static LoadCredentialsResponse getRESTTableCredentials(
      RESTCatalog restCatalog, TableIdentifier identifier) {
    Map<String, String> properties = Maps.newHashMap(restCatalog.properties());
    String credentialsPath =
        ResourcePaths.forCatalogProperties(properties).table(identifier) + "/credentials";

    return callRemoteCatalog(
        restCatalog,
        String.format("loading credentials for table: %s", identifier),
        client ->
            client.get(
                credentialsPath,
                LoadCredentialsResponse.class,
                Collections.emptyMap(),
                ErrorHandlers.tableErrorHandler()));
  }

  /**
   * Runs an action against the remote REST catalog through a short-lived authenticated client.
   *
   * <p>Centralizes the auth manager, HTTP client and auth session lifecycle shared by the federated
   * credential, scan-plan and config requests. Resources are closed in reverse order of creation,
   * and a close failure on one does not prevent the others from being closed.
   *
   * @param restCatalog the underlying REST catalog whose properties supply the URI and auth config.
   * @param description what the action is doing, used in close-failure log messages.
   * @param action invoked with a client bound to an authenticated session.
   * @param <T> the action's result type.
   * @return the action's result.
   */
  private static <T> T callRemoteCatalog(
      RESTCatalog restCatalog, String description, Function<RESTClient, T> action) {
    Map<String, String> properties = Maps.newHashMap(restCatalog.properties());

    AuthManager authManager = null;
    RESTClient client = null;
    AuthSession authSession = null;
    try {
      authManager = AuthManagers.loadAuthManager(restCatalog.name(), properties);
      client =
          HTTPClient.builder(properties)
              .uri(properties.get(CatalogProperties.URI))
              .withHeaders(RESTUtil.configHeaders(properties))
              .build();
      authSession = authManager.catalogSession(client, properties);
      return action.apply(client.withAuthSession(authSession));
    } finally {
      closeQuietly(authSession, "auth session", description);
      closeQuietly(client, "REST client", description);
      closeQuietly(authManager, "auth manager", description);
    }
  }

  private static void closeQuietly(
      AutoCloseable closeable, String resourceName, String description) {
    if (closeable == null) {
      return;
    }

    try {
      closeable.close();
    } catch (Exception e) {
      LOG.warn("Failed to close {} when {}", resourceName, description, e);
    }
  }

  /**
   * Sends a {@code POST {table}/plan} request to the remote REST catalog.
   *
   * <p>Follows the same HTTP client lifecycle as {@link #getRESTTableCredentials}. When credential
   * vending is requested, the {@code X-Iceberg-Access-Delegation: vended-credentials} header is
   * included so the remote catalog returns credentials inline in the plan response.
   *
   * <p>The Iceberg response deserializer requires pre-loaded partition specs to parse {@code
   * file-scan-tasks}. These are supplied via a {@link ParserContext} built from the caller-provided
   * {@code specsById} map (typically obtained from a prior {@code loadTable} call).
   *
   * @param restCatalog the underlying REST catalog whose properties supply the URI and auth config.
   * @param identifier the table to plan.
   * @param scanRequest the scan request parameters.
   * @param requestCredentialVending whether to include the access-delegation header.
   * @param specsById partition specs for the table, needed for response deserialization.
   * @return the plan response from the remote catalog.
   */
  private static PlanTableScanResponse getRESTTablePlanScan(
      RESTCatalog restCatalog,
      TableIdentifier identifier,
      PlanTableScanRequest scanRequest,
      boolean requestCredentialVending,
      Map<Integer, PartitionSpec> specsById) {
    Map<String, String> properties = Maps.newHashMap(restCatalog.properties());
    String planPath = ResourcePaths.forCatalogProperties(properties).planTableScan(identifier);

    Map<String, String> headers = accessDelegationHeaders(requestCredentialVending);

    ParserContext parserContext =
        ParserContext.builder()
            .add("specsById", specsById)
            .add("caseSensitive", scanRequest.caseSensitive())
            .build();

    return callRemoteCatalog(
        restCatalog,
        String.format("planning table scan for table: %s", identifier),
        client ->
            client.post(
                planPath,
                scanRequest,
                PlanTableScanResponse.class,
                headers,
                ErrorHandlers.planErrorHandler(),
                ignored -> {},
                parserContext));
  }

  /**
   * Sends a {@code GET {table}} request to the remote REST catalog.
   *
   * <p>Follows the same HTTP client lifecycle as {@link #getRESTTableCredentials}. When credential
   * vending is requested, the {@code X-Iceberg-Access-Delegation: vended-credentials} header is
   * included so the remote catalog returns credentials inline in the load-table response.
   *
   * @param restCatalog the underlying REST catalog whose properties supply the URI and auth config.
   * @param identifier the table to load.
   * @param requestCredentialVending whether to include the access-delegation header.
   * @return the load-table response from the remote catalog.
   */
  private static LoadTableResponse getRESTLoadTable(
      RESTCatalog restCatalog, TableIdentifier identifier, boolean requestCredentialVending) {
    Map<String, String> properties = Maps.newHashMap(restCatalog.properties());
    String tablePath = ResourcePaths.forCatalogProperties(properties).table(identifier);
    Map<String, String> queryParams = ImmutableMap.of("snapshots", IcebergRESTUtils.SNAPSHOT_ALL);

    return callRemoteCatalog(
        restCatalog,
        String.format("loading table: %s", identifier),
        client ->
            client.get(
                tablePath,
                queryParams,
                LoadTableResponse.class,
                accessDelegationHeaders(requestCredentialVending),
                ErrorHandlers.tableErrorHandler()));
  }

  /**
   * Sends a {@code POST {namespace}/tables} request to the remote REST catalog.
   *
   * @param restCatalog the underlying REST catalog whose properties supply the URI and auth config.
   * @param namespace the namespace that will own the table.
   * @param request the create-table request (including staged create).
   * @param requestCredentialVending whether to include the access-delegation header.
   * @return the create response from the remote catalog.
   */
  private static LoadTableResponse getRESTCreateTable(
      RESTCatalog restCatalog,
      Namespace namespace,
      CreateTableRequest request,
      boolean requestCredentialVending) {
    Map<String, String> properties = Maps.newHashMap(restCatalog.properties());
    String tablesPath = ResourcePaths.forCatalogProperties(properties).tables(namespace);

    return callRemoteCatalog(
        restCatalog,
        String.format("creating table: %s.%s", namespace, request.name()),
        client ->
            client.post(
                tablesPath,
                request,
                LoadTableResponse.class,
                accessDelegationHeaders(requestCredentialVending),
                ErrorHandlers.createTableErrorHandler()));
  }

  /**
   * Sends a {@code POST {namespace}/register} request to the remote REST catalog.
   *
   * @param restCatalog the underlying REST catalog whose properties supply the URI and auth config.
   * @param namespace the namespace that will own the table.
   * @param request the register-table request.
   * @param requestCredentialVending whether to include the access-delegation header.
   * @return the register response from the remote catalog.
   */
  private static LoadTableResponse getRESTRegisterTable(
      RESTCatalog restCatalog,
      Namespace namespace,
      RegisterTableRequest request,
      boolean requestCredentialVending) {
    Map<String, String> properties = Maps.newHashMap(restCatalog.properties());
    String registerPath = ResourcePaths.forCatalogProperties(properties).register(namespace);

    return callRemoteCatalog(
        restCatalog,
        String.format("registering table: %s.%s", namespace, request.name()),
        client ->
            client.post(
                registerPath,
                request,
                LoadTableResponse.class,
                accessDelegationHeaders(requestCredentialVending),
                ErrorHandlers.tableErrorHandler()));
  }

  private static Map<String, String> accessDelegationHeaders(boolean requestCredentialVending) {
    return requestCredentialVending
        ? ImmutableMap.of(X_ICEBERG_ACCESS_DELEGATION, VENDED_CREDENTIALS)
        : Collections.emptyMap();
  }

  private LoadTableResponse createTableViaREST(
      Namespace namespace, CreateTableRequest request, boolean requestCredential) {
    LoadTableResponse upstream =
        getRESTCreateTable((RESTCatalog) getCatalog(), namespace, request, requestCredential);
    return rewriteRemoteLoadTable(TableIdentifier.of(namespace, request.name()), upstream);
  }

  private LoadTableResponse loadTableViaREST(
      TableIdentifier identifier, boolean requestCredential) {
    LoadTableResponse upstream =
        getRESTLoadTable((RESTCatalog) getCatalog(), identifier, requestCredential);
    return rewriteRemoteLoadTable(identifier, upstream);
  }

  private LoadTableResponse registerTableViaREST(
      Namespace namespace, RegisterTableRequest request, boolean requestCredential) {
    LoadTableResponse upstream =
        getRESTRegisterTable((RESTCatalog) getCatalog(), namespace, request, requestCredential);
    return rewriteRemoteLoadTable(TableIdentifier.of(namespace, request.name()), upstream);
  }

  /**
   * Adds credentials issued by this catalog's credential providers to a response fetched from the
   * remote catalog without vended credentials.
   *
   * @param identifier the table identifier
   * @param remote the remote response
   * @param requestCredential whether the client requested vended credentials
   * @param privilege the caller's privilege on the table
   * @return the response, with Gravitino-issued credentials when they were requested and never with
   *     credentials from the remote catalog
   */
  private LoadTableResponse withLocalCredentials(
      TableIdentifier identifier,
      LoadTableResponse remote,
      boolean requestCredential,
      CredentialPrivilege privilege) {
    // Drop anything the remote returned unasked, so its credentials never reach the client.
    Map<String, String> config =
        Maps.filterKeys(
            remote.config(),
            key -> REMOTE_CREDENTIAL_KEY_PREFIXES.stream().noneMatch(key::startsWith));
    LoadTableResponse withoutRemoteCredentials =
        LoadTableResponse.builder()
            .withTableMetadata(remote.tableMetadata())
            .addAllConfig(config)
            .build();
    return shouldGenerateCredential(withoutRemoteCredentials, requestCredential)
        ? injectCredentialConfig(identifier, withoutRemoteCredentials, privilege)
        : withoutRemoteCredentials;
  }

  private LoadTableResponse rewriteRemoteLoadTable(
      TableIdentifier identifier, LoadTableResponse upstream) {
    return IcebergRESTUtils.rewriteLoadTableCredentials(
        catalogCredentialManager.catalogName(), identifier, upstream);
  }

  /**
   * Federation-aware {@code updateTable}: applies the update against the underlying (remote)
   * catalog, including the staged-create path used by federated table creation.
   */
  private LoadTableResponse tableUpdateInternal(TableIdentifier ident, UpdateTableRequest request) {
    if (isCreate(request)) {
      // this is a hacky way to get TableOperations for an uncommitted table
      Optional<Integer> formatVersion =
          request.updates().stream()
              .filter(update -> update instanceof MetadataUpdate.UpgradeFormatVersion)
              .map(update -> ((MetadataUpdate.UpgradeFormatVersion) update).formatVersion())
              .findFirst();

      Schema schema =
          request.updates().stream()
              .filter(update -> update instanceof MetadataUpdate.AddSchema)
              .map(update -> ((MetadataUpdate.AddSchema) update).schema())
              .findFirst()
              .orElse(EMPTY_SCHEMA);

      Catalog.TableBuilder tableBuilder = getCatalog().buildTable(ident, schema);

      TableMetadata.Builder changedMetadata =
          formatVersion.map(TableMetadata::buildFromEmpty).orElse(TableMetadata.buildFromEmpty());
      request.updates().forEach(update -> update.applyTo(changedMetadata));

      TableMetadata changedTableMeta = changedMetadata.build();
      tableBuilder.withPartitionSpec(changedTableMeta.spec());
      tableBuilder.withSortOrder(changedTableMeta.sortOrder());
      tableBuilder.withLocation(changedTableMeta.location());
      tableBuilder.withProperty(FORMAT_VERSION, String.valueOf(changedTableMeta.formatVersion()));
      tableBuilder.withProperties(changedTableMeta.properties());

      Transaction transaction = tableBuilder.createOrReplaceTransaction();
      if (transaction instanceof BaseTransaction) {
        BaseTransaction baseTransaction = (BaseTransaction) transaction;

        return LoadTableResponse.builder()
            .withTableMetadata(create(baseTransaction, request))
            .build();
      } else {
        throw new IllegalStateException(
            "Cannot wrap catalog that does not produce BaseTransaction");
      }

    } else {
      return CatalogHandlers.updateTable(getCatalog(), ident, request);
    }
  }

  private static boolean isCreate(UpdateTableRequest request) {
    boolean isCreate =
        request.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);

    if (isCreate) {
      List<UpdateRequirement> invalidRequirements =
          request.requirements().stream()
              .filter(req -> !(req instanceof UpdateRequirement.AssertTableDoesNotExist))
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          invalidRequirements.isEmpty(), "Invalid create requirements: %s", invalidRequirements);
    }

    return isCreate;
  }

  private static TableMetadata create(BaseTransaction baseTransaction, UpdateTableRequest request) {
    // the only valid requirement is that the table will be created
    TableOperations ops = baseTransaction.underlyingOps();
    request.requirements().forEach(requirement -> requirement.validate(ops.current()));

    TableMetadata.Builder builder = TableMetadata.buildFrom(baseTransaction.currentMetadata());
    request
        .updates()
        .forEach(
            update -> {
              if (shouldApplyMetadataUpdateAfterBuilder(update)) {
                update.applyTo(builder);
              }
            });

    // create transactions do not retry. if the table exists, retrying is not a solution
    ops.commit(null, builder.build());

    return ops.current();
  }

  /**
   * Returns {@code false} for updates already reflected through {@link Catalog.TableBuilder} during
   * staged create; those must not be applied again on {@link TableMetadata.Builder}.
   */
  @VisibleForTesting
  static boolean shouldApplyMetadataUpdateAfterBuilder(MetadataUpdate update) {
    if (update instanceof MetadataUpdate.UpgradeFormatVersion) {
      return false;
    }

    if (update instanceof MetadataUpdate.AddSchema) {
      return false;
    }

    if (update instanceof MetadataUpdate.SetCurrentSchema) {
      return false;
    }

    if (update instanceof MetadataUpdate.RemoveSchemas) {
      return false;
    }

    if (update instanceof MetadataUpdate.SetLocation) {
      return false;
    }

    if (update instanceof MetadataUpdate.SetProperties) {
      return false;
    }

    if (update instanceof MetadataUpdate.RemoveProperties) {
      return false;
    }

    if (update instanceof MetadataUpdate.AddSortOrder) {
      return false;
    }

    if (update instanceof MetadataUpdate.SetDefaultSortOrder) {
      return false;
    }

    if (update instanceof MetadataUpdate.AddPartitionSpec) {
      return false;
    }

    if (update instanceof MetadataUpdate.SetDefaultPartitionSpec) {
      return false;
    }

    if (update instanceof MetadataUpdate.RemovePartitionSpecs) {
      return false;
    }

    return true;
  }
}
