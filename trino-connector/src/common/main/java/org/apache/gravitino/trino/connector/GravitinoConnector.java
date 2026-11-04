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
package org.apache.gravitino.trino.connector;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.security.GravitinoAuthProvider;

/**
 * GravitinoConnector serves as the entry point for operations on the connector managed by Trino and
 * Apache Gravitino. It provides a standard entry point for Trino connectors and delegates their
 * operations to internal connectors.
 */
public class GravitinoConnector implements Connector {

  private static final Logger LOG = Logger.get(GravitinoConnector.class);
  private static final String ICEBERG_PROVIDER = "lakehouse-iceberg";
  private static final String TRINO_ICEBERG_REST_SECURITY = "iceberg.rest-catalog.security";
  private static final String OAUTH2_PASSTHROUGH = "OAUTH2_PASSTHROUGH";

  private final NameIdentifier catalogIdentifier;
  protected final CatalogConnectorContext catalogConnectorContext;
  private final CatalogConnectorMetadata connectorMetadata;
  private final boolean forwardUser;
  private final Cache<String, UserSession> perUserSessionCache;
  // Keyed by handle identity: GravitinoTransactionHandle has no equals, and each beginTransaction
  // creates a new handle.
  private final Map<GravitinoTransactionHandle, Queue<UserSession>> transactionLeases =
      new ConcurrentHashMap<>();

  /**
   * Constructs a new GravitinoConnector with the specified catalog identifier and catalog connector
   * context.
   *
   * @param catalogConnectorContext the catalog connector context
   */
  public GravitinoConnector(CatalogConnectorContext catalogConnectorContext) {
    this.catalogIdentifier = catalogConnectorContext.getCatalog().geNameIdentifier();
    this.catalogConnectorContext = catalogConnectorContext;
    GravitinoConfig config = catalogConnectorContext.getConfig();
    this.connectorMetadata =
        new CatalogConnectorMetadata(
            catalogConnectorContext.getMetalake(), this.catalogIdentifier, config);

    this.forwardUser = config.isForwardUser();
    this.perUserSessionCache = forwardUser ? buildSessionCache(config) : null;
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    Preconditions.checkArgument(internalConnector != null, "Internal connector must not be null");

    ConnectorTransactionHandle internalTransactionHandler =
        internalConnector.beginTransaction(isolationLevel, readOnly, autoCommit);
    Preconditions.checkArgument(
        internalTransactionHandler != null, "Transaction handler must not be null");

    GravitinoTransactionHandle transactionHandle =
        new GravitinoTransactionHandle(internalTransactionHandler);
    if (forwardUser) {
      transactionLeases.put(transactionHandle, new ConcurrentLinkedQueue<>());
    }
    return transactionHandle;
  }

  @Override
  public ConnectorMetadata getMetadata(
      ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    GravitinoTransactionHandle gravitinoTransactionHandle =
        (GravitinoTransactionHandle) transactionHandle;

    ConnectorMetadata internalMetadata =
        getInternalMetadata(session, gravitinoTransactionHandle.getInternalHandle());
    Preconditions.checkArgument(internalMetadata != null, "Internal metadata must not be null");

    CatalogConnectorMetadata metadata =
        forwardUser ? leaseSessionMetadata(session, gravitinoTransactionHandle) : connectorMetadata;
    return createGravitinoMetadata(
        metadata, catalogConnectorContext.getMetadataAdapter(), internalMetadata);
  }

  /**
   * Defers native REST authentication until a data operation needs it. Catalog registration uses a
   * password-authenticated management session without a delegated user token.
   *
   * @param session the authenticated query session
   * @param transactionHandle the native transaction handle
   * @return metadata that preserves user authentication at first data access
   */
  protected ConnectorMetadata getInternalMetadata(
      ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    if (ICEBERG_PROVIDER.equals(catalogConnectorContext.getCatalog().getProvider())
        && OAUTH2_PASSTHROUGH.equalsIgnoreCase(
            catalogConnectorContext
                .getInternalConnectorConfig()
                .get(TRINO_ICEBERG_REST_SECURITY))) {
      return DeferredConnectorMetadata.create(
          session,
          currentSession ->
              catalogConnectorContext
                  .getInternalConnector()
                  .getMetadata(currentSession, transactionHandle));
    }
    return catalogConnectorContext.getInternalConnector().getMetadata(session, transactionHandle);
  }

  protected GravitinoMetadata createGravitinoMetadata(
      CatalogConnectorMetadata catalogConnectorMetadata,
      CatalogConnectorMetadataAdapter metadataAdapter,
      ConnectorMetadata internalMetadata) {
    throw new TrinoException(NOT_SUPPORTED, "Should be overridden in subclass");
  }

  @Override
  public Set<Procedure> getProcedures() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    return internalConnector.getProcedures();
  }

  @Override
  public Set<TableProcedureMetadata> getTableProcedures() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    return internalConnector.getTableProcedures();
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return catalogConnectorContext.getTableProperties();
  }

  @Override
  public List<PropertyMetadata<?>> getSessionProperties() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    return internalConnector.getSessionProperties();
  }

  @Override
  public List<PropertyMetadata<?>> getColumnProperties() {
    return catalogConnectorContext.getColumnProperties();
  }

  @Override
  public List<PropertyMetadata<?>> getSchemaProperties() {
    return catalogConnectorContext.getSchemaProperties();
  }

  @Override
  public List<PropertyMetadata<?>> getViewProperties() {
    return catalogConnectorContext.getViewProperties();
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    ConnectorSplitManager splitManager = internalConnector.getSplitManager();
    return new GravitinoSplitManager(splitManager);
  }

  @Override
  public ConnectorPageSourceProvider getPageSourceProvider() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    ConnectorPageSourceProvider internalPageSourceProvider =
        internalConnector.getPageSourceProvider();
    return new GravitinoDataSourceProvider(internalPageSourceProvider);
  }

  @Override
  public ConnectorRecordSetProvider getRecordSetProvider() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    ConnectorRecordSetProvider internalRecordSetProvider = internalConnector.getRecordSetProvider();
    return new GravitinoRecordSetProvider(internalRecordSetProvider);
  }

  @Override
  public ConnectorPageSinkProvider getPageSinkProvider() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    ConnectorPageSinkProvider pageSinkProvider = internalConnector.getPageSinkProvider();
    return new GravitinoPageSinkProvider(pageSinkProvider);
  }

  @Override
  public void commit(ConnectorTransactionHandle transactionHandle) {
    GravitinoTransactionHandle gravitinoTransactionHandle =
        (GravitinoTransactionHandle) transactionHandle;
    try {
      Connector internalConnector = catalogConnectorContext.getInternalConnector();
      internalConnector.commit(gravitinoTransactionHandle.getInternalHandle());
    } finally {
      releaseLeases(gravitinoTransactionHandle);
    }
  }

  @Override
  public void rollback(ConnectorTransactionHandle transactionHandle) {
    GravitinoTransactionHandle gravitinoTransactionHandle =
        (GravitinoTransactionHandle) transactionHandle;
    try {
      Connector internalConnector = catalogConnectorContext.getInternalConnector();
      internalConnector.rollback(gravitinoTransactionHandle.getInternalHandle());
    } finally {
      releaseLeases(gravitinoTransactionHandle);
    }
  }

  @Override
  public ConnectorAccessControl getAccessControl() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    return internalConnector.getAccessControl();
  }

  @Override
  public Set<ConnectorCapabilities> getCapabilities() {
    return catalogConnectorContext.getInternalConnector().getCapabilities();
  }

  @Override
  public ConnectorNodePartitioningProvider getNodePartitioningProvider() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    ConnectorNodePartitioningProvider nodePartitioningProvider =
        internalConnector.getNodePartitioningProvider();
    return new GravitinoNodePartitioningProvider(nodePartitioningProvider);
  }

  public void shutdown() {
    if (forwardUser) {
      perUserSessionCache.invalidateAll();
      transactionLeases.keySet().forEach(this::releaseLeases);
    }
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    internalConnector.shutdown();
    catalogConnectorContext.close();
  }

  @VisibleForTesting
  CatalogConnectorMetadata resolveSessionMetadata(ConnectorSession session) {
    UserSession userSession = resolveUserSession(session);
    return userSession == null ? connectorMetadata : userSession.metadata;
  }

  /**
   * Builds the per-user Gravitino admin client for a forwarded session. Extracted as an overridable
   * seam so tests can substitute the client-building behavior without needing to mock the static
   * {@link GravitinoAuthProvider#buildForSession}.
   *
   * @param session the current Trino connector session
   * @return the per-user Gravitino admin client
   */
  @VisibleForTesting
  GravitinoAdminClient buildAuthClient(ConnectorSession session) {
    return GravitinoAuthProvider.buildForSession(catalogConnectorContext.getConfig(), session);
  }

  @VisibleForTesting
  static String sessionCacheKey(String authType, String user, String token) {
    String tokenPart =
        StringUtils.isBlank(token) ? "" : ":" + Integer.toHexString(token.hashCode());
    return authType + ":" + user + tokenPart;
  }

  private CatalogConnectorMetadata leaseSessionMetadata(
      ConnectorSession session, GravitinoTransactionHandle transactionHandle) {
    while (true) {
      UserSession userSession = resolveUserSession(session);
      if (userSession == null) {
        return connectorMetadata;
      }
      // A session retired after the lookup is no longer in the cache, so the next lookup returns
      // a different session.
      if (userSession.tryLease()) {
        Queue<UserSession> leased =
            transactionLeases.computeIfPresent(
                transactionHandle,
                (handle, sessions) -> {
                  sessions.add(userSession);
                  return sessions;
                });
        if (leased == null) {
          // The transaction has already ended, or did not come from beginTransaction.
          userSession.release();
        }
        return userSession.metadata;
      }
    }
  }

  private void releaseLeases(GravitinoTransactionHandle transactionHandle) {
    Queue<UserSession> sessions = transactionLeases.remove(transactionHandle);
    if (sessions != null) {
      sessions.forEach(UserSession::release);
    }
  }

  /** Returns the cached per-user session, or null when the service identity serves the session. */
  @Nullable
  private UserSession resolveUserSession(ConnectorSession session) {
    String authType =
        catalogConnectorContext
            .getConfig()
            .getClientConfig()
            .getOrDefault(GravitinoAuthProvider.AUTH_TYPE_KEY, "simple");
    String credentialKey =
        catalogConnectorContext
            .getConfig()
            .getClientConfig()
            .getOrDefault(
                GravitinoAuthProvider.USER_TOKEN_CREDENTIAL_KEY,
                GravitinoAuthProvider.DEFAULT_USER_TOKEN_CREDENTIAL_KEY);
    String token = session.getIdentity().getExtraCredentials().get(credentialKey);
    // Password-authenticated sessions have no OAuth token. Reuse the configured service
    // identity in that case; failures with a supplied token must still propagate.
    if (GravitinoAuthProvider.parseAuthType(authType) == GravitinoAuthProvider.AuthType.OAUTH2
        && StringUtils.isBlank(token)) {
      return null;
    }
    String credKey = sessionCacheKey(authType, session.getUser(), token);
    try {
      return perUserSessionCache.get(
              credKey,
              () -> {
                GravitinoAdminClient userClient;
                try {
                  userClient = buildAuthClient(session);
                } catch (IllegalArgumentException | UnsupportedOperationException e) {
                  // buildAuthClient raises these for an invalid or unsupported credential.
                  throw new TrinoException(
                      PERMISSION_DENIED,
                      "Failed to authenticate user '"
                          + session.getUser()
                          + "' with Gravitino: "
                          + e.getMessage(),
                      e);
                }
                GravitinoMetalake userMetalake =
                    userClient.loadMetalake(catalogConnectorContext.getMetalake().name());
                return new UserSession(
                    userClient,
                    new CatalogConnectorMetadata(
                        userMetalake, catalogIdentifier, catalogConnectorContext.getConfig()));
              });
    } catch (ExecutionException | UncheckedExecutionException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      LOG.warn(
          cause, "Failed to create per-user Gravitino client for user '%s'", session.getUser());
      if (cause instanceof Exception) {
        throw GravitinoErrorCode.toTrinoException(
            (Exception) cause,
            "Failed to create per-user Gravitino client for user '" + session.getUser() + "'");
      }
      throw new TrinoException(
          GENERIC_INTERNAL_ERROR,
          "Unexpected error while creating per-user Gravitino client for user '"
              + session.getUser()
              + "': "
              + cause.getMessage(),
          cause);
    }
  }

  private Cache<String, UserSession> buildSessionCache(GravitinoConfig config) {
    Map<String, String> clientConfig = config.getClientConfig();
    String authTypeStr = clientConfig.get(GravitinoAuthProvider.AUTH_TYPE_KEY);
    if (StringUtils.isBlank(authTypeStr)) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          "gravitino.client.session.forwardUser=true requires gravitino.client.authType to be set");
    }
    GravitinoAuthProvider.AuthType authType = GravitinoAuthProvider.parseAuthType(authTypeStr);
    if (authType != GravitinoAuthProvider.AuthType.SIMPLE
        && authType != GravitinoAuthProvider.AuthType.OAUTH2) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          "gravitino.client.session.forwardUser=true only supports authType=simple or oauth2, got: "
              + authTypeStr);
    }

    long maxSize = config.getSessionCacheMaxSize();
    long expireAfterAccessSeconds = config.getSessionCacheExpireAfterAccessSeconds();
    if (maxSize < 1 || expireAfterAccessSeconds < 1) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          GravitinoAuthProvider.SESSION_CACHE_MAX_SIZE_KEY
              + " and "
              + GravitinoAuthProvider.SESSION_CACHE_EXPIRE_AFTER_ACCESS_SECONDS_KEY
              + " must be positive, got "
              + maxSize
              + " and "
              + expireAfterAccessSeconds);
    }

    return CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .expireAfterAccess(expireAfterAccessSeconds, TimeUnit.SECONDS)
        .removalListener(
            (RemovalNotification<String, UserSession> notification) -> {
              UserSession session = notification.getValue();
              if (session != null) {
                session.retire();
              }
            })
        .build();
  }

  /**
   * Holds a per-user {@link GravitinoAdminClient} together with its derived metadata. The client is
   * closed once the session is retired from the cache and no transaction leases it.
   */
  @VisibleForTesting
  static final class UserSession {
    final GravitinoAdminClient client;
    final CatalogConnectorMetadata metadata;
    private int leases = 0;
    private boolean retired = false;
    private boolean closed = false;

    UserSession(GravitinoAdminClient client, CatalogConnectorMetadata metadata) {
      this.client = client;
      this.metadata = metadata;
    }

    /** Returns false if the session is retired, in which case it can no longer be leased. */
    synchronized boolean tryLease() {
      if (retired) {
        return false;
      }
      leases++;
      return true;
    }

    synchronized void release() {
      leases--;
      closeIfUnused();
    }

    synchronized void retire() {
      retired = true;
      closeIfUnused();
    }

    private void closeIfUnused() {
      if (!retired || leases > 0 || closed) {
        return;
      }
      closed = true;
      try {
        client.close();
      } catch (Exception e) {
        LOG.warn(e, "Failed to close GravitinoAdminClient");
      }
    }
  }
}
