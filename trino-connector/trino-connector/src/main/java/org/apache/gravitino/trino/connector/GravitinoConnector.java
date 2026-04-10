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

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
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
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

  private final NameIdentifier catalogIdentifier;
  protected final CatalogConnectorContext catalogConnectorContext;
  private final CatalogConnectorMetadata connectorMetadata;
  private final boolean forwardUser;
  private final String authType;
  private final String oauth2CredentialKey;
  private final Cache<String, UserSession> perUserSessionCache;

  /**
   * Constructs a new GravitinoConnector with the specified catalog identifier and catalog connector
   * context.
   *
   * @param catalogConnectorContext the catalog connector context
   */
  public GravitinoConnector(CatalogConnectorContext catalogConnectorContext) {
    this.catalogIdentifier = catalogConnectorContext.getCatalog().geNameIdentifier();
    this.catalogConnectorContext = catalogConnectorContext;
    this.connectorMetadata =
        new CatalogConnectorMetadata(catalogConnectorContext.getMetalake(), this.catalogIdentifier);

    GravitinoConfig config = catalogConnectorContext.getConfig();
    Map<String, String> clientConfig = config.getClientConfig();
    this.forwardUser =
        Boolean.parseBoolean(
            clientConfig.getOrDefault(GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "false"));
    this.authType =
        clientConfig.getOrDefault(GravitinoAuthProvider.AUTH_TYPE_KEY, "").toUpperCase(Locale.ROOT);
    this.oauth2CredentialKey = clientConfig.get(GravitinoAuthProvider.OAUTH2_TOKEN_CREDENTIAL_KEY);

    if (forwardUser) {
      this.perUserSessionCache =
          CacheBuilder.newBuilder()
              .maximumSize(500)
              .expireAfterAccess(1, TimeUnit.HOURS)
              .removalListener(
                  new RemovalListener<String, UserSession>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, UserSession> notification) {
                      notification.getValue().close();
                    }
                  })
              .build();
    } else {
      this.perUserSessionCache = null;
    }
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

    return new GravitinoTransactionHandle(internalTransactionHandler);
  }

  @Override
  public ConnectorMetadata getMetadata(
      ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    GravitinoTransactionHandle gravitinoTransactionHandle =
        (GravitinoTransactionHandle) transactionHandle;

    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    ConnectorMetadata internalMetadata =
        internalConnector.getMetadata(session, gravitinoTransactionHandle.getInternalHandle());
    Preconditions.checkArgument(internalMetadata != null, "Internal metadata must not be null");

    if (forwardUser) {
      String credKey =
          authType.equals("OAUTH2")
              ? "oauth2:"
                  + session
                      .getIdentity()
                      .getExtraCredentials()
                      .getOrDefault(oauth2CredentialKey, "")
              : "simple:" + session.getUser();
      UserSession userSession = perUserSessionCache.getIfPresent(credKey);
      if (userSession == null) {
        GravitinoAdminClient userClient =
            GravitinoAuthProvider.buildForSession(catalogConnectorContext.getConfig(), session);
        GravitinoMetalake userMetalake =
            userClient.loadMetalake(catalogConnectorContext.getMetalake().name());
        CatalogConnectorMetadata userMetadata =
            new CatalogConnectorMetadata(userMetalake, catalogIdentifier);
        userSession = new UserSession(userClient, userMetadata);
        perUserSessionCache.put(credKey, userSession);
      }
      return createGravitinoMetadata(
          userSession.metadata, catalogConnectorContext.getMetadataAdapter(), internalMetadata);
    }

    return createGravitinoMetadata(
        connectorMetadata, catalogConnectorContext.getMetadataAdapter(), internalMetadata);
  }

  protected GravitinoMetadata createGravitinoMetadata(
      CatalogConnectorMetadata catalogConnectorMetadata,
      CatalogConnectorMetadataAdapter metadataAdapter,
      ConnectorMetadata internalMetadata) {
    throw new TrinoException(NOT_SUPPORTED, "Should be overridden in subclass");
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
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    internalConnector.commit(gravitinoTransactionHandle.getInternalHandle());
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
    }
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    internalConnector.shutdown();
    catalogConnectorContext.close();
  }

  /** Holds a per-user {@link GravitinoAdminClient} together with its derived metadata. */
  private static final class UserSession {
    final GravitinoAdminClient client;
    final CatalogConnectorMetadata metadata;

    UserSession(GravitinoAdminClient client, CatalogConnectorMetadata metadata) {
      this.client = client;
      this.metadata = metadata;
    }

    void close() {
      try {
        client.close();
      } catch (Exception e) {
        throw new RuntimeException("Failed to close GravitinoAdminClient", e);
      }
    }
  }
}
