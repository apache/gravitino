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
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.ConnectorIdentity;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.apache.gravitino.trino.connector.security.GravitinoAuthProvider;
import org.junit.jupiter.api.Test;

/** Tests for forwardUser startup validation in {@link GravitinoConnector}. */
class TestGravitinoConnectorForwardUser {

  @Test
  void testPasswordSessionUsesServiceMetadataForCatalogLifecycleAndQueries() {
    CatalogConnectorContext ctx =
        mockContextWithConfig(
            ImmutableMap.of(
                GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true",
                GravitinoAuthProvider.AUTH_TYPE_KEY, "oauth2"));
    Connector internal = mock(Connector.class);
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    when(ctx.getInternalConnector()).thenReturn(internal);
    when(internal.getMetadata(any(), any())).thenReturn(internalMetadata);
    GravitinoConnector connector =
        new GravitinoConnector(ctx) {
          @Override
          protected GravitinoMetadata createGravitinoMetadata(
              CatalogConnectorMetadata metadata,
              CatalogConnectorMetadataAdapter adapter,
              ConnectorMetadata delegate) {
            return new GravitinoMetadata(metadata, adapter, delegate) {};
          }
        };
    ConnectorSession session = mockSession("gravitino_catalog_manager", "");
    GravitinoTransactionHandle transaction =
        new GravitinoTransactionHandle(mock(ConnectorTransactionHandle.class));
    ConnectorMetadata metadata =
        assertDoesNotThrow(() -> connector.getMetadata(session, transaction));
    assertDoesNotThrow(() -> metadata.beginQuery(session));
    assertDoesNotThrow(() -> metadata.cleanupQuery(session));
    verify(internalMetadata).beginQuery(session);
    verify(internalMetadata).cleanupQuery(session);
    SupportsSchemas schemas = ctx.getMetalake().loadCatalog("catalog").asSchemas();
    when(schemas.listSchemas()).thenReturn(new String[] {"test_schema"});
    assertEquals(List.of("test_schema"), metadata.listSchemaNames(session));
    verify(schemas).listSchemas();
    verify(internalMetadata, never()).listSchemaNames(any());
  }

  @Test
  void testMissingOAuthTokenReusesServiceMetadataWithoutBuildingUserClient() {
    CatalogConnectorContext ctx =
        mockContextWithConfig(
            ImmutableMap.of(
                GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true",
                GravitinoAuthProvider.AUTH_TYPE_KEY, "oauth2"));
    GravitinoConnector connector =
        newConnectorWithAuthClient(
            ctx,
            session -> {
              throw new AssertionError("A missing token must not create a user client");
            });
    CatalogConnectorMetadata service =
        connector.resolveSessionMetadata(mockSession("manager", null));
    assertSame(service, connector.resolveSessionMetadata(mockSession("alice", "")));
    assertSame(service, connector.resolveSessionMetadata(mockSession("bob", "  ")));
  }

  @Test
  void testCustomTokenKeyControlsForwardingAndFallback() {
    CatalogConnectorContext ctx =
        mockContextWithConfig(
            ImmutableMap.of(
                GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true",
                GravitinoAuthProvider.AUTH_TYPE_KEY, "oauth2",
                GravitinoAuthProvider.USER_TOKEN_CREDENTIAL_KEY, "custom-token"));
    AtomicInteger count = new AtomicInteger();
    GravitinoConnector connector =
        newConnectorWithAuthClient(ctx, session -> mockAdminClient(ctx.getMetalake(), count));
    ConnectorSession session = mockSession("alice", "ignored-default-token");
    CatalogConnectorMetadata service = connector.resolveSessionMetadata(session);
    assertEquals(0, count.get());
    when(session.getIdentity().getExtraCredentials())
        .thenReturn(ImmutableMap.of("custom-token", "user-token"));
    assertNotSame(service, connector.resolveSessionMetadata(session));
    assertEquals(1, count.get());
  }

  @Test
  void testSimpleAuthStillForwardsUsernameWithoutToken() {
    CatalogConnectorContext ctx =
        mockContextWithConfig(
            ImmutableMap.of(
                GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true",
                GravitinoAuthProvider.AUTH_TYPE_KEY, "simple"));
    AtomicInteger count = new AtomicInteger();
    GravitinoConnector connector =
        newConnectorWithAuthClient(ctx, session -> mockAdminClient(ctx.getMetalake(), count));
    assertNotSame(
        connector.resolveSessionMetadata(mockSession("alice", null)),
        connector.resolveSessionMetadata(mockSession("bob", null)));
    assertEquals(2, count.get());
  }

  @Test
  void testForwardUserWithoutAuthTypeThrowsAtConstruction() {
    CatalogConnectorContext ctx =
        mockContextWithConfig(
            ImmutableMap.of(GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true"));

    TrinoException ex = assertThrows(TrinoException.class, () -> new GravitinoConnector(ctx));
    assertEquals(GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), ex.getErrorCode());
  }

  @Test
  void testForwardUserWithKerberosAuthTypeThrowsAtConstruction() {
    CatalogConnectorContext ctx =
        mockContextWithConfig(
            ImmutableMap.of(
                GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true",
                GravitinoAuthProvider.AUTH_TYPE_KEY, "kerberos"));

    TrinoException ex = assertThrows(TrinoException.class, () -> new GravitinoConnector(ctx));
    assertEquals(GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), ex.getErrorCode());
  }

  @Test
  void testForwardUserWithOAuth2AuthTypeSucceeds() {
    CatalogConnectorContext ctx =
        mockContextWithConfig(
            ImmutableMap.of(
                GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true",
                GravitinoAuthProvider.AUTH_TYPE_KEY, "oauth2"));

    assertDoesNotThrow(() -> new GravitinoConnector(ctx));
  }

  @Test
  void testForwardUserWithSimpleAuthTypeSucceeds() {
    CatalogConnectorContext ctx =
        mockContextWithConfig(
            ImmutableMap.of(
                GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true",
                GravitinoAuthProvider.AUTH_TYPE_KEY, "simple"));

    assertDoesNotThrow(() -> new GravitinoConnector(ctx));
  }

  @Test
  void testForwardUserFalseWithNoAuthTypeSucceeds() {
    CatalogConnectorContext ctx = mockContextWithConfig(ImmutableMap.of());
    assertDoesNotThrow(() -> new GravitinoConnector(ctx));
  }

  @Test
  void testSessionCacheKeyIsolatesDifferentUsers() {
    assertNotEquals(
        GravitinoConnector.sessionCacheKey("oauth2", "alice", "tok"),
        GravitinoConnector.sessionCacheKey("oauth2", "bob", "tok"));
  }

  @Test
  void testSessionCacheKeyIsolatesDifferentAuthTypes() {
    assertNotEquals(
        GravitinoConnector.sessionCacheKey("oauth2", "alice", "tok"),
        GravitinoConnector.sessionCacheKey("simple", "alice", "tok"));
  }

  @Test
  void testSessionCacheKeyIsolatesDifferentTokens() {
    assertNotEquals(
        GravitinoConnector.sessionCacheKey("oauth2", "alice", "token-a"),
        GravitinoConnector.sessionCacheKey("oauth2", "alice", "token-b"));
  }

  @Test
  void testSessionCacheKeyIsStableForSameUserAuthTypeAndToken() {
    assertEquals(
        GravitinoConnector.sessionCacheKey("oauth2", "alice", "tok"),
        GravitinoConnector.sessionCacheKey("oauth2", "alice", "tok"));
  }

  @Test
  void testSessionCacheKeyIgnoresBlankTokenForSimpleAuth() {
    assertEquals(
        GravitinoConnector.sessionCacheKey("simple", "alice", null),
        GravitinoConnector.sessionCacheKey("simple", "alice", ""));
  }

  @Test
  void testResolveSessionMetadataBuildsNewClientOnTokenRotation() {
    CatalogConnectorContext ctx =
        mockContextWithConfig(
            ImmutableMap.of(
                GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true",
                GravitinoAuthProvider.AUTH_TYPE_KEY, "oauth2"));
    AtomicInteger buildCount = new AtomicInteger();
    GravitinoConnector connector =
        newConnectorWithAuthClient(ctx, session -> mockAdminClient(ctx.getMetalake(), buildCount));

    CatalogConnectorMetadata first =
        connector.resolveSessionMetadata(mockSession("alice", "token-a"));
    CatalogConnectorMetadata second =
        connector.resolveSessionMetadata(mockSession("alice", "token-a"));
    CatalogConnectorMetadata third =
        connector.resolveSessionMetadata(mockSession("alice", "token-b"));

    assertSame(first, second, "same user/token should reuse the cached client");
    assertNotSame(first, third, "a rotated token must not reuse the stale cached client");
    assertEquals(2, buildCount.get(), "a rotated token must trigger a fresh client build");
  }

  @Test
  void testResolveSessionMetadataMapsAuthSpecificFailureToPermissionDenied() {
    CatalogConnectorContext ctx =
        mockContextWithConfig(
            ImmutableMap.of(
                GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true",
                GravitinoAuthProvider.AUTH_TYPE_KEY, "oauth2"));
    GravitinoConnector connector =
        newConnectorWithAuthClient(
            ctx,
            session -> {
              throw new IllegalArgumentException("No forwarded user token found");
            });

    TrinoException ex =
        assertThrows(
            TrinoException.class,
            () -> connector.resolveSessionMetadata(mockSession("alice", "token-a")));
    assertEquals(PERMISSION_DENIED.toErrorCode(), ex.getErrorCode());
  }

  @Test
  void testResolveSessionMetadataPreservesTrinoExceptionErrorCode() {
    CatalogConnectorContext ctx =
        mockContextWithConfig(
            ImmutableMap.of(
                GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true",
                GravitinoAuthProvider.AUTH_TYPE_KEY, "oauth2"));
    GravitinoConnector connector =
        newConnectorWithAuthClient(
            ctx,
            session -> {
              throw new TrinoException(
                  GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT, "No forwarded user token found");
            });

    TrinoException ex =
        assertThrows(
            TrinoException.class,
            () -> connector.resolveSessionMetadata(mockSession("alice", "token-a")));
    assertEquals(GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), ex.getErrorCode());
  }

  @Test
  void testResolveSessionMetadataMapsUnexpectedFailureToGenericInternalError() {
    CatalogConnectorContext ctx =
        mockContextWithConfig(
            ImmutableMap.of(
                GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true",
                GravitinoAuthProvider.AUTH_TYPE_KEY, "oauth2"));
    GravitinoConnector connector =
        newConnectorWithAuthClient(
            ctx,
            session -> {
              throw new RuntimeException("connection refused");
            });

    TrinoException ex =
        assertThrows(
            TrinoException.class,
            () -> connector.resolveSessionMetadata(mockSession("alice", "token-a")));
    assertEquals(GENERIC_INTERNAL_ERROR.toErrorCode(), ex.getErrorCode());
  }

  /**
   * Creates a {@link GravitinoConnector} whose {@code buildAuthClient} is overridden with the given
   * function, so tests can control what building the per-user client does without mocking the
   * static {@link GravitinoAuthProvider#buildForSession}.
   */
  private static GravitinoConnector newConnectorWithAuthClient(
      CatalogConnectorContext ctx,
      Function<ConnectorSession, GravitinoAdminClient> authClientBuilder) {
    return new GravitinoConnector(ctx) {
      @Override
      GravitinoAdminClient buildAuthClient(ConnectorSession session) {
        return authClientBuilder.apply(session);
      }
    };
  }

  private static GravitinoAdminClient mockAdminClient(
      GravitinoMetalake metalake, AtomicInteger buildCount) {
    buildCount.incrementAndGet();
    GravitinoAdminClient client = mock(GravitinoAdminClient.class);
    when(client.loadMetalake(any())).thenReturn(metalake);
    return client;
  }

  private static ConnectorSession mockSession(String user, String token) {
    ConnectorIdentity identity = mock(ConnectorIdentity.class);
    when(identity.getExtraCredentials())
        .thenReturn(token == null ? ImmutableMap.of() : ImmutableMap.of("token", token));
    ConnectorSession session = mock(ConnectorSession.class);
    when(session.getUser()).thenReturn(user);
    when(session.getIdentity()).thenReturn(identity);
    return session;
  }

  private static CatalogConnectorContext mockContextWithConfig(
      ImmutableMap<String, String> extraConfig) {
    GravitinoCatalog mockCatalog = mock(GravitinoCatalog.class);
    when(mockCatalog.geNameIdentifier()).thenReturn(NameIdentifier.of("metalake", "catalog"));

    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    Catalog catalog = mock(Catalog.class);
    when(catalog.asSchemas()).thenReturn(mock(SupportsSchemas.class));
    when(catalog.asTableCatalog()).thenReturn(mock(TableCatalog.class));
    when(metalake.loadCatalog(any())).thenReturn(catalog);

    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put("gravitino.uri", "http://localhost:8090")
            .put("gravitino.metalake", "test");
    builder.putAll(extraConfig);
    GravitinoConfig config = new GravitinoConfig(builder.build());

    CatalogConnectorContext ctx = mock(CatalogConnectorContext.class);
    when(ctx.getCatalog()).thenReturn(mockCatalog);
    when(ctx.getMetalake()).thenReturn(metalake);
    when(ctx.getConfig()).thenReturn(config);
    return ctx;
  }
}
