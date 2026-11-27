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
package org.apache.gravitino.paimon.service.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialOperationDispatcher;
import org.apache.gravitino.credential.DlfSecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProvider.CatalogConfig;
import org.apache.gravitino.secret.SupportsSecrets;
import org.apache.gravitino.utils.ThrowableFunction;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.exceptions.ServiceUnavailableException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/** Tests authenticated catalog resolution, conversion, expiry and auxiliary invalidation. */
public class TestDynamicPaimonConfigProvider {
  @ParameterizedTest
  @ValueSource(ints = {500, 503})
  void testRealHttpUpstreamFailuresReturnSanitizedUnavailable(int status) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          boolean version = exchange.getRequestURI().getPath().equals("/api/version");
          String body =
              version
                  ? "{\"code\":0,\"version\":{\"version\":\"2.0.0-SNAPSHOT\",\"compileDate\":\"test\",\"gitCommit\":\"test\"}}"
                  : "{\"code\":"
                      + (status == 503 ? 1013 : 1002)
                      + ",\"type\":\"RuntimeException\",\"message\":\"private-upstream-detail\"}";
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(version ? 200 : status, bytes.length);
          try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
          }
        });
    server.start();
    try (DynamicPaimonConfigProvider provider = new DynamicPaimonConfigProvider(false)) {
      provider.initialize(
          Map.of("gravitino-uri", "http://127.0.0.1:" + server.getAddress().getPort()));
      ServiceUnavailableException failure =
          assertThrows(ServiceUnavailableException.class, () -> provider.getCatalogConfig("sales"));
      assertFalse(failure.getMessage().contains("private-upstream-detail"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void testPropertiesSecretsAndExpiringCredentials() throws Exception {
    GravitinoClient client = mock(GravitinoClient.class);
    Catalog catalog = mock(Catalog.class);
    when(client.loadCatalog("sales")).thenReturn(catalog);
    when(catalog.provider()).thenReturn("lakehouse-paimon");
    when(catalog.properties())
        .thenReturn(
            Map.of(
                "catalog-backend",
                "filesystem",
                "jdbc-password",
                "******",
                "gravitino.bypass.lock.enabled",
                "true",
                "token-provider",
                "bear",
                "dlf-token-path",
                "/token",
                "gravitino.bypass.data-token.enabled",
                "true",
                "ignored",
                "ignored"));
    SupportsSecrets secrets = mock(SupportsSecrets.class);
    when(catalog.supportsSecrets()).thenReturn(secrets);
    when(secrets.getSecrets()).thenReturn(Map.of("jdbc-password", "resolved-password"));
    SupportsCredentials credentials = mock(SupportsCredentials.class);
    when(catalog.supportsCredentials()).thenReturn(credentials);
    long expiry = System.currentTimeMillis() + 7200000;
    when(credentials.getCredentials())
        .thenReturn(
            new Credential[] {
              new S3TokenCredential("key", "secret", "session", expiry),
              new DlfSecretKeyCredential("dlf-key", "dlf-secret")
            });
    try (DynamicPaimonConfigProvider provider = new DynamicPaimonConfigProvider(client)) {
      provider.initialize(
          Map.of("gravitino-uri", "http://localhost:1", "gravitino-metalake", "ml"));
      CatalogConfig config = provider.getCatalogConfig("sales").orElseThrow();
      assertEquals("filesystem", config.properties().get("metastore"));
      assertEquals("bear", config.properties().get("token.provider"));
      assertEquals("/token", config.properties().get("dlf.token-path"));
      assertEquals("true", config.properties().get("lock.enabled"));
      assertEquals("true", config.properties().get("data-token.enabled"));
      assertEquals("resolved-password", config.properties().get("jdbc.password"));
      assertEquals("session", config.properties().get("s3.session-token"));
      assertEquals("dlf-secret", config.properties().get("dlf.access-key-secret"));
      assertFalse(config.properties().containsKey("ignored"));
      assertEquals(expiry, config.expiresAtMillis());
    }
    verify(client).close();
  }

  @Test
  void testMissingUnsupportedAndKerberosCatalogs() throws Exception {
    GravitinoClient client = mock(GravitinoClient.class);
    Catalog catalog = mock(Catalog.class);
    when(client.loadCatalog("missing")).thenThrow(new NoSuchCatalogException("missing"));
    when(client.loadCatalog("other")).thenReturn(catalog);
    when(catalog.provider()).thenReturn("hive");
    try (DynamicPaimonConfigProvider provider = new DynamicPaimonConfigProvider(client)) {
      provider.initialize(Map.of("gravitino-uri", "http://localhost:1"));
      assertTrue(provider.getCatalogConfig("missing").isEmpty());
      when(client.loadCatalog("unavailable")).thenThrow(new RESTException("upstream unavailable"));
      assertThrows(
          ServiceUnavailableException.class, () -> provider.getCatalogConfig("unavailable"));
      assertTrue(provider.getCatalogConfig("other").isEmpty());
      when(catalog.provider()).thenReturn("lakehouse-paimon");
      when(catalog.properties()).thenReturn(Map.of("authentication.type", "kerberos"));
      assertThrows(NotImplementedException.class, () -> provider.getCatalogConfig("other"));
    }
  }

  @Test
  void testAuxiliaryLookupUsesLeaseAndRemovesListenerOnClose() throws Exception {
    GravitinoEnv env = mock(GravitinoEnv.class);
    CatalogManager manager = mock(CatalogManager.class);
    CredentialOperationDispatcher credentials = mock(CredentialOperationDispatcher.class);
    BaseCatalog<?> catalog = mock(BaseCatalog.class);
    when(env.catalogManager()).thenReturn(manager);
    when(env.credentialOperationDispatcher()).thenReturn(credentials);
    when(catalog.name()).thenReturn("sales");
    when(catalog.provider()).thenReturn("lakehouse-paimon");
    when(catalog.propertiesWithCredentialProviders())
        .thenReturn(Map.of("catalog-backend", "filesystem"));
    when(credentials.getCredentials(any(), any())).thenReturn(List.of());
    when(manager.doWithCatalog(eq(NameIdentifier.of("ml", "sales")), any()))
        .thenAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog, ?> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            });
    AtomicReference<Consumer<NameIdentifier>> listener = new AtomicReference<>();
    doAnswer(
            invocation -> {
              listener.set(invocation.getArgument(0));
              return null;
            })
        .when(manager)
        .addCatalogCacheRemoveListener(any());
    try (MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class);
        DynamicPaimonConfigProvider provider = new DynamicPaimonConfigProvider(true)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      provider.initialize(Map.of("gravitino-metalake", "ml"));
      assertEquals(
          "filesystem",
          provider.getCatalogConfig("sales").orElseThrow().properties().get("metastore"));
      Consumer<String> invalidate = mock(Consumer.class);
      provider.addInvalidationListener(invalidate);
      listener.get().accept(NameIdentifier.of("other", "sales"));
      verifyNoInteractions(invalidate);
      listener.get().accept(NameIdentifier.of("ml", "sales"));
      verify(invalidate).accept("sales");
    }
    verify(manager).removeCatalogCacheRemoveListener(listener.get());
  }

  @ParameterizedTest
  @ValueSource(strings = {"simple", "oauth2"})
  void testOutgoingAuthenticationHeaders(String authType) throws Exception {
    AtomicReference<String> authorization = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          byte[] body;
          if (exchange.getRequestURI().getPath().equals("/oauth/token")) {
            body =
                "{\"access_token\":\"test-access-token\",\"token_type\":\"Bearer\",\"expires_in\":7200}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
          } else if (exchange.getRequestURI().getPath().equals("/api/version")) {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            body =
                "{\"code\":0,\"version\":{\"version\":\"2.0.0-SNAPSHOT\",\"compileDate\":\"test\",\"gitCommit\":\"test\"}}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
          } else {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            body =
                "{\"code\":1000,\"type\":\"NoSuchCatalogException\",\"message\":\"missing\"}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(404, body.length);
          }
          try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
          }
        });
    server.start();
    String uri = "http://127.0.0.1:" + server.getAddress().getPort();
    try (DynamicPaimonConfigProvider provider = new DynamicPaimonConfigProvider(false)) {
      provider.initialize(
          Map.of(
              "gravitino-uri",
              uri,
              "gravitino-auth-type",
              authType,
              "gravitino-simple.user-name",
              "reader",
              "gravitino-oauth2.server-uri",
              uri,
              "gravitino-oauth2.credential",
              "client:secret",
              "gravitino-oauth2.token-path",
              "oauth/token",
              "gravitino-oauth2.scope",
              "catalog"));
      try (GravitinoClient client =
          DynamicPaimonConfigProvider.createClient(
              Map.of(
                  "gravitino-uri",
                  uri,
                  "gravitino-auth-type",
                  authType,
                  "gravitino-simple.user-name",
                  "reader",
                  "gravitino-oauth2.server-uri",
                  uri,
                  "gravitino-oauth2.credential",
                  "client:secret",
                  "gravitino-oauth2.token-path",
                  "oauth/token",
                  "gravitino-oauth2.scope",
                  "catalog"),
              "ml")) {
        client.serverVersion();
      }
      assertNotNull(authorization.get());
      String expected =
          authType.equals("simple")
              ? "Basic "
                  + Base64.getEncoder()
                      .encodeToString("reader:dummy".getBytes(StandardCharsets.UTF_8))
              : "Bearer test-access-token";
      assertEquals(expected, authorization.get());
    } finally {
      server.stop(0);
    }
  }
}
