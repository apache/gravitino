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
package org.apache.gravitino.paimon.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import javax.ws.rs.core.Response;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.paimon.common.PaimonCatalogWrapper;
import org.apache.gravitino.paimon.service.PaimonCatalogWrapperManager.CatalogLease;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProvider;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProvider.CatalogConfig;
import org.apache.gravitino.paimon.service.rest.PaimonTableOperations;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.ServiceUnavailableException;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

/** Tests static catalog routing and credential forwarding without schema access. */
public class TestPaimonCatalogWrapperManager {
  @TempDir Path warehouse;

  @Test
  void testIdleDynamicBackendsAreClosedWithoutAnotherRequest() throws Exception {
    PaimonConfigProvider provider = mock(PaimonConfigProvider.class);
    when(provider.getMetalakeName()).thenReturn(Optional.of("ml"));
    when(provider.getCatalogConfig("sales")).thenReturn(Optional.of(config(Map.of())));
    Clock clock = mock(Clock.class);
    AtomicLong now = new AtomicLong(1000);
    when(clock.millis()).thenAnswer(invocation -> now.get());
    try (MockedConstruction<PaimonCatalogWrapper> wrappers =
            Mockito.mockConstruction(PaimonCatalogWrapper.class);
        PaimonCatalogWrapperManager manager =
            new PaimonCatalogWrapperManager(provider, clock, 100000)) {
      PaimonCatalogWrapper backend;
      try (CatalogLease lease = manager.acquire("sales")) {
        backend = lease.wrapper();
      }
      manager.cleanUpExpiredCatalogs();
      Mockito.verify(backend, never()).close();
      now.addAndGet(100001);
      manager.cleanUpExpiredCatalogs();
      Mockito.verify(backend).close();
      assertEquals(1, wrappers.constructed().size());
    }
  }

  @Test
  void testRetiredBackendLivesUntilItsLastRequestFinishes() throws Exception {
    PaimonConfigProvider provider = mock(PaimonConfigProvider.class);
    when(provider.getMetalakeName()).thenReturn(Optional.of("ml"));
    when(provider.getCatalogConfig("sales")).thenReturn(Optional.of(config(Map.of())));
    Clock clock = mock(Clock.class);
    AtomicLong now = new AtomicLong(1000);
    when(clock.millis()).thenAnswer(invocation -> now.get());
    try (MockedConstruction<PaimonCatalogWrapper> wrappers =
        Mockito.mockConstruction(PaimonCatalogWrapper.class)) {
      PaimonCatalogWrapperManager manager = new PaimonCatalogWrapperManager(provider, clock, 100);
      CatalogLease first = manager.acquire("sales");
      now.addAndGet(101);
      CatalogLease second = manager.acquire("sales");
      Mockito.verify(first.wrapper(), never()).close();
      first.close();
      first.close();
      Mockito.verify(first.wrapper()).close();
      manager.close();
      Mockito.verify(second.wrapper(), never()).close();
      second.close();
      Mockito.verify(second.wrapper()).close();
      manager.close();
      assertThrows(IllegalStateException.class, () -> manager.acquire("sales"));
      assertEquals(2, wrappers.constructed().size());
    }
  }

  @Test
  void testDynamicCacheTtlInvalidationAndCallerIsolation() throws Exception {
    PaimonConfigProvider provider = mock(PaimonConfigProvider.class);
    when(provider.getMetalakeName()).thenReturn(Optional.of("ml"));
    when(provider.getCatalogConfig("sales")).thenReturn(Optional.of(config(Map.of())));
    Clock clock = mock(Clock.class);
    AtomicLong now = new AtomicLong(10000000);
    when(clock.millis()).thenAnswer(invocation -> now.get());
    try (PaimonCatalogWrapperManager manager =
        new PaimonCatalogWrapperManager(provider, clock, 1000)) {
      PrincipalUtils.doAs(
          new UserPrincipal("alice"),
          () -> {
            try (CatalogLease first = manager.acquire("sales");
                CatalogLease repeated = manager.acquire("sales")) {
              assertTrue(first.wrapper() == repeated.wrapper());
              String changedWarehouse = warehouse.resolve("changed").toUri().toString();
              when(provider.getCatalogConfig("sales"))
                  .thenAnswer(
                      invocation ->
                          Optional.of(
                              config(
                                  Map.of(
                                      "warehouse",
                                      changedWarehouse,
                                      "s3.access-key",
                                      PrincipalUtils.getCurrentUserName()))));
              now.addAndGet(1001);
              PaimonCatalogWrapper aliceBackend;
              try (CatalogLease refreshed = manager.acquire("sales")) {
                assertTrue(first.wrapper() != refreshed.wrapper());
                assertEquals(
                    changedWarehouse, refreshed.wrapper().catalogOptions().get("warehouse"));
                assertEquals("alice", refreshed.wrapper().catalogOptions().get("s3.access-key"));
                aliceBackend = refreshed.wrapper();
              }
              PrincipalUtils.doAs(
                  new UserPrincipal("bob"),
                  () -> {
                    try (CatalogLease other = manager.acquire("sales")) {
                      assertTrue(aliceBackend != other.wrapper());
                      assertEquals("bob", other.wrapper().catalogOptions().get("s3.access-key"));
                    }
                    return null;
                  });
              manager.invalidate("sales");
              when(provider.getCatalogConfig("sales")).thenReturn(Optional.empty());
              assertThrows(NoSuchResourceException.class, () -> manager.acquire("sales"));
            }
            return null;
          });
    }
  }

  @Test
  void testTokenExpiryRefreshBoundaryAndShortLivedCachedCredentials() throws Exception {
    PaimonConfigProvider provider = mock(PaimonConfigProvider.class);
    when(provider.getMetalakeName()).thenReturn(Optional.of("ml"));
    Clock clock = mock(Clock.class);
    AtomicLong now = new AtomicLong(10000000);
    when(clock.millis()).thenAnswer(invocation -> now.get());
    long firstExpiry = now.get() + 7200000;
    when(provider.getCatalogConfig("sales"))
        .thenReturn(
            Optional.of(
                new CatalogConfig(
                    config(Map.of("data-token.enabled", "true", "s3.session-token", "session"))
                        .properties(),
                    firstExpiry)));
    try (PaimonCatalogWrapperManager manager =
        new PaimonCatalogWrapperManager(provider, clock, 10000000)) {
      try (CatalogLease first = manager.acquire("sales")) {
        assertEquals(firstExpiry, first.expiresAtMillis());
        now.addAndGet(3539999);
        try (CatalogLease same = manager.acquire("sales")) {
          assertTrue(first.wrapper() == same.wrapper());
        }
        now.incrementAndGet();
        ServiceUnavailableException error =
            assertThrows(ServiceUnavailableException.class, () -> manager.acquire("sales"));
        assertTrue(error.getMessage().contains("s3-token-expire-in-secs"));
        assertTrue(error.getMessage().contains("oss-token-expire-in-secs"));
        when(provider.getCatalogConfig("sales"))
            .thenReturn(
                Optional.of(
                    new CatalogConfig(
                        config(
                                Map.of(
                                    "data-token.enabled",
                                    "true",
                                    "s3.session-token",
                                    "new-session"))
                            .properties(),
                        now.get() + 7200000)));
        try (CatalogLease refreshed = manager.acquire("sales")) {
          assertTrue(first.wrapper() != refreshed.wrapper());
          assertEquals("new-session", refreshed.wrapper().catalogOptions().get("s3.session-token"));
        }
      }
    }
  }

  @Test
  void testStaticCatalogIsReused() throws Exception {
    PaimonConfigProvider provider = mock(PaimonConfigProvider.class);
    when(provider.getCatalogConfig("sales")).thenReturn(Optional.of(config(Map.of())));
    try (PaimonCatalogWrapperManager manager = new PaimonCatalogWrapperManager(provider);
        CatalogLease first = manager.acquire("sales");
        CatalogLease second = manager.acquire("sales")) {
      assertSame(first.wrapper(), second.wrapper());
      verify(provider).getCatalogConfig("sales");
    }
    verify(provider).close();
  }

  @Test
  void testMissingCatalogDoesNotLoadDefaultAndLookupCanBeRetried() throws Exception {
    PaimonConfigProvider provider = mock(PaimonConfigProvider.class);
    when(provider.getCatalogConfig("sales"))
        .thenThrow(new ServiceUnavailableException("upstream unavailable"))
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(config(Map.of())));
    try (PaimonCatalogWrapperManager manager = new PaimonCatalogWrapperManager(provider)) {
      assertThrows(ServiceUnavailableException.class, () -> manager.acquire("sales"));
      assertThrows(NoSuchResourceException.class, () -> manager.acquire("sales"));
      try (CatalogLease lease = manager.acquire("sales")) {
        assertEquals("sales", lease.wrapper().name());
      }
      verify(provider, never()).getCatalogConfig("default_catalog");
    }
  }

  @Test
  void testStaticDataTokensRejectTemporaryCredentials() throws Exception {
    for (String tokenKey :
        new String[] {
          "s3.session-token",
          "s3-session-token",
          "s3.session.token",
          "s3a.session-token",
          "s3a.session.token",
          "fs.s3a.session-token",
          "fs.s3a.session.token",
          "fs.oss.securityToken",
          "fs.oss.securitytoken",
          "fs.oss.SecurityToken",
          "fs.oss.SECURITYTOKEN",
          "oss-security-token"
        }) {
      PaimonConfigProvider provider = mock(PaimonConfigProvider.class);
      when(provider.getCatalogConfig("sales"))
          .thenReturn(
              Optional.of(config(Map.of("data-token.enabled", "true", tokenKey, "temporary"))));
      try (PaimonCatalogWrapperManager manager = new PaimonCatalogWrapperManager(provider)) {
        IllegalArgumentException error =
            assertThrows(IllegalArgumentException.class, () -> manager.acquire("sales"));
        assertTrue(error.getMessage().contains("actual expiry"));
      }
    }
  }

  @Test
  void testTemporaryBackendCredentialsWithoutDataTokens() throws Exception {
    PaimonConfigProvider provider = mock(PaimonConfigProvider.class);
    when(provider.getCatalogConfig("sales"))
        .thenReturn(Optional.of(config(Map.of("s3.session-token", "temporary"))));
    try (PaimonCatalogWrapperManager manager = new PaimonCatalogWrapperManager(provider);
        CatalogLease lease = manager.acquire("sales")) {
      assertEquals("temporary", lease.wrapper().catalogOptions().get("s3.session-token"));
    }
  }

  @Test
  void testRepeatedTokensDoNotLoadTableSchema() throws Exception {
    PaimonCatalogRequest catalogs = mock(PaimonCatalogRequest.class);
    CatalogLease lease = mock(CatalogLease.class);
    PaimonCatalogWrapper wrapper = mock(PaimonCatalogWrapper.class);
    when(catalogs.acquire("sales")).thenReturn(lease);
    when(lease.wrapper()).thenReturn(wrapper);
    when(wrapper.catalogOptions())
        .thenReturn(
            Options.fromMap(
                Map.of(
                    "data-token.enabled",
                    "true",
                    "s3.access-key",
                    "key",
                    "s3.secret-key",
                    "secret")));
    PaimonTableOperations operations = new PaimonTableOperations(catalogs);
    long before = System.currentTimeMillis();
    for (int i = 0; i < 3; i++) {
      try (Response response = operations.loadTableToken("sales/", "db", "events")) {
        GetTableTokenResponse token =
            RESTApi.fromJson((String) response.getEntity(), GetTableTokenResponse.class);
        assertEquals(Map.of("s3.access-key", "key", "s3.secret-key", "secret"), token.getToken());
        assertTrue(
            token.getExpiresAtMillis() >= before + 2 * RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS);
      }
    }
    verify(wrapper, never()).getFileTable(any(Identifier.class));
    verify(wrapper, never()).getTable(any(Identifier.class));
  }

  private CatalogConfig config(Map<String, String> extra) {
    Map<String, String> options = new HashMap<>(extra);
    options.putIfAbsent("warehouse", warehouse.toUri().toString());
    return new CatalogConfig(options);
  }
}
