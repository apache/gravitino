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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.core.Response;
import org.apache.gravitino.paimon.common.PaimonCatalogWrapper;
import org.apache.gravitino.paimon.service.PaimonCatalogWrapperManager.CatalogLease;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProvider;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProvider.CatalogConfig;
import org.apache.gravitino.paimon.service.rest.PaimonTableOperations;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.ServiceUnavailableException;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests static catalog routing and credential forwarding without schema access. */
public class TestPaimonCatalogWrapperManager {
  @TempDir Path warehouse;

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
    options.put("warehouse", warehouse.toUri().toString());
    return new CatalogConfig(options);
  }
}
