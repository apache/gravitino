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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.apache.gravitino.trino.connector.security.GravitinoAuthProvider;
import org.junit.jupiter.api.Test;

/** Tests for forwardUser startup validation in {@link GravitinoConnector}. */
class TestGravitinoConnectorForwardUser {

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
  void testForwardUserWithOAuth2AuthTypeThrowsAtConstruction() {
    CatalogConnectorContext ctx =
        mockContextWithConfig(
            ImmutableMap.of(
                GravitinoAuthProvider.FORWARD_SESSION_USER_KEY, "true",
                GravitinoAuthProvider.AUTH_TYPE_KEY, "oauth2"));

    TrinoException ex = assertThrows(TrinoException.class, () -> new GravitinoConnector(ctx));
    assertEquals(GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), ex.getErrorCode());
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
