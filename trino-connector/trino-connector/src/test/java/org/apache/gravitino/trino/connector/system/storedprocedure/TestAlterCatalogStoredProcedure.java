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
package org.apache.gravitino.trino.connector.system.storedprocedure;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import java.util.List;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link AlterCatalogStoredProcedure} around the METALAKE argument. */
public class TestAlterCatalogStoredProcedure {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";

  @Test
  public void testMetalakeArgumentOverridesConfiguredMetalake() {
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    CatalogConnectorContext context = mock(CatalogConnectorContext.class);
    GravitinoMetalake other = mock(GravitinoMetalake.class);
    when(manager.getTrinoCatalogName(anyString(), eq(CATALOG))).thenReturn(CATALOG);
    when(manager.getCatalogConnector("other", CATALOG)).thenReturn(context);
    when(context.getMetalake()).thenReturn(other);
    when(context.getCatalog())
        .thenReturn(new GravitinoCatalog("other", "memory", CATALOG, ImmutableMap.of(), 1L))
        .thenReturn(
            new GravitinoCatalog("other", "memory", CATALOG, ImmutableMap.of("k", "v"), 2L));

    AlterCatalogStoredProcedure procedure = new AlterCatalogStoredProcedure(manager, METALAKE);

    assertDoesNotThrow(
        () -> procedure.alterCatalog(CATALOG, ImmutableMap.of("k", "v"), List.of(), "other"));

    verify(other, times(1)).alterCatalog(eq(CATALOG), any());
    verify(manager, never()).getCatalogConnector(METALAKE, CATALOG);
  }

  @Test
  public void testMetalakeArgumentRequiredWithoutConfiguredMetalake() {
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    AlterCatalogStoredProcedure procedure = new AlterCatalogStoredProcedure(manager, null);

    TrinoException error =
        assertThrows(
            TrinoException.class,
            () -> procedure.alterCatalog(CATALOG, ImmutableMap.of("k", "v"), List.of(), null));
    assertEquals(GravitinoErrorCode.GRAVITINO_MISSING_CONFIG.toErrorCode(), error.getErrorCode());
    verify(manager, never()).getCatalogConnector(anyString(), anyString());
  }

  @Test
  public void testTrinoNameHeldByAnotherMetalakeIsNotAltered() {
    // With unqualified names another metalake's catalog may hold the Trino name. It is not the
    // catalog asked for, so nothing is altered and the caller learns why it is not registered.
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    CatalogConnectorContext otherContext = mock(CatalogConnectorContext.class);
    GravitinoMetalake otherMetalake = mock(GravitinoMetalake.class);
    when(manager.getTrinoCatalogName(anyString(), eq(CATALOG))).thenReturn(CATALOG);
    when(manager.getCatalogConnector(CATALOG)).thenReturn(otherContext);
    when(manager.getCatalogConnector(METALAKE, CATALOG)).thenReturn(null);
    when(otherContext.getMetalake()).thenReturn(otherMetalake);
    when(manager.describeRegistrationFailure(METALAKE, CATALOG))
        .thenReturn("FAILED: already registered by metalake other");

    AlterCatalogStoredProcedure procedure = new AlterCatalogStoredProcedure(manager, METALAKE);

    TrinoException error =
        assertThrows(
            TrinoException.class,
            () -> procedure.alterCatalog(CATALOG, ImmutableMap.of("k", "v"), List.of(), null));
    assertTrue(error.getMessage().contains("already registered"), error.getMessage());
    verify(otherMetalake, never()).alterCatalog(anyString(), any());
  }
}
