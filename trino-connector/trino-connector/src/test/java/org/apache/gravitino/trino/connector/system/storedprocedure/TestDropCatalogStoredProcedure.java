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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.trino.spi.TrinoException;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DropCatalogStoredProcedure} covering the server-side fallback path added
 * for issue #11401.
 */
public class TestDropCatalogStoredProcedure {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final String TRINO_CATALOG = "test_metalake.test_catalog";

  @Test
  public void testDropCatalogFallsBackToServerWhenLocalCacheMissesAndServerHasIt() {
    // The local connector cache does not have the catalog, but the Gravitino server does.
    // The procedure must call the server-side drop and succeed without throwing "not exists".
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);

    when(manager.getTrinoCatalogName(METALAKE, CATALOG)).thenReturn(TRINO_CATALOG);
    when(manager.getCatalogConnector(TRINO_CATALOG)).thenReturn(null);
    when(manager.getMetalake(METALAKE)).thenReturn(metalake);
    when(metalake.dropCatalog(CATALOG, true)).thenReturn(true);

    DropCatalogStoredProcedure procedure = new DropCatalogStoredProcedure(manager, METALAKE);

    assertDoesNotThrow(() -> procedure.dropCatalog(CATALOG, false));

    verify(metalake, times(1)).dropCatalog(CATALOG, true);
  }

  @Test
  public void testDropCatalogThrowsWhenAbsentEverywhereAndIgnoreNotExistFalse() {
    // Catalog is absent from both the local cache and the server, ignoreNotExist=false.
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);

    when(manager.getTrinoCatalogName(METALAKE, CATALOG)).thenReturn(TRINO_CATALOG);
    when(manager.getCatalogConnector(TRINO_CATALOG)).thenReturn(null);
    when(manager.getMetalake(METALAKE)).thenReturn(metalake);
    when(metalake.dropCatalog(CATALOG, true)).thenReturn(false);

    DropCatalogStoredProcedure procedure = new DropCatalogStoredProcedure(manager, METALAKE);

    // The internally-thrown "not exists" TrinoException is caught by the outer catch(Exception)
    // and re-wrapped as GRAVITINO_UNSUPPORTED_OPERATION, with the original exception preserved
    // as the cause. This matches the historical behavior observed in the issue's stack trace.
    // The contract we verify is: the user-visible message still tells them the catalog does not
    // exist, and the root cause carries the GRAVITINO_CATALOG_NOT_EXISTS error code.
    TrinoException error =
        assertThrows(TrinoException.class, () -> procedure.dropCatalog(CATALOG, false));
    assertTrue(
        error.getMessage().contains("not exists"),
        () -> "Expected message to contain 'not exists' but was: " + error.getMessage());
    Throwable cause = error.getCause();
    assertTrue(
        cause instanceof TrinoException, () -> "Expected TrinoException cause, was: " + cause);
    assertEquals(
        GravitinoErrorCode.GRAVITINO_CATALOG_NOT_EXISTS.toErrorCode(),
        ((TrinoException) cause).getErrorCode());
  }

  @Test
  public void testDropCatalogReturnsSilentlyWhenAbsentEverywhereAndIgnoreNotExistTrue() {
    // Catalog is absent from both the local cache and the server, ignoreNotExist=true.
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);

    when(manager.getTrinoCatalogName(METALAKE, CATALOG)).thenReturn(TRINO_CATALOG);
    when(manager.getCatalogConnector(TRINO_CATALOG)).thenReturn(null);
    when(manager.getMetalake(METALAKE)).thenReturn(metalake);
    when(metalake.dropCatalog(CATALOG, true)).thenReturn(false);

    DropCatalogStoredProcedure procedure = new DropCatalogStoredProcedure(manager, METALAKE);

    assertDoesNotThrow(() -> procedure.dropCatalog(CATALOG, true));

    // We DID consult the server, but no exception was thrown.
    verify(metalake, times(1)).dropCatalog(CATALOG, true);
    // Local-cache reload should NOT be triggered when the local connector was already absent.
    verify(manager, never()).catalogConnectorExist(anyString());
  }
}
