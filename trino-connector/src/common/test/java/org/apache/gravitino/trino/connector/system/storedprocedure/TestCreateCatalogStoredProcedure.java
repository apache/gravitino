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

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link CreateCatalogStoredProcedure} around the METALAKE argument. */
public class TestCreateCatalogStoredProcedure {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final Map<String, String> PROPERTIES = ImmutableMap.of("k", "v");

  @Test
  public void testMetalakeArgumentOverridesConfiguredMetalake() {
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    GravitinoMetalake other = mock(GravitinoMetalake.class);
    when(manager.getTrinoCatalogName(anyString(), eq(CATALOG))).thenReturn(CATALOG);
    when(manager.getMetalake("other")).thenReturn(other);
    // Registered by the load loop once created.
    when(manager.getCatalogConnector("other", CATALOG))
        .thenReturn(null)
        .thenReturn(mock(CatalogConnectorContext.class));

    CreateCatalogStoredProcedure procedure = new CreateCatalogStoredProcedure(manager, METALAKE);

    assertDoesNotThrow(
        () -> procedure.createCatalog(CATALOG, "memory", PROPERTIES, false, "other"));

    verify(other, times(1))
        .createCatalog(CATALOG, Catalog.Type.RELATIONAL, "memory", "Trino created", PROPERTIES);
    verify(manager, never()).getMetalake(METALAKE);
  }

  @Test
  public void testMetalakeArgumentRequiredWithoutConfiguredMetalake() {
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    CreateCatalogStoredProcedure procedure = new CreateCatalogStoredProcedure(manager, null);

    TrinoException error =
        assertThrows(
            TrinoException.class,
            () -> procedure.createCatalog(CATALOG, "memory", PROPERTIES, false, null));
    assertEquals(GravitinoErrorCode.GRAVITINO_MISSING_CONFIG.toErrorCode(), error.getErrorCode());
    verify(manager, never()).getMetalake(anyString());
  }

  @Test
  public void testTrinoNameHeldByAnotherMetalakeIsNotAnExistingCatalog() {
    // With unqualified names another metalake's catalog may hold the Trino name; that is not the
    // catalog being created, so it is created on the server rather than reported as existing.
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    GravitinoMetalake requested = mock(GravitinoMetalake.class);
    when(manager.getTrinoCatalogName(anyString(), eq(CATALOG))).thenReturn(CATALOG);
    when(manager.getCatalogConnector(CATALOG)).thenReturn(mock(CatalogConnectorContext.class));
    when(manager.getCatalogConnector(METALAKE, CATALOG)).thenReturn(null);
    when(manager.getMetalake(METALAKE)).thenReturn(requested);
    when(manager.describeRegistrationFailure(METALAKE, CATALOG))
        .thenReturn("FAILED: already registered by metalake other");

    CreateCatalogStoredProcedure procedure = new CreateCatalogStoredProcedure(manager, METALAKE);

    // Created on the server, but the name stays with the other metalake so registration fails.
    TrinoException error =
        assertThrows(
            TrinoException.class,
            () -> procedure.createCatalog(CATALOG, "memory", PROPERTIES, false, null));
    verify(requested, times(1)).createCatalog(eq(CATALOG), any(), eq("memory"), any(), any());
    assertEquals(GravitinoErrorCode.GRAVITINO_OPERATION_FAILED.toErrorCode(), error.getErrorCode());
    assertTrue(error.getMessage().contains("already registered"), error.getMessage());
  }

  @Test
  public void testCreateCatalogDeniedIsPermissionDenied() {
    String message = "User joel is not authorized to perform operation createCatalog";
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    when(manager.getMetalake(METALAKE)).thenReturn(metalake);
    when(metalake.createCatalog(anyString(), any(), anyString(), anyString(), any()))
        .thenThrow(new ForbiddenException(message));

    CreateCatalogStoredProcedure procedure = new CreateCatalogStoredProcedure(manager, METALAKE);

    TrinoException error =
        assertThrows(
            TrinoException.class,
            () -> procedure.createCatalog(CATALOG, "memory", PROPERTIES, false, null));
    assertEquals(PERMISSION_DENIED.toErrorCode(), error.getErrorCode());
    assertEquals(message, error.getMessage());
  }

  @Test
  public void testCreateCatalogIllegalArgumentIsIllegalArgument() {
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    when(manager.getMetalake(METALAKE)).thenReturn(metalake);
    when(metalake.createCatalog(anyString(), any(), anyString(), anyString(), any()))
        .thenThrow(new IllegalArgumentException("Unsupported provider: bad"));

    CreateCatalogStoredProcedure procedure = new CreateCatalogStoredProcedure(manager, METALAKE);

    TrinoException error =
        assertThrows(
            TrinoException.class,
            () -> procedure.createCatalog(CATALOG, "bad", PROPERTIES, false, null));
    assertEquals(GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), error.getErrorCode());
    assertTrue(error.getMessage().contains("Unsupported provider: bad"), error.getMessage());
  }

  @Test
  public void testCreateCatalogUnclassifiedFailureKeepsContextAndRedactsSecrets() throws Exception {
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    when(manager.getMetalake(METALAKE)).thenReturn(mock(GravitinoMetalake.class));
    doThrow(
            new RuntimeException(
                "CREATE CATALOG c USING jdbc WITH (\"trino.jdbc.password\"='s3cret') failed"))
        .when(manager)
        .loadMetalakeSync();

    CreateCatalogStoredProcedure procedure = new CreateCatalogStoredProcedure(manager, METALAKE);

    TrinoException error =
        assertThrows(
            TrinoException.class,
            () -> procedure.createCatalog(CATALOG, "memory", PROPERTIES, false, null));
    assertEquals(GENERIC_INTERNAL_ERROR.toErrorCode(), error.getErrorCode());
    assertTrue(
        error.getMessage().startsWith("Create catalog test_metalake.test_catalog failed: "),
        error.getMessage());
    assertFalse(error.getMessage().contains("s3cret"), error.getMessage());
  }

  @Test
  public void testCreateCatalogReloadTimeoutKeepsContext() throws Exception {
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    when(manager.getMetalake(METALAKE)).thenReturn(mock(GravitinoMetalake.class));
    doThrow(new TimeoutException()).when(manager).loadMetalakeSync();

    CreateCatalogStoredProcedure procedure = new CreateCatalogStoredProcedure(manager, METALAKE);

    TrinoException error =
        assertThrows(
            TrinoException.class,
            () -> procedure.createCatalog(CATALOG, "memory", PROPERTIES, false, null));
    assertEquals(GENERIC_INTERNAL_ERROR.toErrorCode(), error.getErrorCode());
    assertEquals(
        "Create catalog test_metalake.test_catalog failed: java.util.concurrent.TimeoutException",
        error.getMessage());
  }
}
