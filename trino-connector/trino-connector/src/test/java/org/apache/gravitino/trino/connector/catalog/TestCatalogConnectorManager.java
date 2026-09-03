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
package org.apache.gravitino.trino.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorContext;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.secret.SupportsSecrets;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestCatalogConnectorManager {

  @Test
  public void testSingleMetalakeCatalogNaming() throws Exception {
    CatalogConnectorManager manager =
        createManager(
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test",
                "gravitino.use-single-metalake",
                "true"));

    assertEquals("memory", manager.getTrinoCatalogName("test", "memory"));
  }

  @Test
  public void testMultiMetalakeCatalogNaming() throws Exception {
    CatalogConnectorManager manager =
        createManager(
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test",
                "gravitino.use-single-metalake",
                "false"));

    assertEquals("\"test.memory\"", manager.getTrinoCatalogName("test", "memory"));
  }

  @Test
  public void testSingleMetalakeRejectsDifferentMetalakeConnector() throws Exception {
    CatalogConnectorFactory catalogFactory = createCatalogConnectorFactory();
    CatalogConnectorManager manager =
        createManager(
            catalogFactory,
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test",
                "gravitino.use-single-metalake",
                "true"));

    GravitinoConfig connectorConfig = createConnectorConfig(catalogConfigJson("test", "memory"));
    assertDoesNotThrow(
        () -> manager.createCatalogConnectorContext("test0", connectorConfig, mockContext()));

    GravitinoConfig otherConnectorConfig =
        createConnectorConfig(catalogConfigJson("test2", "memory"));
    TrinoException error =
        assertThrows(
            TrinoException.class,
            () ->
                manager.createCatalogConnectorContext(
                    "test1", otherConnectorConfig, mockContext()));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION.toErrorCode(), error.getErrorCode());
    assertTrue(error.getMessage().contains("Multiple metalakes are not supported"));
  }

  @Test
  public void testMultiMetalakeAllowsDifferentMetalakeConnector() throws Exception {
    CatalogConnectorFactory catalogFactory = createCatalogConnectorFactory();
    CatalogConnectorManager manager =
        createManager(
            catalogFactory,
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test",
                "gravitino.use-single-metalake",
                "false"));

    GravitinoConfig connectorConfig = createConnectorConfig(catalogConfigJson("test", "memory"));
    assertDoesNotThrow(
        () -> manager.createCatalogConnectorContext("test0", connectorConfig, mockContext()));

    GravitinoConfig otherConnectorConfig =
        createConnectorConfig(catalogConfigJson("test2", "memory"));
    assertDoesNotThrow(
        () -> manager.createCatalogConnectorContext("test1", otherConnectorConfig, mockContext()));
  }

  @Test
  public void testSkipCatalogPatterns() throws Exception {
    CatalogConnectorManager manager =
        createManager(
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test1",
                "gravitino.trino.skip-catalog-patterns",
                "a.*, b1"));

    assertTrue(manager.skipCatalog("a1"));
    assertTrue(manager.skipCatalog("b1"));
    assertFalse(manager.skipCatalog("b2"));
  }

  @Test
  public void testSuccessfulRegistrationIsRecorded() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.REGISTERED, state.getStatus());
    assertEquals("test", state.getMetalake());
    assertEquals("memory", state.getCatalogName());
    assertEquals("memory", state.getTrinoCatalogName());
    assertEquals("memory", state.getProvider());
    assertNull(state.getLastError());
    assertEquals(0, state.getFailureCount());
    assertTrue(state.getLastSuccessTimeMs() > 0);

    assertTrue(manager.isTrinoReachable());
    assertNull(manager.getLoadOutcome().getLastError());
    assertEquals(0, manager.getLoadOutcome().getConsecutiveFailures());
    assertTrue(manager.getMetalakeErrors().isEmpty());
  }

  @Test
  public void testRegistrationFailureIsRecorded() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));
    doThrow(
            new TrinoException(
                GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR,
                "Access Denied: Cannot create catalog memory"))
        .when(fixture.catalogRegister)
        .registerCatalog(any(), any());

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.FAILED, state.getStatus());
    assertTrue(state.getLastError().contains("Access Denied"));
    assertEquals(1, state.getFailureCount());
    assertEquals(0, state.getLastSuccessTimeMs());

    // A second failing round must accumulate rather than reset the failure count.
    manager.loadMetalakeSync();
    state = singleState(manager);
    assertEquals(2, state.getFailureCount());
    assertEquals(0, state.getLastSuccessTimeMs());
  }

  @Test
  public void testRegistrationFailureIsReportedByStoredProcedureMessage() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));
    doThrow(
            new TrinoException(
                GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR,
                "Access Denied: Cannot create catalog memory"))
        .when(fixture.catalogRegister)
        .registerCatalog(any(), any());

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    String description = manager.describeRegistrationFailure("test", "memory");
    assertTrue(description.contains("FAILED"));
    assertTrue(description.contains("Access Denied"));
  }

  @Test
  public void testNonRelationalCatalogIsRecorded() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("files", "hadoop", Catalog.Type.FILESET));

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.UNSUPPORTED, state.getStatus());
    assertTrue(state.getLastError().contains("FILESET"));
  }

  @Test
  public void testUnsupportedProviderIsRecorded() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("other", "unknown", Catalog.Type.RELATIONAL));

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.UNSUPPORTED, state.getStatus());
    assertTrue(state.getLastError().contains("unknown"));
    assertEquals("unknown", state.getProvider());
  }

  @Test
  public void testFailureBeforeProviderIsKnownKeepsPreviousProvider() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();
    assertEquals("memory", singleState(manager).getProvider());

    // The next attempt fails before the provider can even be read off the catalog.
    Mockito.doThrow(new RuntimeException("Connection reset"))
        .when(fixture.metalake)
        .loadCatalog("memory");
    manager.loadMetalakeSync();

    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.FAILED, state.getStatus());
    // The provider a previous successful attempt discovered must not be blanked out.
    assertEquals("memory", state.getProvider());
  }

  @Test
  public void testSkippedCatalogIsRecorded() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));

    CatalogConnectorManager manager =
        fixture.createManager(ImmutableMap.of("gravitino.trino.skip-catalog-patterns", "mem.*"));
    manager.loadMetalakeSync();

    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.SKIPPED, state.getStatus());
    assertTrue(state.getLastError().contains("skip-catalog-patterns"));
  }

  @Test
  public void testStaleStateIsPruned() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog first = mockCatalog("a", "memory", Catalog.Type.RELATIONAL);
    Catalog second = mockCatalog("b", "memory", Catalog.Type.RELATIONAL);
    fixture.withCatalogs(first, second);

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();
    assertEquals(2, manager.getCatalogRegistrationStates().size());

    // The second catalog is dropped in the Gravitino server, its state must not linger.
    fixture.withCatalogs(first);
    manager.loadMetalakeSync();

    CatalogRegistrationState state = singleState(manager);
    assertEquals("a", state.getCatalogName());
  }

  @Test
  public void testListCatalogsFailureIsRecordedPerMetalake() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();
    assertEquals(CatalogRegistrationState.Status.REGISTERED, singleState(manager).getStatus());

    Mockito.doThrow(new RuntimeException("Connection refused"))
        .when(fixture.metalake)
        .listCatalogs();
    manager.loadMetalakeSync();

    Map<String, String> metalakeErrors = manager.getMetalakeErrors();
    assertEquals(1, metalakeErrors.size());
    assertTrue(metalakeErrors.get("test").contains("Connection refused"));
    // A transient listing failure must not turn a healthy catalog into a failed one.
    assertEquals(CatalogRegistrationState.Status.REGISTERED, singleState(manager).getStatus());
  }

  @Test
  public void testUnreachableServerIsRecordedInLoadStatus() throws Exception {
    LoadFixture fixture = new LoadFixture();
    when(fixture.client.loadMetalake(any())).thenThrow(new RuntimeException("Connection refused"));

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    CatalogConnectorManager.LoadOutcome loadOutcome = manager.getLoadOutcome();
    assertNotNull(loadOutcome.getLastError());
    assertTrue(loadOutcome.getLastError().contains("Connection refused"));
    assertEquals(1, loadOutcome.getConsecutiveFailures());
    assertEquals(0, loadOutcome.getLastSuccessTimeMs());
    assertTrue(manager.getLastLoadAttemptTimeMs() > 0);
    assertTrue(manager.getCatalogRegistrationStates().isEmpty());
  }

  @Test
  public void testUnreachableTrinoIsRecordedInLoadStatus() throws Exception {
    LoadFixture fixture = new LoadFixture();
    when(fixture.catalogRegister.isTrinoReachable()).thenReturn(false);

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    assertFalse(manager.isTrinoReachable());
    String lastError = manager.getLoadOutcome().getLastError();
    assertNotNull(lastError);
    assertTrue(lastError.contains("The Trino server is not reachable"));
    assertTrue(manager.getCatalogRegistrationStates().isEmpty());
  }

  @Test
  public void testCatalogRecoversFromFailure() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));
    doThrow(new TrinoException(GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, "Access Denied"))
        .when(fixture.catalogRegister)
        .registerCatalog(any(), any());

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();
    assertEquals(CatalogRegistrationState.Status.FAILED, singleState(manager).getStatus());

    // The underlying problem is fixed; the row must not stay stuck on FAILED.
    Mockito.reset(fixture.catalogRegister);
    when(fixture.catalogRegister.isTrinoReachable()).thenReturn(true);
    manager.loadMetalakeSync();

    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.REGISTERED, state.getStatus());
    assertNull(state.getLastError());
    assertEquals(0, state.getFailureCount());
    assertTrue(state.getLastSuccessTimeMs() > 0);
  }

  @Test
  public void testFailureAfterSuccessKeepsLastSuccessTime() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();
    long successTime = singleState(manager).getLastSuccessTimeMs();
    assertTrue(successTime > 0);

    doThrow(new TrinoException(GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, "Access Denied"))
        .when(fixture.catalogRegister)
        .registerCatalog(any(), any());
    manager.loadMetalakeSync();

    // A catalog that regressed must keep telling the user when it last worked.
    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.FAILED, state.getStatus());
    assertEquals(1, state.getFailureCount());
    assertEquals(successTime, state.getLastSuccessTimeMs());
  }

  @Test
  public void testUnsupportedCatalogKeepsLastSuccessTime() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    fixture.withCatalogs(catalog);

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();
    long successTime = singleState(manager).getLastSuccessTimeMs();

    // The provider changes to one the connector does not support.
    when(catalog.provider()).thenReturn("unknown");
    manager.loadMetalakeSync();

    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.UNSUPPORTED, state.getStatus());
    assertEquals(successTime, state.getLastSuccessTimeMs());
  }

  @Test
  public void testSkippedCatalogSurvivesPruning() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));

    CatalogConnectorManager manager =
        fixture.createManager(ImmutableMap.of("gravitino.trino.skip-catalog-patterns", "mem.*"));
    manager.loadMetalakeSync();
    manager.loadMetalakeSync();

    // The row must not flicker away on the second iteration.
    assertEquals(CatalogRegistrationState.Status.SKIPPED, singleState(manager).getStatus());
  }

  @Test
  public void testMetalakeErrorClearsOnRecovery() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());

    Mockito.doThrow(new RuntimeException("Connection refused"))
        .when(fixture.metalake)
        .listCatalogs();
    manager.loadMetalakeSync();
    assertEquals(1, manager.getMetalakeErrors().size());
    // A metalake that cannot be listed is not a healthy loop.
    assertNotNull(manager.getLoadOutcome().getLastError());
    assertEquals(0, manager.getLoadOutcome().getLastSuccessTimeMs());

    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));
    manager.loadMetalakeSync();

    assertTrue(manager.getMetalakeErrors().isEmpty());
    CatalogConnectorManager.LoadOutcome loadOutcome = manager.getLoadOutcome();
    assertNull(loadOutcome.getLastError());
    assertEquals(0, loadOutcome.getConsecutiveFailures());
    assertTrue(loadOutcome.getLastSuccessTimeMs() > 0);
  }

  @Test
  public void testUnloadFailureKeepsCatalogVisibleInState() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    fixture.withCatalogs(catalog);
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    // Trino has the connector, so the manager will try to unload it when it disappears.
    CatalogConnectorContext context =
        manager.createCatalogConnectorContext(
            "memory", createConnectorConfig(catalogConfigJson("test", "memory")), mockContext());
    when(context.getMetalake()).thenReturn(fixture.metalake);
    when(context.getCatalog())
        .thenReturn(new GravitinoCatalog("test", "memory", "memory", ImmutableMap.of(), 0L));
    doThrow(new TrinoException(GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, "Access Denied"))
        .when(fixture.catalogRegister)
        .unregisterCatalog(any());
    Mockito.doReturn(new String[0]).when(fixture.metalake).listCatalogs();
    manager.loadMetalakeSync();

    // The catalog is gone from Gravitino but still registered in Trino: the state must say so
    // rather than disappear.
    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.FAILED, state.getStatus());
    assertTrue(state.getLastError().contains("could not be unregistered"));
  }

  @Test
  public void testUnloadFailureKeepsStateWhenMetalakeVanishes() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    fixture.withCatalogs(catalog);
    CatalogConnectorManager manager =
        fixture.createManager(ImmutableMap.of("gravitino.use-single-metalake", "false"));
    manager.loadMetalakeSync();

    // Trino has the connector, so the manager will try to unload it when the metalake disappears.
    CatalogConnectorContext context =
        manager.createCatalogConnectorContext(
            "memory", createConnectorConfig(catalogConfigJson("test", "memory")), mockContext());
    when(context.getMetalake()).thenReturn(fixture.metalake);
    when(context.getCatalog())
        .thenReturn(new GravitinoCatalog("test", "memory", "memory", ImmutableMap.of(), 0L));
    doThrow(new TrinoException(GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, "Access Denied"))
        .when(fixture.catalogRegister)
        .unregisterCatalog(any());
    Mockito.doReturn(new GravitinoMetalake[0]).when(fixture.client).listMetalakes();

    manager.loadMetalakeSync();

    // The whole metalake is gone but the catalog is still registered in Trino: pruning the
    // metalake's state must not take the failure it just recorded with it.
    assertTrue(manager.catalogConnectorExist("memory"));
    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.FAILED, state.getStatus());
    assertTrue(state.getLastError().contains("The metalake was removed"));
  }

  @Test
  public void testRefreshFailureKeepsRegisteredCatalogRegistered() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    fixture.withCatalogs(catalog);
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    // Trino loaded the catalog Gravitino registered, so it is live from now on.
    CatalogConnectorContext context =
        manager.createCatalogConnectorContext(
            "memory", createConnectorConfig(catalogConfigJson("test", "memory")), mockContext());
    when(context.getCatalog())
        .thenReturn(new GravitinoCatalog("test", "memory", "memory", ImmutableMap.of(), 0L));

    Mockito.doThrow(new RESTException("simulated: the server rejected the load"))
        .when(fixture.metalake)
        .loadCatalog("memory");
    manager.loadMetalakeSync();

    // The catalog is still registered and queryable, so a failed refresh must not flip it to
    // FAILED and hide that it is usable.
    assertTrue(manager.catalogConnectorExist("memory"));
    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.REGISTERED, state.getStatus());
    assertNull(state.getLastError());
  }

  @Test
  public void testUnknownCatalogTypeIsRecordedAsUnsupported() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());

    // The client library cannot map this catalog's type, e.g. DTOConverters.toCatalog throwing
    // for a type it does not know. That is a "not supported", not a registration failure.
    Mockito.doThrow(new UnsupportedOperationException("Unsupported catalog type: UNKNOWN"))
        .when(fixture.metalake)
        .loadCatalog("memory");
    manager.loadMetalakeSync();

    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.UNSUPPORTED, state.getStatus());
    assertTrue(state.getLastError().contains("Unsupported catalog type"));
    assertFalse(manager.catalogConnectorExist("memory"));
  }

  @Test
  public void testUnchangedCatalogIsNotReRegistered() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    Audit audit = mock(Audit.class);
    Instant modifiedTime = Instant.ofEpochMilli(1000L);
    when(audit.createTime()).thenReturn(modifiedTime);
    when(audit.lastModifiedTime()).thenReturn(modifiedTime);
    when(catalog.auditInfo()).thenReturn(audit);
    fixture.withCatalogs(catalog);
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    // Trino holds a connector built from the very same version the server reports.
    CatalogConnectorContext context =
        manager.createCatalogConnectorContext(
            "memory", createConnectorConfig(catalogConfigJson("test", "memory")), mockContext());
    when(context.getCatalog())
        .thenReturn(
            new GravitinoCatalog(
                "test", "memory", "memory", ImmutableMap.of(), modifiedTime.toEpochMilli()));
    Mockito.clearInvocations(fixture.catalogRegister);

    manager.loadMetalakeSync();

    // Nothing changed, so the connector must be left alone rather than dropped and rebuilt.
    verify(fixture.catalogRegister, never()).unregisterCatalog(any());
    verify(fixture.catalogRegister, never()).registerCatalog(any(), any());
    assertTrue(manager.catalogConnectorExist("memory"));
    assertEquals(CatalogRegistrationState.Status.REGISTERED, singleState(manager).getStatus());
  }

  @Test
  public void testUpdatedCatalogIsReRegistered() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    fixture.withCatalogs(catalog);
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    // Trino holds a connector built from an older version than the one the server reports.
    CatalogConnectorContext context =
        manager.createCatalogConnectorContext(
            "memory", createConnectorConfig(catalogConfigJson("test", "memory")), mockContext());
    when(context.getCatalog())
        .thenReturn(new GravitinoCatalog("test", "memory", "memory", ImmutableMap.of(), 0L));
    Mockito.clearInvocations(fixture.catalogRegister);

    manager.loadMetalakeSync();

    // The old connector is dropped and the new definition registered in its place.
    verify(fixture.catalogRegister, times(1)).unregisterCatalog("memory");
    verify(fixture.catalogRegister, times(1)).registerCatalog(Mockito.eq("memory"), any());
    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.REGISTERED, state.getStatus());
    assertNull(state.getLastError());
  }

  @Test
  public void testDeletedCatalogWithConnectorIsRemoved() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    fixture.withCatalogs(catalog);
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    CatalogConnectorContext context =
        manager.createCatalogConnectorContext(
            "memory", createConnectorConfig(catalogConfigJson("test", "memory")), mockContext());
    when(context.getMetalake()).thenReturn(fixture.metalake);
    when(context.getCatalog())
        .thenReturn(new GravitinoCatalog("test", "memory", "memory", ImmutableMap.of(), 0L));

    // The catalog is deleted in Gravitino and unregisters cleanly from Trino.
    Mockito.doReturn(new String[0]).when(fixture.metalake).listCatalogs();
    manager.loadMetalakeSync();

    verify(fixture.catalogRegister, times(1)).unregisterCatalog("memory");
    assertFalse(manager.catalogConnectorExist("memory"));
    assertTrue(manager.getCatalogRegistrationStates().isEmpty());
    assertNull(manager.getLoadOutcome().getLastError());
  }

  @Test
  public void testVanishedMetalakeCleansUpItsConnectors() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    fixture.withCatalogs(catalog);
    CatalogConnectorManager manager =
        fixture.createManager(ImmutableMap.of("gravitino.use-single-metalake", "false"));
    manager.loadMetalakeSync();

    CatalogConnectorContext context =
        manager.createCatalogConnectorContext(
            "memory", createConnectorConfig(catalogConfigJson("test", "memory")), mockContext());
    when(context.getMetalake()).thenReturn(fixture.metalake);
    when(context.getCatalog())
        .thenReturn(new GravitinoCatalog("test", "memory", "memory", ImmutableMap.of(), 0L));
    assertTrue(manager.getUsedMetalakes().contains("test"));

    // The metalake is deleted in Gravitino and its catalog unregisters cleanly from Trino.
    Mockito.doReturn(new GravitinoMetalake[0]).when(fixture.client).listMetalakes();
    manager.loadMetalakeSync();

    verify(fixture.catalogRegister, times(1)).unregisterCatalog("memory");
    assertFalse(manager.catalogConnectorExist("memory"));
    assertTrue(manager.getCatalogRegistrationStates().isEmpty());
    assertTrue(manager.getMetalakeErrors().isEmpty());
    // getUsedMetalakes() reads the cached metalake handles, so an empty set is what proves the
    // deleted metalake was dropped from the cache too.
    assertTrue(manager.getUsedMetalakes().isEmpty());
  }

  @Test
  public void testCatalogStatesAreFilteredPerMetalake() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));

    // A second metalake loaded by the same manager, which every entry catalog shares.
    GravitinoMetalake devMetalake = mock(GravitinoMetalake.class);
    when(devMetalake.name()).thenReturn("dev");
    Catalog devCatalog = mockCatalog("sandbox", "memory", Catalog.Type.RELATIONAL);
    Mockito.doReturn(new String[] {"sandbox"}).when(devMetalake).listCatalogs();
    Mockito.doReturn(devCatalog).when(devMetalake).loadCatalog("sandbox");
    Mockito.doReturn(new GravitinoMetalake[] {fixture.metalake, devMetalake})
        .when(fixture.client)
        .listMetalakes();

    CatalogConnectorManager manager =
        fixture.createManager(ImmutableMap.of("gravitino.use-single-metalake", "false"));
    manager.loadMetalakeSync();

    assertEquals(2, manager.getCatalogRegistrationStates().size());

    // Each entry catalog's system tables read through this filter, so it must not leak the other
    // metalake's rows.
    List<CatalogRegistrationState> testStates = manager.getCatalogRegistrationStates("test");
    assertEquals(1, testStates.size());
    assertEquals("memory", testStates.get(0).getCatalogName());
    List<CatalogRegistrationState> devStates = manager.getCatalogRegistrationStates("dev");
    assertEquals(1, devStates.size());
    assertEquals("sandbox", devStates.get(0).getCatalogName());
  }

  @Test
  public void testSkipPatternMatchesQualifiedCatalogName() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));

    // In multi metalake mode the catalog is registered under its qualified Trino name, and that
    // is the name the skip patterns are matched against.
    CatalogConnectorManager manager =
        fixture.createManager(
            ImmutableMap.of(
                "gravitino.use-single-metalake",
                "false",
                "gravitino.trino.skip-catalog-patterns",
                ".*test\\.memory.*"),
            // The qualified name Trino sees in multi metalake mode, quotes included.
            (metalakeName, catalog) -> "\"" + metalakeName + "." + catalog + "\"");
    assertTrue(manager.skipCatalog(manager.getTrinoCatalogName("test", "memory")));

    manager.loadMetalakeSync();

    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.SKIPPED, state.getStatus());
    assertTrue(state.getLastError().contains("skip-catalog-patterns"));
    verify(fixture.catalogRegister, never()).registerCatalog(any(), any());
  }

  @Test
  public void testRecordedErrorRedactsSecrets() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));
    // What a driver that echoes the failing CREATE CATALOG back would report.
    doThrow(
            new TrinoException(
                GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR,
                "Query failed: CREATE CATALOG memory USING gravitino WITH ("
                    + "\"trino.bypass.password\"='hunter2')"))
        .when(fixture.catalogRegister)
        .registerCatalog(any(), any());

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    // The exception keeps its own message; the text the status tables report does not.
    CatalogRegistrationState state = singleState(manager);
    assertFalse(state.getLastError().contains("hunter2"));
    assertTrue(state.getLastError().contains("\"trino.bypass.password\"='***'"));
  }

  @Test
  public void testRepeatedUnreachableTrinoKeepsItsOwnMessage() throws Exception {
    LoadFixture fixture = new LoadFixture();
    when(fixture.catalogRegister.isTrinoReachable()).thenReturn(false);
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());

    // The second attempt reports the same message as the first, which is the branch that logs a
    // repeated failure. It carries no cause, and rendering one anyway would fail there.
    manager.loadMetalakeSync();
    manager.loadMetalakeSync();

    CatalogConnectorManager.LoadOutcome outcome = manager.getLoadOutcome();
    assertTrue(outcome.getLastError().contains("The Trino server is not reachable"));
    assertEquals(2, outcome.getConsecutiveFailures());
  }

  @Test
  public void testMetalakeFailureAggregationKeepsItsOwnMessage() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Mockito.doThrow(new RESTException("simulated: listing failed"))
        .when(fixture.metalake)
        .listCatalogs();
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());

    manager.loadMetalakeSync();

    // The aggregated failure has no single cause of its own, and load_status must report it
    // rather than whatever went wrong while trying to log it.
    CatalogConnectorManager.LoadOutcome outcome = manager.getLoadOutcome();
    assertTrue(outcome.getLastError().contains("1 of 1 metalakes failed"));
    assertTrue(outcome.getLastError().contains("simulated: listing failed"));
  }

  @Test
  public void testLiveCatalogBecomingUnsupportedIsUnregistered() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    fixture.withCatalogs(catalog);
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    CatalogConnectorContext context =
        manager.createCatalogConnectorContext(
            "memory", createConnectorConfig(catalogConfigJson("test", "memory")), mockContext());
    when(context.getMetalake()).thenReturn(fixture.metalake);
    when(context.getCatalog())
        .thenReturn(new GravitinoCatalog("test", "memory", "memory", ImmutableMap.of(), 0L));

    // The catalog was dropped and recreated under the same name with a provider this connector
    // cannot serve.
    when(catalog.provider()).thenReturn("unsupported-provider");
    manager.loadMetalakeSync();

    // Leaving it registered would keep Trino serving a catalog the status table calls
    // UNSUPPORTED.
    verify(fixture.catalogRegister, times(1)).unregisterCatalog("memory");
    assertFalse(manager.catalogConnectorExist("memory"));
    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.UNSUPPORTED, state.getStatus());
    assertTrue(state.getLastError().contains("unsupported-provider"));
  }

  @Test
  public void testLiveCatalogMatchingSkipPatternIsUnregistered() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    CatalogConnectorContext context =
        manager.createCatalogConnectorContext(
            "memory", createConnectorConfig(catalogConfigJson("test", "memory")), mockContext());
    when(context.getMetalake()).thenReturn(fixture.metalake);
    when(context.getCatalog())
        .thenReturn(new GravitinoCatalog("test", "memory", "memory", ImmutableMap.of(), 0L));

    // The operator added a skip pattern that now matches a catalog Trino already serves.
    manager.updateConfig(
        fixture.config(ImmutableMap.of("gravitino.trino.skip-catalog-patterns", "mem.*")));
    manager.loadMetalakeSync();

    verify(fixture.catalogRegister, times(1)).unregisterCatalog("memory");
    assertFalse(manager.catalogConnectorExist("memory"));
    assertEquals(CatalogRegistrationState.Status.SKIPPED, singleState(manager).getStatus());
  }

  @Test
  public void testSkippedCatalogUnregisterFailureIsNotReportedAsDeletion() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    CatalogConnectorContext context =
        manager.createCatalogConnectorContext(
            "memory", createConnectorConfig(catalogConfigJson("test", "memory")), mockContext());
    when(context.getMetalake()).thenReturn(fixture.metalake);
    when(context.getCatalog())
        .thenReturn(new GravitinoCatalog("test", "memory", "memory", ImmutableMap.of(), 0L));
    doThrow(new TrinoException(GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, "Access Denied"))
        .when(fixture.catalogRegister)
        .unregisterCatalog(any());

    manager.updateConfig(
        fixture.config(ImmutableMap.of("gravitino.trino.skip-catalog-patterns", "mem.*")));
    manager.loadMetalakeSync();

    // The catalog still exists in Gravitino, so its owner must not be told it was deleted.
    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.FAILED, state.getStatus());
    assertTrue(state.getLastError().contains("skip-catalog-patterns"));
    assertFalse(state.getLastError().contains("deleted in Gravitino"));
  }

  @Test
  public void testUnchangedCatalogKeepsItsSuccessTime() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    Audit audit = mock(Audit.class);
    Instant modifiedTime = Instant.ofEpochMilli(1000L);
    when(audit.createTime()).thenReturn(modifiedTime);
    when(audit.lastModifiedTime()).thenReturn(modifiedTime);
    when(catalog.auditInfo()).thenReturn(audit);
    fixture.withCatalogs(catalog);
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    CatalogConnectorContext context =
        manager.createCatalogConnectorContext(
            "memory", createConnectorConfig(catalogConfigJson("test", "memory")), mockContext());
    when(context.getCatalog())
        .thenReturn(
            new GravitinoCatalog(
                "test", "memory", "memory", ImmutableMap.of(), modifiedTime.toEpochMilli()));
    long registeredAt = singleState(manager).getLastSuccessTimeMs();

    Thread.sleep(5);
    manager.loadMetalakeSync();

    // A poll that found nothing to do is not a new registration: advancing the time here would
    // make last_success_time say nothing more than last_attempt_time already does.
    CatalogRegistrationState state = singleState(manager);
    assertEquals(registeredAt, state.getLastSuccessTimeMs());
    // The poll still happened, so the row must not look like a catalog the loop stopped visiting.
    assertTrue(state.getLastAttemptTimeMs() > registeredAt);
    assertEquals(CatalogRegistrationState.Status.REGISTERED, state.getStatus());
  }

  @Test
  public void testTrinoBecomingUnreachableIsReported() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();
    assertTrue(manager.isTrinoReachable());

    // The coordinator restarted, or the connection behind the register died. A reachability that
    // was latched on the first success would keep the loop issuing statements over it and report
    // every catalog failing separately instead of the one reason they all did.
    when(fixture.catalogRegister.isTrinoReachable()).thenReturn(false);
    when(fixture.catalogRegister.getLastConnectionError()).thenReturn("Connection reset");
    manager.loadMetalakeSync();

    assertFalse(manager.isTrinoReachable());
    CatalogConnectorManager.LoadOutcome outcome = manager.getLoadOutcome();
    assertFalse(outcome.isTrinoReachable());
    assertTrue(outcome.getLastError().contains("The Trino server is not reachable"));
    assertTrue(outcome.getLastError().contains("Connection reset"));
  }

  @Test
  public void testUnsupportedUnregisterFailureKeepsTheCatalogVisible() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    fixture.withCatalogs(catalog);
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    CatalogConnectorContext context =
        manager.createCatalogConnectorContext(
            "memory", createConnectorConfig(catalogConfigJson("test", "memory")), mockContext());
    when(context.getMetalake()).thenReturn(fixture.metalake);
    when(context.getCatalog())
        .thenReturn(new GravitinoCatalog("test", "memory", "memory", ImmutableMap.of(), 0L));

    when(catalog.provider()).thenReturn("unsupported-provider");
    doThrow(new TrinoException(GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, "Access Denied"))
        .when(fixture.catalogRegister)
        .unregisterCatalog(any());
    manager.loadMetalakeSync();

    // Reporting it as merely UNSUPPORTED would hide that Trino is still serving it.
    assertTrue(manager.catalogConnectorExist("memory"));
    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.FAILED, state.getStatus());
    assertTrue(state.getLastError().contains("could not be unregistered"));
  }

  @Test
  public void testUnreachableTrinoReportsNoMetalakeErrors() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Mockito.doThrow(new RESTException("simulated: listing failed"))
        .when(fixture.metalake)
        .listCatalogs();
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();
    assertFalse(manager.getLoadOutcome().getMetalakeErrors().isEmpty());

    // Trino goes away. This cycle touches no metalake at all, so the errors left over from the
    // previous one say nothing about it.
    when(fixture.catalogRegister.isTrinoReachable()).thenReturn(false);
    manager.loadMetalakeSync();

    CatalogConnectorManager.LoadOutcome outcome = manager.getLoadOutcome();
    assertFalse(outcome.isTrinoReachable());
    assertTrue(outcome.getMetalakeErrors().isEmpty());
  }

  @Test
  public void testReloadFailureAfterUnregisterIsRecordedAsFailed() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    fixture.withCatalogs(catalog);
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());

    // Trino already has a live connector for this catalog, built from an older version.
    CatalogConnectorContext context =
        manager.createCatalogConnectorContext(
            "memory", createConnectorConfig(catalogConfigJson("test", "memory")), mockContext());
    when(context.getCatalog())
        .thenReturn(new GravitinoCatalog("test", "memory", "memory", ImmutableMap.of(), 0L));

    // The Gravitino server now reports a newer version, so the load loop takes the reload path:
    // reloadCatalog() unregisters the old connector before attempting to re-register it.
    Audit audit = mock(Audit.class);
    when(audit.createTime()).thenReturn(Instant.now());
    when(audit.lastModifiedTime()).thenReturn(Instant.now());
    when(catalog.auditInfo()).thenReturn(audit);

    doThrow(new TrinoException(GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, "Access Denied"))
        .when(fixture.catalogRegister)
        .registerCatalog(any(), any());

    manager.loadMetalakeSync();

    // The old connector is genuinely gone from Trino now; the state must say FAILED with the
    // real cause instead of assuming the catalog is still usable.
    assertFalse(manager.catalogConnectorExist("memory"));
    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.FAILED, state.getStatus());
    assertTrue(state.getLastError().contains("Access Denied"));
  }

  @Test
  public void testTrinoConnectionErrorIsReported() throws Exception {
    LoadFixture fixture = new LoadFixture();
    when(fixture.catalogRegister.isTrinoReachable()).thenReturn(false);
    when(fixture.catalogRegister.getLastConnectionError())
        .thenReturn("Authentication failed: Access Denied");

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    // "Waiting for Trino" alone would read the same for a misconfiguration that never resolves.
    assertFalse(manager.isTrinoReachable());
    CatalogConnectorManager.LoadOutcome loadOutcome = manager.getLoadOutcome();
    assertTrue(loadOutcome.getLastError().contains("Authentication failed"));
    assertEquals(1, loadOutcome.getConsecutiveFailures());
  }

  @Test
  public void testErrorMessageFallsBackWhenNoMessageIsPresent() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));
    doThrow(new IllegalStateException())
        .when(fixture.catalogRegister)
        .registerCatalog(any(), any());

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    // An exception with no message must not produce a null or empty last_error.
    CatalogRegistrationState state = singleState(manager);
    assertEquals(CatalogRegistrationState.Status.FAILED, state.getStatus());
    assertTrue(state.getLastError().contains("IllegalStateException"));
  }

  @Test
  public void testStateOfAVanishedMetalakeIsPruned() throws Exception {
    LoadFixture fixture = new LoadFixture();
    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));

    // Multi metalake mode, so the used metalakes come from listMetalakes().
    CatalogConnectorManager manager =
        fixture.createManager(ImmutableMap.of("gravitino.use-single-metalake", "false"));
    manager.loadMetalakeSync();
    assertEquals(1, manager.getCatalogRegistrationStates().size());

    // The metalake is deleted in Gravitino: its catalog rows must not linger as REGISTERED.
    Mockito.doReturn(new GravitinoMetalake[0]).when(fixture.client).listMetalakes();
    manager.loadMetalakeSync();

    assertTrue(manager.getCatalogRegistrationStates().isEmpty());
    assertTrue(manager.getMetalakeErrors().isEmpty());
  }

  @Test
  public void testRefreshIcebergRestUriCachesDiscoveredUri() throws Exception {
    GravitinoAdminClient client = mock(GravitinoAdminClient.class);
    CatalogRegister catalogRegister = mock(CatalogRegister.class);
    when(catalogRegister.isTrinoReachable()).thenReturn(true);
    when(client.loadMetalake("test")).thenReturn(mock(GravitinoMetalake.class));
    when(client.icebergRestServiceUri("test"))
        .thenReturn(Optional.of("http://irc-host:9001/iceberg"));

    CatalogConnectorManager manager =
        new CatalogConnectorManager(catalogRegister, createCatalogConnectorFactory(), null);
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.uri", "http://127.0.0.1:8090",
                "gravitino.metalake", "test",
                "gravitino.use-single-metalake", "true"));
    manager.config(config, client);

    manager.loadMetalakeSync();

    assertEquals("http://irc-host:9001/iceberg", config.getDiscoveredIcebergRestUri("test"));
  }

  @Test
  public void testRefreshIcebergRestUriSwallowsFailureAndKeepsCatalogLoadingGoing()
      throws Exception {
    GravitinoAdminClient client = mock(GravitinoAdminClient.class);
    CatalogRegister catalogRegister = mock(CatalogRegister.class);
    when(catalogRegister.isTrinoReachable()).thenReturn(true);
    when(client.loadMetalake("test")).thenReturn(mock(GravitinoMetalake.class));
    when(client.icebergRestServiceUri("test"))
        .thenThrow(new RESTException("simulated: endpoint not found on an older server"));

    CatalogConnectorManager manager =
        new CatalogConnectorManager(catalogRegister, createCatalogConnectorFactory(), null);
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.uri", "http://127.0.0.1:8090",
                "gravitino.metalake", "test",
                "gravitino.use-single-metalake", "true"));
    manager.config(config, client);

    // A discovery failure must not abort the metalake load (which loads catalogs), and must
    // leave the discovered URI at its previous value rather than throwing out of loadMetalake.
    assertDoesNotThrow(manager::loadMetalakeSync);
    assertEquals("", config.getDiscoveredIcebergRestUri("test"));
  }

  @Test
  public void testConfigRejectsInvalidIcebergRestRoutingEnabledAtStartup() throws Exception {
    GravitinoAdminClient client = mock(GravitinoAdminClient.class);
    CatalogRegister catalogRegister = mock(CatalogRegister.class);

    CatalogConnectorManager manager =
        new CatalogConnectorManager(catalogRegister, createCatalogConnectorFactory(), null);
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.uri", "http://127.0.0.1:8090",
                "gravitino.metalake", "test",
                "gravitino.use-single-metalake", "true",
                "gravitino.iceberg.rest-routing-enabled", "yes"));

    assertThrows(TrinoException.class, () -> manager.config(config, client));
  }

  @Test
  public void testIcebergRestRoutingDisabledSkipsDiscovery() throws Exception {
    GravitinoAdminClient client = mock(GravitinoAdminClient.class);
    CatalogRegister catalogRegister = mock(CatalogRegister.class);
    when(catalogRegister.isTrinoReachable()).thenReturn(true);
    when(client.loadMetalake("test")).thenReturn(mock(GravitinoMetalake.class));

    CatalogConnectorManager manager =
        new CatalogConnectorManager(catalogRegister, createCatalogConnectorFactory(), null);
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.uri", "http://127.0.0.1:8090",
                "gravitino.metalake", "test",
                "gravitino.use-single-metalake", "true",
                "gravitino.iceberg.rest-routing-enabled", "false"));
    manager.config(config, client);

    manager.loadMetalakeSync();

    verify(client, never()).icebergRestServiceUri("test");
  }

  @Test
  public void testConfiguredIcebergRestUriSkipsDiscovery() throws Exception {
    GravitinoAdminClient client = mock(GravitinoAdminClient.class);
    CatalogRegister catalogRegister = mock(CatalogRegister.class);
    when(catalogRegister.isTrinoReachable()).thenReturn(true);
    when(client.loadMetalake("test")).thenReturn(mock(GravitinoMetalake.class));

    CatalogConnectorManager manager =
        new CatalogConnectorManager(catalogRegister, createCatalogConnectorFactory(), null);
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.uri", "http://127.0.0.1:8090",
                "gravitino.metalake", "test",
                "gravitino.use-single-metalake", "true",
                "gravitino.iceberg.rest-uri", "http://irc-host:9001/iceberg"));
    manager.config(config, client);

    manager.loadMetalakeSync();

    verify(client, never()).icebergRestServiceUri("test");
  }

  @Test
  public void testIcebergRestDiscoveryRetriesAndRecovers() throws Exception {
    GravitinoAdminClient client = mock(GravitinoAdminClient.class);
    CatalogRegister catalogRegister = mock(CatalogRegister.class);
    when(catalogRegister.isTrinoReachable()).thenReturn(true);
    when(client.loadMetalake("test")).thenReturn(mock(GravitinoMetalake.class));
    when(client.icebergRestServiceUri("test"))
        .thenThrow(new RESTException("simulated discovery failure"))
        .thenReturn(Optional.of("http://irc-host:9001/iceberg"));

    CatalogConnectorManager manager =
        new CatalogConnectorManager(catalogRegister, createCatalogConnectorFactory(), null);
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.uri", "http://127.0.0.1:8090",
                "gravitino.metalake", "test",
                "gravitino.use-single-metalake", "true"));
    manager.config(config, client);

    manager.loadMetalakeSync();
    manager.loadMetalakeSync();

    verify(client, times(2)).icebergRestServiceUri("test");
    assertEquals("http://irc-host:9001/iceberg", config.getDiscoveredIcebergRestUri("test"));
  }

  @Test
  public void testRegistrationCarriesNoSecrets() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    when(catalog.properties()).thenReturn(Map.of("visible", "v1", "shared", "from-props"));
    when(catalog.supportsSecrets().getSecrets())
        .thenReturn(Map.of("jdbc-password", "hunter2", "shared", "from-secret"));
    fixture.withCatalogs(catalog);

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    // The registered definition is what the CREATE CATALOG statement carries and what Trino
    // persists as a catalog properties file, so no secret may be in it.
    ArgumentCaptor<GravitinoCatalog> registered = ArgumentCaptor.forClass(GravitinoCatalog.class);
    verify(fixture.catalogRegister).registerCatalog(eq("memory"), registered.capture());
    Map<String, String> properties = registered.getValue().getProperties();
    assertEquals("v1", properties.get("visible"));
    assertEquals("from-props", properties.get("shared"));
    assertFalse(properties.containsKey("jdbc-password"));
    assertFalse(properties.toString().contains("hunter2"));
  }

  @Test
  public void testConnectorContextResolvesSecrets() throws Exception {
    LoadFixture fixture = new LoadFixture();
    Catalog catalog = mockCatalog("memory", "memory", Catalog.Type.RELATIONAL);
    when(catalog.properties()).thenReturn(Map.of("visible", "v1", "shared", "from-props"));
    when(catalog.supportsSecrets().getSecrets())
        .thenReturn(Map.of("jdbc-password", "hunter2", "shared", "from-secret"));
    fixture.withCatalogs(catalog);
    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());

    // What Trino hands back to the node when it loads the catalog that was registered without
    // secrets.
    manager.createCatalogConnectorContext(
        "memory",
        createConnectorConfig(
            GravitinoCatalog.toJson(
                new GravitinoCatalog(
                    "test",
                    "memory",
                    "memory",
                    Map.of("visible", "v1", "shared", "from-props"),
                    0L))),
        mockContext());

    // The node resolves them against the server, and a secret wins over a visible property of
    // the same name the way the merge at registration time used to.
    ArgumentCaptor<GravitinoCatalog> built = ArgumentCaptor.forClass(GravitinoCatalog.class);
    verify(fixture.catalogFactory).createCatalogConnectorContextBuilder(built.capture());
    Map<String, String> properties = built.getValue().getProperties();
    assertEquals("v1", properties.get("visible"));
    assertEquals("hunter2", properties.get("jdbc-password"));
    assertEquals("from-secret", properties.get("shared"));
  }

  @Test
  public void testVisiblePropsHandlesNullProperties() {
    Catalog catalog = mock(Catalog.class);
    when(catalog.properties()).thenReturn(null);

    assertTrue(CatalogConnectorManager.visibleProps(catalog).isEmpty());
  }

  private CatalogConnectorManager createManager(ImmutableMap<String, String> configMap)
      throws Exception {
    return createManager(createCatalogConnectorFactory(), configMap);
  }

  private CatalogConnectorManager createManager(
      CatalogConnectorFactory catalogFactory, ImmutableMap<String, String> configMap) {
    CatalogRegister catalogRegister = mock(CatalogRegister.class);

    boolean singleMetalakeMode =
        configMap.getOrDefault("gravitino.use-single-metalake", "true").equals("true");
    CatalogConnectorManager manager =
        new CatalogConnectorManager(
            catalogRegister,
            catalogFactory,
            singleMetalakeMode
                ? null
                : (metalake, catalog) -> String.format("\"%s.%s\"", metalake, catalog));
    // Building a connector resolves the catalog's secrets against the server, so the client has
    // to answer for the metalake it is asked about.
    GravitinoAdminClient client = mock(GravitinoAdminClient.class);
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    Mockito.doReturn(metalake).when(client).loadMetalake(any());
    Mockito.doReturn(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL))
        .when(metalake)
        .loadCatalog(any());
    manager.config(new GravitinoConfig(configMap), client);
    return manager;
  }

  private CatalogConnectorFactory createCatalogConnectorFactory() throws Exception {
    CatalogConnectorFactory catalogFactory = mock(CatalogConnectorFactory.class);
    CatalogConnectorContext.Builder builder = mock(CatalogConnectorContext.Builder.class);
    when(catalogFactory.createCatalogConnectorContextBuilder(any())).thenReturn(builder);
    when(builder.withMetalake(any())).thenReturn(builder);
    when(builder.withContext(any())).thenReturn(builder);
    when(builder.build()).thenReturn(mock(CatalogConnectorContext.class));
    return catalogFactory;
  }

  private static GravitinoConfig createConnectorConfig(String catalogConfigJson) {
    return new GravitinoConfig(
        ImmutableMap.of(
            "gravitino.uri",
            "http://127.0.0.1:8090",
            "gravitino.metalake",
            "test",
            GravitinoConfig.GRAVITINO_DYNAMIC_CONNECTOR,
            "true",
            GravitinoConfig.GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG,
            catalogConfigJson));
  }

  private static String catalogConfigJson(String metalake, String name) throws Exception {
    GravitinoCatalog catalog =
        new GravitinoCatalog(metalake, "memory", name, ImmutableMap.of(), 0L);
    return GravitinoCatalog.toJson(catalog);
  }

  private static ConnectorContext mockContext() {
    return mock(ConnectorContext.class);
  }

  private static CatalogRegistrationState singleState(CatalogConnectorManager manager) {
    List<CatalogRegistrationState> states = manager.getCatalogRegistrationStates();
    assertEquals(1, states.size());
    return states.get(0);
  }

  private static Catalog mockCatalog(String name, String provider, Catalog.Type type) {
    Catalog catalog = mock(Catalog.class);
    when(catalog.name()).thenReturn(name);
    when(catalog.provider()).thenReturn(provider);
    when(catalog.type()).thenReturn(type);
    when(catalog.properties()).thenReturn(ImmutableMap.of());
    SupportsSecrets supportsSecrets = mock(SupportsSecrets.class);
    when(supportsSecrets.getSecrets()).thenReturn(Map.of());
    when(catalog.supportsSecrets()).thenReturn(supportsSecrets);
    Audit audit = mock(Audit.class);
    when(audit.createTime()).thenReturn(Instant.now());
    when(audit.lastModifiedTime()).thenReturn(null);
    when(catalog.auditInfo()).thenReturn(audit);
    return catalog;
  }

  /** Wires up a manager whose load loop can be driven with {@code loadMetalakeSync()}. */
  private static class LoadFixture {
    private final CatalogRegister catalogRegister = mock(CatalogRegister.class);
    private final GravitinoAdminClient client = mock(GravitinoAdminClient.class);
    private final GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    private final CatalogConnectorFactory catalogFactory = mock(CatalogConnectorFactory.class);

    LoadFixture() throws Exception {
      when(catalogRegister.isTrinoReachable()).thenReturn(true);
      when(metalake.name()).thenReturn("test");
      when(client.loadMetalake(any())).thenReturn(metalake);
      Mockito.doReturn(new GravitinoMetalake[] {metalake}).when(client).listMetalakes();
      when(catalogFactory.getSupportedCatalogProviders()).thenReturn(ImmutableSet.of("memory"));
      CatalogConnectorContext.Builder builder = mock(CatalogConnectorContext.Builder.class);
      when(catalogFactory.createCatalogConnectorContextBuilder(any())).thenReturn(builder);
      when(builder.withMetalake(any())).thenReturn(builder);
      when(builder.withContext(any())).thenReturn(builder);
      when(builder.build()).thenReturn(mock(CatalogConnectorContext.class));
    }

    void withCatalogs(Catalog... catalogs) {
      String[] names = new String[catalogs.length];
      for (int i = 0; i < catalogs.length; i++) {
        names[i] = catalogs[i].name();
        Mockito.doReturn(catalogs[i]).when(metalake).loadCatalog(names[i]);
      }
      Mockito.doReturn(names).when(metalake).listCatalogs();
    }

    CatalogConnectorManager createManager(Map<String, String> extraConfig) {
      return createManager(extraConfig, (metalakeName, catalog) -> catalog);
    }

    CatalogConnectorManager createManager(
        Map<String, String> extraConfig,
        CatalogConnectorManager.TrinoCatalogNameHandler nameHandler) {
      CatalogConnectorManager manager =
          new CatalogConnectorManager(catalogRegister, catalogFactory, nameHandler);
      manager.config(config(extraConfig), client);
      return manager;
    }

    /** The manager configuration this fixture builds, so a test can re-apply a changed one. */
    GravitinoConfig config(Map<String, String> extraConfig) {
      Map<String, String> defaults = new HashMap<>();
      defaults.put("gravitino.uri", "http://127.0.0.1:8090");
      defaults.put("gravitino.metalake", "test");
      defaults.put("gravitino.use-single-metalake", "true");
      defaults.putAll(extraConfig);
      return new GravitinoConfig(ImmutableMap.copyOf(defaults));
    }
  }
}
