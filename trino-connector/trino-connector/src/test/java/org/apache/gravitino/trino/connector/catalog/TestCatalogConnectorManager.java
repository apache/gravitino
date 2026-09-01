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
    assertEquals(GravitinoErrorCode.GRAVITINO_OPERATION_FAILED.toErrorCode(), error.getErrorCode());
    assertTrue(error.getMessage().contains("Failed to create connector"));
    assertTrue(error.getCause().getMessage().contains("Multiple metalakes are not supported"));
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

    assertTrue(manager.isTrinoStarted());
    assertNull(manager.getLastLoadError());
    assertEquals(0, manager.getConsecutiveLoadFailures());
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

    assertNotNull(manager.getLastLoadError());
    assertTrue(manager.getLastLoadError().contains("Connection refused"));
    assertEquals(1, manager.getConsecutiveLoadFailures());
    assertEquals(0, manager.getLastSuccessfulLoadTimeMs());
    assertTrue(manager.getLastLoadAttemptTimeMs() > 0);
    assertTrue(manager.getCatalogRegistrationStates().isEmpty());
  }

  @Test
  public void testTrinoNotStartedIsRecordedInLoadStatus() throws Exception {
    LoadFixture fixture = new LoadFixture();
    when(fixture.catalogRegister.isTrinoStarted()).thenReturn(false);

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    assertFalse(manager.isTrinoStarted());
    assertNotNull(manager.getLastLoadError());
    assertTrue(manager.getLastLoadError().contains("Waiting for the Trino server"));
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
    when(fixture.catalogRegister.isTrinoStarted()).thenReturn(true);
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
    assertNotNull(manager.getLastLoadError());
    assertEquals(0, manager.getLastSuccessfulLoadTimeMs());

    fixture.withCatalogs(mockCatalog("memory", "memory", Catalog.Type.RELATIONAL));
    manager.loadMetalakeSync();

    assertTrue(manager.getMetalakeErrors().isEmpty());
    assertNull(manager.getLastLoadError());
    assertEquals(0, manager.getConsecutiveLoadFailures());
    assertTrue(manager.getLastSuccessfulLoadTimeMs() > 0);
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
    when(fixture.catalogRegister.isTrinoStarted()).thenReturn(false);
    when(fixture.catalogRegister.getLastConnectionError())
        .thenReturn("Authentication failed: Access Denied");

    CatalogConnectorManager manager = fixture.createManager(ImmutableMap.of());
    manager.loadMetalakeSync();

    // "Waiting for Trino" alone would read the same for a misconfiguration that never resolves.
    assertFalse(manager.isTrinoStarted());
    assertTrue(manager.getLastLoadError().contains("Authentication failed"));
    assertEquals(1, manager.getConsecutiveLoadFailures());
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
    when(catalogRegister.isTrinoStarted()).thenReturn(true);
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
    when(catalogRegister.isTrinoStarted()).thenReturn(true);
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
    when(catalogRegister.isTrinoStarted()).thenReturn(true);
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
    when(catalogRegister.isTrinoStarted()).thenReturn(true);
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
    when(catalogRegister.isTrinoStarted()).thenReturn(true);
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
  public void testPropsWithSecrets() {
    Catalog catalog = mock(Catalog.class);
    SupportsSecrets supportsSecrets = mock(SupportsSecrets.class);
    when(catalog.properties()).thenReturn(Map.of("visible", "v1", "shared", "from-props"));
    when(catalog.supportsSecrets()).thenReturn(supportsSecrets);
    when(supportsSecrets.getSecrets())
        .thenReturn(Map.of("jdbc-password", "secret", "shared", "from-secret"));

    Map<String, String> merged = CatalogConnectorManager.propsWithSecrets(catalog);

    assertEquals("v1", merged.get("visible"));
    assertEquals("secret", merged.get("jdbc-password"));
    assertEquals("from-secret", merged.get("shared"));
  }

  @Test
  public void testPropsWithSecretsNullProps() {
    Catalog catalog = mock(Catalog.class);
    SupportsSecrets supportsSecrets = mock(SupportsSecrets.class);
    when(catalog.properties()).thenReturn(null);
    when(catalog.supportsSecrets()).thenReturn(supportsSecrets);
    when(supportsSecrets.getSecrets()).thenReturn(Map.of("jdbc-password", "secret"));

    Map<String, String> merged = CatalogConnectorManager.propsWithSecrets(catalog);

    assertEquals("secret", merged.get("jdbc-password"));
    assertEquals(1, merged.size());
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
    manager.config(new GravitinoConfig(configMap), mock(GravitinoAdminClient.class));
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
      when(catalogRegister.isTrinoStarted()).thenReturn(true);
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
      Map<String, String> defaults = new HashMap<>();
      defaults.put("gravitino.uri", "http://127.0.0.1:8090");
      defaults.put("gravitino.metalake", "test");
      defaults.put("gravitino.use-single-metalake", "true");
      defaults.putAll(extraConfig);
      ImmutableMap<String, String> configMap = ImmutableMap.copyOf(defaults);
      CatalogConnectorManager manager =
          new CatalogConnectorManager(
              catalogRegister, catalogFactory, (metalakeName, catalog) -> catalog);
      manager.config(new GravitinoConfig(configMap), client);
      return manager;
    }
  }
}
