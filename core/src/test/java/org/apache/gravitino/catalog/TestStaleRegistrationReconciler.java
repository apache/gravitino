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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.Configs.RECONCILE_CATALOG_JITTER_SECS;
import static org.apache.gravitino.Configs.RECONCILE_ENABLED;
import static org.apache.gravitino.Configs.RECONCILE_INTERVAL_SECS;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY1;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY2;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY5_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link StaleRegistrationReconciler} (report-only phase). */
public class TestStaleRegistrationReconciler extends TestOperationDispatcher {

  private StaleRegistrationReconciler reconciler;

  private static final String CATALOG2 = "catalog2";

  private StaleRegistrationReconciler createReconciler(Config config) {
    return new StaleRegistrationReconciler(config, entityStore, catalogManager);
  }

  @AfterEach
  public void closeReconciler() throws IOException {
    if (reconciler != null) {
      reconciler.close();
      reconciler = null;
    }
  }

  @BeforeEach
  public void cleanUpReconcileFixtures() throws IOException {
    // The in-memory entity store is shared by all test methods of this class; drop the schema
    // registrations and the optional catalog2 left behind by other test methods.
    for (String catalogName : new String[] {catalog, CATALOG2}) {
      List<SchemaEntity> schemas =
          entityStore.list(
              Namespace.of(metalake, catalogName), SchemaEntity.class, Entity.EntityType.SCHEMA);
      for (SchemaEntity schema : schemas) {
        entityStore.delete(schema.nameIdentifier(), Entity.EntityType.SCHEMA, false);
      }
    }
    entityStore.delete(NameIdentifier.of(metalake, CATALOG2), Entity.EntityType.CATALOG, true);
  }

  private static Config createConfig() {
    Config config = new Config(false) {};
    config.set(RECONCILE_INTERVAL_SECS, 3600L);
    config.set(RECONCILE_CATALOG_JITTER_SECS, 0L);
    return config;
  }

  private void createCatalog2() {
    catalogManager.createCatalog(
        NameIdentifier.of(metalake, CATALOG2),
        Catalog.Type.RELATIONAL,
        "test",
        "comment",
        ImmutableMap.of(
            PROPERTY_KEY1,
            "value1",
            PROPERTY_KEY2,
            "value2",
            PROPERTY_KEY5_PREFIX + "1",
            "value3"));
  }

  private static SchemaEntity createSchemaEntity(String metalake, String catalog, String name) {
    return SchemaEntity.builder()
        .withId(1L)
        .withName(name)
        .withNamespace(Namespace.of(metalake, catalog))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }

  @Test
  public void testReconcilePolicyParsing() {
    assertEquals(ReconcilePolicy.REPORT_ONLY, ReconcilePolicy.fromString("report-only"));
    assertEquals(ReconcilePolicy.REPORT_ONLY, ReconcilePolicy.fromString("REPORT_ONLY"));
    assertEquals(ReconcilePolicy.AUTO_REMOVE, ReconcilePolicy.fromString("auto-remove"));
    assertEquals(ReconcilePolicy.AUTO_REMOVE, ReconcilePolicy.fromString(" Auto-Remove "));
    assertEquals(ReconcilePolicy.REPORT_ONLY, ReconcilePolicy.fromString(null));
    assertEquals(ReconcilePolicy.REPORT_ONLY, ReconcilePolicy.fromString("unknown"));
    assertEquals("report-only", ReconcilePolicy.REPORT_ONLY.configValue());
    assertEquals("auto-remove", ReconcilePolicy.AUTO_REMOVE.configValue());
  }

  @Test
  public void testReconcileDefaults() {
    Config config = new Config(false) {};
    assertFalse(config.get(RECONCILE_ENABLED));
    assertEquals(3600L, config.get(RECONCILE_INTERVAL_SECS));
    assertEquals(60L, config.get(RECONCILE_CATALOG_JITTER_SECS));
    assertEquals("report-only", config.get(Configs.RECONCILE_POLICY));
  }

  @Test
  public void testAutoRemovePolicyRejectedUntilImplemented() {
    Config config = new Config(false) {};
    config.set(Configs.RECONCILE_POLICY, "auto-remove");

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> config.get(Configs.RECONCILE_POLICY));
  }

  @Test
  public void testStartDisabledByDefault() {
    Config config = new Config(false) {};
    reconciler = createReconciler(config);

    reconciler.start();

    assertEquals(0, reconciler.reconcilePool.getTaskCount());
  }

  @Test
  public void testStartEnabledSchedulesRound() {
    Config config = createConfig();
    config.set(RECONCILE_ENABLED, true);
    config.set(RECONCILE_INTERVAL_SECS, 3600L);
    reconciler = createReconciler(config);

    reconciler.start();

    assertTrue(reconciler.reconcilePool.getTaskCount() > 0);
  }

  @Test
  public void testScheduledRoundSummarizesReports() throws Exception {
    reconciler = createReconciler(createConfig());
    entityStore.put(createSchemaEntity(metalake, catalog, "schema_stale"), true);

    StaleRegistrationReconciler.RoundSummary summary = reconciler.runScheduledRound();

    assertEquals(1, summary.scannedCatalogs());
    assertEquals(0, summary.unreachableCatalogs());
    assertEquals(1, summary.staleSchemas());
    assertEquals(
        ImmutableList.of(metalake + "." + catalog + ".schema_stale"), summary.staleSchemaIdents());
  }

  @Test
  public void testJitterBounds() throws IOException {
    Config config = createConfig();
    config.set(RECONCILE_CATALOG_JITTER_SECS, 0L);
    reconciler = createReconciler(config);
    assertEquals(0L, reconciler.nextJitterMillis());

    config.set(RECONCILE_CATALOG_JITTER_SECS, 60L);
    reconciler.close();
    reconciler = createReconciler(config);
    for (int i = 0; i < 1000; i++) {
      long jitterMillis = reconciler.nextJitterMillis();
      assertTrue(jitterMillis >= 0L);
      assertTrue(jitterMillis < 60_000L);
    }
  }

  @Test
  public void testProbeSchemaClassification() throws Exception {
    CatalogManager.CatalogWrapper wrapper = mock(CatalogManager.CatalogWrapper.class);
    SupportsSchemas schemaOps = mock(SupportsSchemas.class);
    stubSchemaOps(wrapper, schemaOps);

    NameIdentifier ident = NameIdentifier.of(Namespace.of("m", "c"), "s");

    doReturn(true).when(schemaOps).schemaExists(ident);
    assertEquals(
        StaleRegistrationReconciler.ProbeResult.PRESENT,
        StaleRegistrationReconciler.probeSchema(wrapper, ident));

    doReturn(false).when(schemaOps).schemaExists(ident);
    assertEquals(
        StaleRegistrationReconciler.ProbeResult.ABSENT,
        StaleRegistrationReconciler.probeSchema(wrapper, ident));

    doThrow(new NoSuchSchemaException("schema not found")).when(schemaOps).schemaExists(ident);
    assertEquals(
        StaleRegistrationReconciler.ProbeResult.ABSENT,
        StaleRegistrationReconciler.probeSchema(wrapper, ident));

    doThrow(new ConnectionFailedException("source unreachable"))
        .when(schemaOps)
        .schemaExists(ident);
    assertEquals(
        StaleRegistrationReconciler.ProbeResult.UNREACHABLE,
        StaleRegistrationReconciler.probeSchema(wrapper, ident));

    doThrow(new UnsupportedOperationException("no schema ops"))
        .when(wrapper)
        .doWithSchemaOps(any());
    assertEquals(
        StaleRegistrationReconciler.ProbeResult.UNSUPPORTED,
        StaleRegistrationReconciler.probeSchema(wrapper, ident));
  }

  @Test
  public void testSelectOutermostStaleNames() {
    // Parent and children all stale: only the outermost is kept.
    List<String> hierarchical =
        StaleRegistrationReconciler.selectOutermostStaleNames(
            ImmutableSet.of("A:B:C", "A:B", "A"), ":");
    assertEquals(ImmutableList.of("A"), hierarchical);

    // Mixed tree: covered children are dropped, unrelated names are kept.
    List<String> mixed =
        StaleRegistrationReconciler.selectOutermostStaleNames(
            ImmutableSet.of("A:B:C", "A:B", "X", "Y:Z"), ":");
    assertEquals(ImmutableList.of("A:B", "X", "Y:Z"), mixed);

    // A stale child with a present parent is itself outermost.
    List<String> staleChild =
        StaleRegistrationReconciler.selectOutermostStaleNames(ImmutableSet.of("A:B"), ":");
    assertEquals(ImmutableList.of("A:B"), staleChild);

    // Flat names are never filtered.
    List<String> flat =
        StaleRegistrationReconciler.selectOutermostStaleNames(ImmutableSet.of("a", "b"), ":");
    assertEquals(ImmutableList.of("a", "b"), flat);

    assertTrue(
        StaleRegistrationReconciler.selectOutermostStaleNames(Collections.emptySet(), ":")
            .isEmpty());
  }

  @Test
  public void testReconcileCatalogReportsStaleRegistration() throws Exception {
    reconciler = createReconciler(createConfig());
    entityStore.put(createSchemaEntity(metalake, catalog, "schema_stale"), true);

    StaleCatalogReport report = reconciler.reconcileCatalog(metalake, catalog);

    assertFalse(report.catalogUnreachable());
    assertEquals(1, report.staleSchemas().size());
    StaleSchemaEntry entry = report.staleSchemas().get(0);
    assertEquals("schema_stale", entry.name());
    assertFalse(entry.removed());
    assertTrue(entry.detectedAt() > 0);
  }

  @Test
  public void testReconcileCatalogWithNoRegistrations() throws Exception {
    reconciler = createReconciler(createConfig());

    StaleCatalogReport report = reconciler.reconcileCatalog(metalake, catalog);

    assertFalse(report.catalogUnreachable());
    assertTrue(report.staleSchemas().isEmpty());
  }

  @Test
  public void testReconcileRoundIsolatesCatalogFailures() throws Exception {
    reconciler = createReconciler(createConfig());
    catalogManager.createCatalog(
        NameIdentifier.of(metalake, CATALOG2),
        Catalog.Type.RELATIONAL,
        "test",
        "comment",
        ImmutableMap.of(
            PROPERTY_KEY1,
            "value1",
            PROPERTY_KEY2,
            "value2",
            PROPERTY_KEY5_PREFIX + "1",
            "value3"));
    entityStore.put(createSchemaEntity(metalake, CATALOG2, "schema_stale2"), true);

    // Make the shared catalog fail in its reconcile: the store list for its schema
    // registrations throws, which propagates out of reconcileCatalog and must be isolated
    // by the round so the other catalog is still reconciled.
    doThrow(new IOException("store failure"))
        .when(entityStore)
        .list(
            eq(Namespace.of(metalake, catalog)),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA));

    List<StaleCatalogReport> reports = reconciler.reconcileRound();

    List<String> reconciledCatalogs =
        reports.stream().map(StaleCatalogReport::catalog).collect(Collectors.toList());
    assertFalse(reconciledCatalogs.contains(catalog));
    assertTrue(reconciledCatalogs.contains(CATALOG2));
    StaleCatalogReport healthyReport =
        reports.stream()
            .filter(r -> r.catalog().equals(CATALOG2))
            .collect(Collectors.toList())
            .get(0);
    assertEquals(1, healthyReport.staleSchemas().size());
    assertEquals("schema_stale2", healthyReport.staleSchemas().get(0).name());
  }

  @Test
  public void testRunningRoundStopsWhenClosed() throws Exception {
    reconciler = createReconciler(createConfig());
    createCatalog2();
    // Clear the calls made by the @BeforeEach cleanup so verify(never()) sees this round only.
    clearInvocations(entityStore);

    // Block the first catalog inside its schema listing, like a slow source.
    CountDownLatch enteredFirstCatalog = new CountDownLatch(1);
    CountDownLatch releaseFirstCatalog = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              enteredFirstCatalog.countDown();
              releaseFirstCatalog.await(10, TimeUnit.SECONDS);
              return invocation.callRealMethod();
            })
        .when(entityStore)
        .list(
            eq(Namespace.of(metalake, catalog)),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA));

    AtomicReference<Throwable> workerFailure = new AtomicReference<>();
    Thread worker =
        new Thread(
            () -> {
              try {
                reconciler.reconcileRound();
              } catch (Throwable t) {
                workerFailure.set(t);
              }
            });
    worker.start();
    assertTrue(enteredFirstCatalog.await(10, TimeUnit.SECONDS));

    reconciler.close();
    releaseFirstCatalog.countDown();
    worker.join(10000);

    assertFalse(worker.isAlive());
    Assertions.assertNull(workerFailure.get());
    verify(entityStore, never())
        .list(
            eq(Namespace.of(metalake, CATALOG2)),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA));
  }

  @Test
  public void testRunningRoundStopsOnInterruption() throws Exception {
    reconciler = createReconciler(createConfig());
    createCatalog2();
    // Clear the calls made by the @BeforeEach cleanup so verify(never()) sees this round only.
    clearInvocations(entityStore);

    CountDownLatch enteredFirstCatalog = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              enteredFirstCatalog.countDown();
              Thread.sleep(30000);
              return invocation.callRealMethod();
            })
        .when(entityStore)
        .list(
            eq(Namespace.of(metalake, catalog)),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA));

    AtomicReference<Throwable> workerFailure = new AtomicReference<>();
    Thread worker =
        new Thread(
            () -> {
              try {
                reconciler.reconcileRound();
              } catch (Throwable t) {
                workerFailure.set(t);
              }
            });
    worker.start();
    assertTrue(enteredFirstCatalog.await(10, TimeUnit.SECONDS));

    worker.interrupt();
    worker.join(10000);

    assertFalse(worker.isAlive());
    // The round loop restored the interrupt status.
    assertTrue(worker.isInterrupted());
    verify(entityStore, never())
        .list(
            eq(Namespace.of(metalake, CATALOG2)),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA));
  }

  @Test
  public void testReconcileRoundReportsStaleRegistrations() throws Exception {
    reconciler = createReconciler(createConfig());
    entityStore.put(createSchemaEntity(metalake, catalog, "schema_round_stale"), true);

    List<StaleCatalogReport> reports = reconciler.reconcileRound();

    assertEquals(1, reports.size());
    StaleCatalogReport report = reports.get(0);
    assertEquals(metalake, report.metalake());
    assertEquals(catalog, report.catalog());
    assertFalse(report.catalogUnreachable());
    assertEquals(1, report.staleSchemas().size());
    assertEquals("schema_round_stale", report.staleSchemas().get(0).name());
  }

  @Test
  public void testReconcileCatalogSkipsManagedCatalog() throws Exception {
    Capability managedCapability =
        new Capability.DefaultCapability() {
          @Override
          public CapabilityResult managedStorage(Scope scope) {
            return CapabilityResult.SUPPORTED;
          }
        };
    CatalogManager.CatalogWrapper wrapper = mock(CatalogManager.CatalogWrapper.class);
    doReturn(managedCapability).when(wrapper).capabilities();
    CatalogManager mockCatalogManager = mock(CatalogManager.class);
    stubCatalogWrapper(mockCatalogManager, wrapper);

    reconciler = new StaleRegistrationReconciler(createConfig(), entityStore, mockCatalogManager);
    entityStore.put(createSchemaEntity(metalake, catalog, "schema_any"), true);

    StaleCatalogReport report = reconciler.reconcileCatalog(metalake, catalog);

    assertTrue(report.staleSchemas().isEmpty());
    assertFalse(report.catalogUnreachable());
    verify(wrapper, never()).doWithSchemaOps(any());
  }

  @Test
  public void testReconcileCatalogSkipsUnsupportedSchemaOps() throws Exception {
    CatalogManager.CatalogWrapper wrapper = mock(CatalogManager.CatalogWrapper.class);
    doReturn(Capability.DEFAULT).when(wrapper).capabilities();
    doThrow(new UnsupportedOperationException("no schema operations"))
        .when(wrapper)
        .doWithSchemaOps(any());
    CatalogManager mockCatalogManager = mock(CatalogManager.class);
    stubCatalogWrapper(mockCatalogManager, wrapper);

    reconciler = new StaleRegistrationReconciler(createConfig(), entityStore, mockCatalogManager);
    entityStore.put(createSchemaEntity(metalake, catalog, "schema_any"), true);

    StaleCatalogReport report = reconciler.reconcileCatalog(metalake, catalog);

    assertTrue(report.staleSchemas().isEmpty());
    assertFalse(report.catalogUnreachable());
  }

  @Test
  public void testReconcileCatalogMarksUnreachableWhenProbeFails() throws Exception {
    CatalogManager.CatalogWrapper wrapper = mock(CatalogManager.CatalogWrapper.class);
    doReturn(Capability.DEFAULT).when(wrapper).capabilities();
    doThrow(new ConnectionFailedException("source unreachable"))
        .when(wrapper)
        .doWithSchemaOps(any());
    CatalogManager mockCatalogManager = mock(CatalogManager.class);
    stubCatalogWrapper(mockCatalogManager, wrapper);

    reconciler = new StaleRegistrationReconciler(createConfig(), entityStore, mockCatalogManager);
    entityStore.put(createSchemaEntity(metalake, catalog, "schema_any"), true);

    StaleCatalogReport report = reconciler.reconcileCatalog(metalake, catalog);

    assertTrue(report.catalogUnreachable());
    assertTrue(report.staleSchemas().isEmpty());
  }

  private static void stubSchemaOps(
      CatalogManager.CatalogWrapper wrapper, SupportsSchemas schemaOps) throws Exception {
    doAnswer(
            invocation -> {
              ThrowableFunction<SupportsSchemas, Object> fn = invocation.getArgument(0);
              return fn.apply(schemaOps);
            })
        .when(wrapper)
        .doWithSchemaOps(any());
  }

  private static void stubCatalogWrapper(
      CatalogManager mockCatalogManager, CatalogManager.CatalogWrapper wrapper) throws Exception {
    doAnswer(
            invocation -> {
              ThrowableFunction<CatalogManager.CatalogWrapper, Object> fn =
                  invocation.getArgument(1);
              return fn.apply(wrapper);
            })
        .when(mockCatalogManager)
        .doWithCatalogWrapper(any(), any());
  }

  @Test
  public void testEntryGetters() {
    StaleSchemaEntry entry = new StaleSchemaEntry("s", 123L, false);
    assertEquals("s", entry.name());
    assertEquals(123L, entry.detectedAt());
    assertFalse(entry.removed());
  }

  @Test
  public void testReportFactoriesAndUnmodifiableList() {
    StaleCatalogReport empty = StaleCatalogReport.empty("m", "c");
    assertEquals("m", empty.metalake());
    assertEquals("c", empty.catalog());
    assertFalse(empty.catalogUnreachable());
    assertTrue(empty.staleSchemas().isEmpty());

    StaleCatalogReport unreachable = StaleCatalogReport.unreachable("m", "c");
    assertTrue(unreachable.catalogUnreachable());
    assertTrue(unreachable.staleSchemas().isEmpty());

    StaleCatalogReport report =
        StaleCatalogReport.of("m", "c", Arrays.asList(new StaleSchemaEntry("s", 1L, false)));
    assertEquals(1, report.staleSchemas().size());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> report.staleSchemas().clear());
  }
}
