/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.service.EntityDeletionService;
import org.apache.gravitino.storage.relational.service.TableDeletionService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;

/** Cross-backend transaction tests for the Iceberg-specific DELETE coordinator. */
public class TestIcebergTableDeletionLifecycle extends TestJDBCBackend {

  private static final String METALAKE = "iceberg_deletion_metalake";
  private static final String CATALOG = "iceberg_deletion_catalog";
  private static final String SCHEMA = "sales";
  private static final String TABLE = "orders";

  private IcebergRequestContext requestContext;
  private TableIdentifier icebergIdentifier;
  private NameIdentifier gravitinoIdentifier;
  private TableEntity table;

  @BeforeEach
  public void setUpLifecycle() throws IOException {
    when(GravitinoEnv.getInstance().config().get(Configs.ENABLE_AUTHORIZATION)).thenReturn(false);
    createParentEntities(METALAKE, CATALOG, SCHEMA, AUDIT_INFO);
    Namespace namespace = NamespaceUtil.ofTable(METALAKE, CATALOG, SCHEMA);
    gravitinoIdentifier = NameIdentifier.of(namespace, TABLE);
    table = createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, TABLE, AUDIT_INFO);
    backend.insert(table, false);

    requestContext = mock(IcebergRequestContext.class);
    icebergIdentifier = TableIdentifier.of(org.apache.iceberg.catalog.Namespace.of(SCHEMA), TABLE);
    when(requestContext.catalogName()).thenReturn(CATALOG);

    IcebergConfigProvider provider = mock(IcebergConfigProvider.class);
    when(provider.getMetalakeName()).thenReturn(METALAKE);
    when(provider.getDefaultCatalogName()).thenReturn(CATALOG);
    IcebergRESTServerContext.create(provider, false, true, true, null);
  }

  @TestTemplate
  public void testDeleteStoresOnlyTheActionAndTombstonesTheExistingRoot()
      throws IOException, SQLException {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);
    long originalTableId = tableId();
    long beforeChange = maxChangeId();

    lifecycle.delete(requestContext, icebergIdentifier, true);

    assertFalse(backend.exists(gravitinoIdentifier, Entity.EntityType.TABLE));
    IcebergRetainedTableDeletion retained = onlyRetained();
    EntityDeletionPO deletion = retained.getDeletion();
    TablePO table = retained.getTable();
    assertEquals(originalTableId, table.getTableId());
    assertEquals(TABLE, table.getTableName());
    assertEquals(deletion.getDeletionId(), table.getDeletionId());
    assertEquals(IcebergTableDeletionLifecycle.DELETED, deletion.getState());
    assertEquals(table.getDeletedAt() + 86_400_000L, deletion.getRetentionExpiresAt());
    assertNull(deletion.getPurgeJobId());
    assertTrue(lifecycle.isNameReserved(CATALOG, icebergIdentifier));
    assertEquals(
        Set.of(TABLE), lifecycle.reservedTableNames(CATALOG, icebergIdentifier.namespace()));
    assertEquals(1, lifecycle.listDeleted(CATALOG, icebergIdentifier.namespace()).size());
    assertEquals(
        TABLE, lifecycle.listDeleted(CATALOG, icebergIdentifier.namespace()).get(0).getTableName());
    assertEquals(
        deletion.getDeletionId(),
        lifecycle.findActive(CATALOG, icebergIdentifier).getDeletion().getDeletionId());
    assertTrue(
        lifecycle
            .reservedTableNames(CATALOG, org.apache.iceberg.catalog.Namespace.of("missing-parent"))
            .isEmpty());
    assertThrows(
        NoSuchNamespaceException.class,
        () ->
            lifecycle.listDeleted(
                CATALOG, org.apache.iceberg.catalog.Namespace.of("missing-parent")));
    assertChange(beforeChange, OperateType.DROP);
  }

  @TestTemplate
  public void testRepeatedDeleteReusesTheActiveDeletion() throws SQLException {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);

    lifecycle.delete(requestContext, icebergIdentifier, false);
    String deletionId = onlyRetained().getDeletion().getDeletionId();
    lifecycle.delete(requestContext, icebergIdentifier, true);

    assertEquals(deletionId, onlyRetained().getDeletion().getDeletionId());
    assertEquals(1L, selectLong("SELECT COUNT(*) FROM entity_deletion"));
  }

  @TestTemplate
  public void testConcurrentDeleteCreatesOneDeletionGeneration() throws Exception {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);
    long beforeChange = maxChangeId();
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> first =
          executor.submit(
              () -> {
                ready.countDown();
                await(start);
                lifecycle.delete(requestContext, icebergIdentifier, false);
              });
      Future<?> second =
          executor.submit(
              () -> {
                ready.countDown();
                await(start);
                lifecycle.delete(requestContext, icebergIdentifier, false);
              });

      assertTrue(ready.await(10, TimeUnit.SECONDS));
      start.countDown();
      first.get(10, TimeUnit.SECONDS);
      second.get(10, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    assertNotNull(onlyRetained());
    assertEquals(1L, selectLong("SELECT COUNT(*) FROM entity_deletion"));
    assertChange(beforeChange, OperateType.DROP);
  }

  @TestTemplate
  public void testDeleteInvalidatesAWarmedLocalTableCache() {
    CaffeineEntityCache cache = new CaffeineEntityCache(new Config(false) {});
    cache.put(table);
    assertTrue(cache.getIfPresent(gravitinoIdentifier, Entity.EntityType.TABLE).isPresent());
    IcebergTableDeletionLifecycle lifecycle =
        lifecycle(true, 86_400_000L, true, new IcebergTableCacheInvalidator(cache::invalidate));

    lifecycle.delete(requestContext, icebergIdentifier, false);

    assertTrue(cache.getIfPresent(gravitinoIdentifier, Entity.EntityType.TABLE).isEmpty());
  }

  @TestTemplate
  public void testDeleteMissingTableIsNotFound() {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);
    TableIdentifier missing =
        TableIdentifier.of(org.apache.iceberg.catalog.Namespace.of(SCHEMA), "missing");

    assertThrows(
        NoSuchTableException.class, () -> lifecycle.delete(requestContext, missing, false));
  }

  @TestTemplate
  public void testDisabledSoftDeleteKeepsBothRequestsOnLegacyPaths() throws SQLException {
    IcebergTableDeletionLifecycle disabled = lifecycle(false, 86_400_000L);

    assertFalse(disabled.manages(false));
    assertFalse(disabled.manages(true));
    assertThrows(
        IllegalStateException.class,
        () -> disabled.delete(requestContext, icebergIdentifier, true));
    assertEquals(0L, selectLong("SELECT COUNT(*) FROM entity_deletion"));
  }

  @TestTemplate
  public void testDisabledSoftDeleteStillReservesExistingDeletion() {
    lifecycle(true, 86_400_000L).delete(requestContext, icebergIdentifier, false);
    IcebergTableDeletionLifecycle disabled = lifecycle(false, 86_400_000L);

    assertFalse(disabled.manages(false));
    assertTrue(disabled.isNameReserved(CATALOG, icebergIdentifier));
    assertEquals(
        Set.of(TABLE), disabled.reservedTableNames(CATALOG, icebergIdentifier.namespace()));
    assertNotNull(disabled.getDeleted(CATALOG, icebergIdentifier));
  }

  @TestTemplate
  public void testRetentionZeroHasNoRecoveryWindow() throws SQLException {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 0L);
    lifecycle.delete(requestContext, icebergIdentifier, false);

    IcebergRetainedTableDeletion retained = onlyRetained();
    assertEquals(
        retained.getTable().getDeletedAt(), retained.getDeletion().getRetentionExpiresAt());
    assertFalse(lifecycle.toAction(retained, retained.getTable().getDeletedAt()).isRecoverable());
  }

  @TestTemplate
  public void testNullRetentionHasNoAutomaticExpiry() throws SQLException {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);
    lifecycle.delete(requestContext, icebergIdentifier, false);

    IcebergRetainedTableDeletion retained = onlyRetained();
    retained.getDeletion().setRetentionExpiresAt(null);
    assertNull(retained.getDeletion().getRetentionExpiresAt());
    assertNull(lifecycle.toAction(retained, Long.MAX_VALUE).getRetentionExpiresAt());
    assertTrue(lifecycle.toAction(retained, Long.MAX_VALUE).isRecoverable());
  }

  @TestTemplate
  public void testDeletedListMapsMissingNamespaceToSafeNotFound() {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);

    IcebergDeletionException exception =
        assertThrows(
            IcebergDeletionException.class,
            () ->
                lifecycle.listDeleted(
                    CATALOG, org.apache.iceberg.catalog.Namespace.of("missing-parent")));
    assertEquals(IcebergDeletionException.Outcome.NOT_FOUND, exception.outcome());
    assertEquals("Deleted table is not available", exception.getMessage());
  }

  @TestTemplate
  public void testUndropReactivatesTheOriginalRootAndConsumesTheAction()
      throws IOException, SQLException {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);
    lifecycle.delete(requestContext, icebergIdentifier, false);
    IcebergRetainedTableDeletion retained = onlyRetained();
    String deletionId = retained.getDeletion().getDeletionId();
    long tableId = retained.getTable().getTableId();
    long version = retained.getTable().getCurrentVersion();
    long beforeRestoreChange = maxChangeId();

    lifecycle.undrop(requestContext, icebergIdentifier, deletionId);

    assertTrue(backend.exists(gravitinoIdentifier, Entity.EntityType.TABLE));
    assertEquals(
        1L,
        selectLong(
            "SELECT COUNT(*) FROM table_meta WHERE table_id = "
                + tableId
                + " AND current_version = "
                + version
                + " AND deletion_id IS NULL AND deleted_at = 0"));
    assertFalse(lifecycle.isNameReserved(CATALOG, icebergIdentifier));
    assertNull(EntityDeletionService.getInstance().get(deletionId));
    assertChange(beforeRestoreChange, OperateType.ALTER);

    IcebergDeletionException replay =
        assertThrows(
            IcebergDeletionException.class,
            () -> lifecycle.undrop(requestContext, icebergIdentifier, deletionId));
    assertEquals(IcebergDeletionException.Outcome.NOT_FOUND, replay.outcome());
  }

  @TestTemplate
  public void testUndropInvalidatesTheLocalTableCacheAfterCommit()
      throws IOException, SQLException {
    AtomicInteger invalidations = new AtomicInteger();
    IcebergTableDeletionLifecycle lifecycle =
        lifecycle(
            true,
            86_400_000L,
            true,
            new IcebergTableCacheInvalidator(
                (identifier, type) -> {
                  invalidations.incrementAndGet();
                  return true;
                }));
    lifecycle.delete(requestContext, icebergIdentifier, false);
    String deletionId = onlyRetained().getDeletion().getDeletionId();

    lifecycle.undrop(requestContext, icebergIdentifier, deletionId);

    assertEquals(2, invalidations.get());
  }

  @TestTemplate
  public void testUndropRejectsADeletionAuthorizedForAnotherTableIdentity()
      throws IOException, SQLException {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);
    lifecycle.delete(requestContext, icebergIdentifier, false);
    IcebergRetainedTableDeletion retained = onlyRetained();
    String deletionId = retained.getDeletion().getDeletionId();

    IcebergDeletionException mismatch =
        assertThrows(
            IcebergDeletionException.class,
            () ->
                lifecycle.undrop(
                    requestContext,
                    icebergIdentifier,
                    deletionId,
                    retained.getTable().getTableId() + 1));

    assertEquals(IcebergDeletionException.Outcome.NOT_FOUND, mismatch.outcome());
    assertNotNull(TableDeletionService.getInstance().getRetainedTable(deletionId));
  }

  @TestTemplate
  public void testUndropRollsBackWhenTheChangeRecordFailsAndCanRetry()
      throws IOException, SQLException {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);
    lifecycle.delete(requestContext, icebergIdentifier, false);
    String deletionId = onlyRetained().getDeletion().getDeletionId();
    long beforeRestoreChange = maxChangeId();

    try (MockedStatic<SessionUtils> sessions = mockStatic(SessionUtils.class, CALLS_REAL_METHODS)) {
      sessions
          .when(
              () ->
                  SessionUtils.doWithoutCommit(
                      ArgumentMatchers.eq(EntityChangeLogMapper.class),
                      ArgumentMatchers.<Consumer<EntityChangeLogMapper>>any()))
          .thenThrow(new RuntimeException("injected change-log failure"));

      RuntimeException error =
          assertThrows(
              RuntimeException.class,
              () -> lifecycle.undrop(requestContext, icebergIdentifier, deletionId));
      assertEquals("injected change-log failure", error.getMessage());
    }

    assertFalse(backend.exists(gravitinoIdentifier, Entity.EntityType.TABLE));
    assertNotNull(EntityDeletionService.getInstance().get(deletionId));
    assertNotNull(TableDeletionService.getInstance().getRetainedTable(deletionId));
    assertEquals(beforeRestoreChange, maxChangeId());

    lifecycle.undrop(requestContext, icebergIdentifier, deletionId);

    assertTrue(backend.exists(gravitinoIdentifier, Entity.EntityType.TABLE));
    assertNull(EntityDeletionService.getInstance().get(deletionId));
    assertChange(beforeRestoreChange, OperateType.ALTER);
  }

  @TestTemplate
  public void testConcurrentUndropTransitionsOnlyOnce() throws Exception {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);
    lifecycle.delete(requestContext, icebergIdentifier, false);
    String deletionId = onlyRetained().getDeletion().getDeletionId();
    long beforeRestoreChange = maxChangeId();
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Callable<String> undrop =
        () -> {
          ready.countDown();
          assertTrue(start.await(10, TimeUnit.SECONDS));
          try {
            lifecycle.undrop(requestContext, icebergIdentifier, deletionId);
            return "SUCCESS";
          } catch (IcebergDeletionException e) {
            return e.outcome().name();
          }
        };

    try {
      Future<String> first = executor.submit(undrop);
      Future<String> second = executor.submit(undrop);
      assertTrue(ready.await(10, TimeUnit.SECONDS));
      start.countDown();
      List<String> results =
          List.of(first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS));

      assertEquals(1, Collections.frequency(results, "SUCCESS"));
      assertTrue(results.contains("NOT_FOUND") || results.contains("CONFLICT"), results.toString());
    } finally {
      start.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }

    assertTrue(backend.exists(gravitinoIdentifier, Entity.EntityType.TABLE));
    assertNull(EntityDeletionService.getInstance().get(deletionId));
    assertNull(TableDeletionService.getInstance().getRetainedTable(deletionId));
    assertChange(beforeRestoreChange, OperateType.ALTER);
  }

  @TestTemplate
  public void testUndropRejectsExpiredAndPurgeOwnedActions() throws IOException, SQLException {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 0L);
    lifecycle.delete(requestContext, icebergIdentifier, false);
    IcebergRetainedTableDeletion retained = onlyRetained();
    assertGone(lifecycle, retained.getDeletion().getDeletionId());

    execute(
        "UPDATE entity_deletion SET state = 'PURGING', purge_job_id = 'job-1', "
            + "retention_expires_at = "
            + (retained.getTable().getDeletedAt() + 86_400_000L)
            + " WHERE deletion_id = '"
            + retained.getDeletion().getDeletionId()
            + "'");
    assertGone(lifecycle, retained.getDeletion().getDeletionId());
  }

  @TestTemplate
  public void testUndropPreflightConcealsWrongRoute() throws IOException, SQLException {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L);
    lifecycle.delete(requestContext, icebergIdentifier, false);
    String deletionId = onlyRetained().getDeletion().getDeletionId();

    IcebergDeletionException error =
        assertThrows(
            IcebergDeletionException.class,
            () ->
                lifecycle.getUndropAction(
                    CATALOG,
                    TableIdentifier.of(icebergIdentifier.namespace(), "other"),
                    deletionId));

    assertEquals(IcebergDeletionException.Outcome.NOT_FOUND, error.outcome());
    assertNotNull(EntityDeletionService.getInstance().get(deletionId));
    assertNotNull(TableDeletionService.getInstance().getRetainedTable(deletionId));
  }

  @TestTemplate
  public void testUnavailableLifecyclePreservesLegacyRouting() {
    IcebergTableDeletionLifecycle lifecycle = lifecycle(true, 86_400_000L, false);

    assertFalse(lifecycle.manages(false));
    assertFalse(lifecycle.manages(true));
    assertFalse(lifecycle.isNameReserved(CATALOG, icebergIdentifier));
    assertTrue(lifecycle.reservedTableNames(CATALOG, icebergIdentifier.namespace()).isEmpty());
    assertTrue(lifecycle.listDeleted(CATALOG, icebergIdentifier.namespace()).isEmpty());
    assertThrows(
        IcebergDeletionException.class, () -> lifecycle.getDeleted(CATALOG, icebergIdentifier));
  }

  private IcebergTableDeletionLifecycle lifecycle(boolean enabled, long retentionMs) {
    return lifecycle(enabled, retentionMs, true);
  }

  private IcebergTableDeletionLifecycle lifecycle(
      boolean enabled, long retentionMs, boolean available) {
    return lifecycle(enabled, retentionMs, available, new IcebergTableCacheInvalidator());
  }

  private IcebergTableDeletionLifecycle lifecycle(
      boolean enabled,
      long retentionMs,
      boolean available,
      IcebergTableCacheInvalidator cacheInvalidator) {
    Map<String, String> properties = new HashMap<>();
    properties.put("soft-delete.enabled", String.valueOf(enabled));
    properties.put("soft-delete.retention-ms", String.valueOf(retentionMs));
    return new IcebergTableDeletionLifecycle(
        new IcebergConfig(properties), available, cacheInvalidator);
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while coordinating concurrent deletes", e);
    }
  }

  private IcebergRetainedTableDeletion onlyRetained() throws SQLException {
    String deletionId = selectString("SELECT deletion_id FROM entity_deletion");
    EntityDeletionPO deletion = EntityDeletionService.getInstance().get(deletionId);
    TablePO table = TableDeletionService.getInstance().getRetainedTable(deletionId);
    assertNotNull(deletion);
    assertNotNull(table);
    return IcebergRetainedTableDeletion.builder().deletion(deletion).table(table).build();
  }

  private long tableId() throws SQLException {
    return selectLong("SELECT table_id FROM table_meta WHERE table_name = '" + TABLE + "'");
  }

  private long maxChangeId() {
    return SessionUtils.doWithCommitAndFetchResult(
        EntityChangeLogMapper.class, EntityChangeLogMapper::selectMaxChangeId);
  }

  private void assertChange(long after, OperateType expected) {
    List<EntityChangeRecord> changes =
        SessionUtils.doWithCommitAndFetchResult(
            EntityChangeLogMapper.class, mapper -> mapper.selectEntityChanges(after, 10));
    assertEquals(1, changes.size());
    assertEquals(expected, changes.get(0).getOperateType());
    assertEquals(gravitinoIdentifier.toString(), changes.get(0).getFullName());
  }

  private void assertGone(IcebergTableDeletionLifecycle lifecycle, String deletionId) {
    IcebergDeletionException error =
        assertThrows(
            IcebergDeletionException.class,
            () -> lifecycle.undrop(requestContext, icebergIdentifier, deletionId));
    assertEquals(IcebergDeletionException.Outcome.GONE, error.outcome());
  }

  private void execute(String sql) throws SQLException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate(sql);
    }
  }

  private String selectString(String sql) throws SQLException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      assertTrue(resultSet.next());
      return resultSet.getString(1);
    }
  }

  private long selectLong(String sql) throws SQLException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      assertTrue(resultSet.next());
      return resultSet.getLong(1);
    }
  }
}
