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
package org.apache.gravitino.storage.relational.service;

import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.IllegalStatisticNameException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.StatisticEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TableStatisticEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.StatisticPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestStatisticMetaService extends TestJDBCBackend {
  private final StatisticMetaService statisticMetaService = StatisticMetaService.getInstance();

  /** Verifies the version snapshot query excludes unrelated statistic rows. */
  @TestTemplate
  public void testStatisticSnapshotLoadsOnlyRequestedNames() throws Exception {
    String metalake = "statistic_filtered_metalake";
    String catalog = "statistic_filtered_catalog";
    String schema = "statistic_filtered_schema";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    createParentEntities(metalake, catalog, schema, auditInfo);
    TableEntity table =
        createAndInsertTableEntity(Namespace.of(metalake, catalog, schema), "statistic_filtered");
    StatisticEntity unrelated =
        TableStatisticEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("unrelated")
            .withValue(StatisticValues.longValue(2L))
            .withAuditInfo(auditInfo)
            .build();
    statisticMetaService.writeStatisticsWithVersion(
        List.of(createStatisticEntity(auditInfo, 1L), unrelated),
        table.nameIdentifier(),
        Entity.EntityType.TABLE);

    NamespacedEntityId endpoint =
        EntityIdService.getEntityIds(table.nameIdentifier(), Entity.EntityType.TABLE);
    List<StatisticPO> selected = statisticMetaService.listStatisticPOs(endpoint, List.of("test"));
    Assertions.assertEquals(1, selected.size());
    Assertions.assertEquals("test", selected.get(0).getStatisticName());
  }

  /** Verifies two writers that observe a missing statistic get one conflict rather than a 500. */
  @TestTemplate
  public void testConcurrentFirstStatisticWritesReportConflict() throws Exception {
    String metalake = "statistic_first_write_metalake";
    String catalog = "statistic_first_write_catalog";
    String schema = "statistic_first_write_schema";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    createParentEntities(metalake, catalog, schema, auditInfo);
    TableEntity table =
        createAndInsertTableEntity(
            Namespace.of(metalake, catalog, schema), "statistic_first_write");

    CyclicBarrier bothReadMissing = new CyclicBarrier(2);
    StatisticMetaService racingService =
        new StatisticMetaService() {
          @Override
          List<StatisticPO> listStatisticPOs(NamespacedEntityId endpoint, List<String> names) {
            List<StatisticPO> rows = super.listStatisticPOs(endpoint, names);
            try {
              bothReadMissing.await(30, TimeUnit.SECONDS);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            return rows;
          }
        };
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Throwable> first =
          executor.submit(() -> writeFirstStatistic(racingService, table, auditInfo, 1L));
      Future<Throwable> second =
          executor.submit(() -> writeFirstStatistic(racingService, table, auditInfo, 2L));

      Throwable firstFailure = first.get(30, TimeUnit.SECONDS);
      Throwable secondFailure = second.get(30, TimeUnit.SECONDS);
      Assertions.assertTrue((firstFailure == null) != (secondFailure == null));
      Throwable conflict = firstFailure == null ? secondFailure : firstFailure;
      Assertions.assertInstanceOf(OptimisticLockException.class, conflict);
      // The duplicate-key failure stays attached for diagnosis.
      Assertions.assertNotNull(conflict.getCause());
      Assertions.assertEquals(
          1,
          statisticMetaService
              .listStatisticsByEntity(table.nameIdentifier(), Entity.EntityType.TABLE)
              .size());
    } finally {
      executor.shutdownNow();
    }
  }

  private Throwable writeFirstStatistic(
      StatisticMetaService service, TableEntity table, AuditInfo auditInfo, long value) {
    try {
      service.writeStatisticsWithVersion(
          List.of(createStatisticEntity(auditInfo, value)),
          table.nameIdentifier(),
          Entity.EntityType.TABLE);
      return null;
    } catch (Throwable failure) {
      return failure;
    }
  }

  /** Verifies stale mapper updates and deletes cannot change a newer statistic version. */
  @TestTemplate
  public void testStatisticVersionCompareAndSet() throws Exception {
    String metalake = "statistic_cas_metalake";
    String catalog = "statistic_cas_catalog";
    String schema = "statistic_cas_schema";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    createParentEntities(metalake, catalog, schema, auditInfo);
    TableEntity table =
        createAndInsertTableEntity(Namespace.of(metalake, catalog, schema), "statistic_cas_table");
    StatisticEntity initial = createStatisticEntity(auditInfo, 10L);
    statisticMetaService.writeStatisticsWithVersion(
        List.of(initial), table.nameIdentifier(), Entity.EntityType.TABLE);
    Long metalakeId =
        EntityIdService.getEntityId(NameIdentifier.of(metalake), Entity.EntityType.METALAKE);
    StatisticPO stale =
        SessionUtils.getWithoutCommit(
                StatisticMetaMapper.class,
                mapper -> mapper.listStatisticPOsByEntityId(metalakeId, table.id()))
            .get(0);
    Assertions.assertEquals(1L, stale.getCurrentVersion());

    // Identity, target and version alone must not permit a delete for a different name.
    StatisticPO wrongName =
        StatisticPO.builder()
            .withMetalakeId(stale.getMetalakeId())
            .withStatisticId(stale.getStatisticId())
            .withMetadataObjectId(stale.getMetadataObjectId())
            .withMetadataObjectType(stale.getMetadataObjectType())
            .withStatisticName("different_name")
            .withStatisticValue(stale.getStatisticValue())
            .withAuditInfo(stale.getAuditInfo())
            .withCurrentVersion(stale.getCurrentVersion())
            .withLastVersion(stale.getLastVersion())
            .withDeletedAt(stale.getDeletedAt())
            .build();
    int wrongNameDeleted =
        SessionUtils.getWithoutCommit(
            StatisticMetaMapper.class,
            mapper -> mapper.batchDeleteStatisticPOsWithVersion(List.of(wrongName)));
    Assertions.assertEquals(0, wrongNameDeleted);

    StatisticEntity replacement = createStatisticEntity(auditInfo, 20L);
    statisticMetaService.writeStatisticsWithVersion(
        List.of(replacement), table.nameIdentifier(), Entity.EntityType.TABLE);
    StatisticPO current =
        SessionUtils.getWithoutCommit(
                StatisticMetaMapper.class,
                mapper -> mapper.listStatisticPOsByEntityId(metalakeId, table.id()))
            .get(0);
    Assertions.assertEquals(stale.getStatisticId(), current.getStatisticId());
    Assertions.assertEquals(2L, current.getCurrentVersion());
    Assertions.assertEquals(1L, current.getLastVersion());
    String staleValue =
        StatisticPO.initializeStatisticPOs(
                List.of(createStatisticEntity(auditInfo, 30L)),
                metalakeId,
                table.id(),
                MetadataObject.Type.TABLE)
            .get(0)
            .getStatisticValue();
    StatisticPO staleReplacement =
        StatisticPO.builder()
            .withMetalakeId(stale.getMetalakeId())
            .withStatisticId(stale.getStatisticId())
            .withMetadataObjectId(stale.getMetadataObjectId())
            .withMetadataObjectType(stale.getMetadataObjectType())
            .withStatisticName(stale.getStatisticName())
            .withStatisticValue(staleValue)
            .withAuditInfo(stale.getAuditInfo())
            .withCurrentVersion(stale.getCurrentVersion())
            .withLastVersion(stale.getLastVersion())
            .withDeletedAt(stale.getDeletedAt())
            .build();
    int staleUpdated =
        SessionUtils.getWithoutCommit(
            StatisticMetaMapper.class,
            mapper -> mapper.batchUpdateStatisticPOsWithVersion(List.of(staleReplacement)));
    Assertions.assertEquals(0, staleUpdated);
    int staleDeleted =
        SessionUtils.getWithoutCommit(
            StatisticMetaMapper.class,
            mapper -> mapper.batchDeleteStatisticPOsWithVersion(List.of(stale)));
    Assertions.assertEquals(0, staleDeleted);
    Assertions.assertEquals(
        20L,
        statisticMetaService
            .listStatisticsByEntity(table.nameIdentifier(), Entity.EntityType.TABLE)
            .get(0)
            .value()
            .value());
  }

  /** Verifies a write or delete fails when its target is dropped after the version snapshot. */
  @TestTemplate
  public void testStatisticWritesFailWhenTargetDroppedAfterSnapshot() throws Exception {
    String metalake = "statistic_dropped_target_metalake";
    String catalog = "statistic_dropped_target_catalog";
    String schema = "statistic_dropped_target_schema";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    createParentEntities(metalake, catalog, schema, auditInfo);
    Long metalakeId =
        EntityIdService.getEntityId(NameIdentifier.of(metalake), Entity.EntityType.METALAKE);

    TableEntity writeTarget =
        createAndInsertTableEntity(Namespace.of(metalake, catalog, schema), "dropped_write");
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            dropTableAfterSnapshot(writeTarget)
                .writeStatisticsWithVersion(
                    List.of(createStatisticEntity(auditInfo, 1L)),
                    writeTarget.nameIdentifier(),
                    Entity.EntityType.TABLE));
    Assertions.assertEquals(0, countActiveStats(metalakeId));

    TableEntity deleteTarget =
        createAndInsertTableEntity(Namespace.of(metalake, catalog, schema), "dropped_delete");
    statisticMetaService.writeStatisticsWithVersion(
        List.of(createStatisticEntity(auditInfo, 1L)),
        deleteTarget.nameIdentifier(),
        Entity.EntityType.TABLE);
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            dropTableAfterSnapshot(deleteTarget)
                .batchDeleteStatisticPOs(
                    deleteTarget.nameIdentifier(), Entity.EntityType.TABLE, List.of("test")));
    Assertions.assertEquals(0, countActiveStats(metalakeId));
  }

  /** Verifies a drop that loses to a concurrent drop of the same name is not a conflict. */
  @TestTemplate
  public void testConcurrentDropOfSameStatisticIsNotConflict() throws Exception {
    String metalake = "statistic_double_drop_metalake";
    String catalog = "statistic_double_drop_catalog";
    String schema = "statistic_double_drop_schema";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    createParentEntities(metalake, catalog, schema, auditInfo);
    TableEntity table =
        createAndInsertTableEntity(Namespace.of(metalake, catalog, schema), "double_drop");
    statisticMetaService.writeStatisticsWithVersion(
        List.of(createStatisticEntity(auditInfo, 1L)),
        table.nameIdentifier(),
        Entity.EntityType.TABLE);

    StatisticMetaService losingDrop =
        new StatisticMetaService() {
          @Override
          List<StatisticPO> listStatisticPOs(NamespacedEntityId endpoint, List<String> names) {
            List<StatisticPO> rows = super.listStatisticPOs(endpoint, names);
            Assertions.assertEquals(
                1,
                statisticMetaService.batchDeleteStatisticPOs(
                    table.nameIdentifier(), Entity.EntityType.TABLE, names));
            return rows;
          }
        };

    Assertions.assertEquals(
        0,
        losingDrop.batchDeleteStatisticPOs(
            table.nameIdentifier(), Entity.EntityType.TABLE, List.of("test")));
    Assertions.assertTrue(
        statisticMetaService
            .listStatisticsByEntity(table.nameIdentifier(), Entity.EntityType.TABLE)
            .isEmpty());
  }

  /** Verifies one batch statement replaces each statistic with its own value. */
  @TestTemplate
  public void testBatchReplacementKeepsValuesPerStatistic() throws Exception {
    TableEntity table = createBatchConflictTable("replace");
    statisticMetaService.writeStatisticsWithVersion(
        List.of(
            createNamedStatistic("a", 1L),
            createNamedStatistic("b", 2L),
            createNamedStatistic("c", 3L)),
        table.nameIdentifier(),
        Entity.EntityType.TABLE);

    statisticMetaService.writeStatisticsWithVersion(
        List.of(
            createNamedStatistic("c", 30L),
            createNamedStatistic("d", 40L),
            createNamedStatistic("a", 10L)),
        table.nameIdentifier(),
        Entity.EntityType.TABLE);

    Map<String, StatisticEntity> current = statisticsByName(table);
    Assertions.assertEquals(4, current.size());
    Assertions.assertEquals(10L, current.get("a").value().value());
    Assertions.assertEquals(2L, current.get("b").value().value());
    Assertions.assertEquals(30L, current.get("c").value().value());
    Assertions.assertEquals(40L, current.get("d").value().value());
  }

  /** Verifies a batch drop counts only its own deletes when a concurrent drop removed one name. */
  @TestTemplate
  public void testBatchDropCountsOnlyItsOwnDeletes() throws Exception {
    TableEntity table = createBatchConflictTable("partial_drop");
    statisticMetaService.writeStatisticsWithVersion(
        List.of(createNamedStatistic("a", 1L), createNamedStatistic("b", 2L)),
        table.nameIdentifier(),
        Entity.EntityType.TABLE);

    StatisticMetaService losingDrop =
        new StatisticMetaService() {
          @Override
          List<StatisticPO> listStatisticPOs(NamespacedEntityId endpoint, List<String> names) {
            List<StatisticPO> rows = super.listStatisticPOs(endpoint, names);
            Assertions.assertEquals(
                1,
                statisticMetaService.batchDeleteStatisticPOs(
                    table.nameIdentifier(), Entity.EntityType.TABLE, List.of("a")));
            return rows;
          }
        };

    Assertions.assertEquals(
        1,
        losingDrop.batchDeleteStatisticPOs(
            table.nameIdentifier(), Entity.EntityType.TABLE, List.of("a", "b")));
    Assertions.assertTrue(statisticsByName(table).isEmpty());
  }

  /** Verifies concurrency failures of batch statements become conflicts on writes and drops. */
  @TestTemplate
  public void testBatchStatementConcurrencyFailuresAreConflicts() throws Exception {
    TableEntity table = createBatchConflictTable("deadlock");
    statisticMetaService.writeStatisticsWithVersion(
        List.of(createNamedStatistic("a", 1L)), table.nameIdentifier(), Entity.EntityType.TABLE);

    for (SQLException failure :
        List.of(
            new SQLException("MySQL deadlock", "40001", 1213),
            new SQLException("PostgreSQL deadlock", "40P01"),
            new SQLException("serialization failure", "40001"),
            new SQLException("MySQL lock wait timeout", "40001", 1205),
            new SQLException("MySQL lock wait timeout", "HY000", 1205))) {
      StatisticMetaService failing = failingStatements(new PersistenceException(failure));
      OptimisticLockException writeConflict =
          Assertions.assertThrows(
              OptimisticLockException.class,
              () ->
                  failing.writeStatisticsWithVersion(
                      List.of(createNamedStatistic("a", 2L), createNamedStatistic("b", 2L)),
                      table.nameIdentifier(),
                      Entity.EntityType.TABLE));
      Assertions.assertSame(failure, writeConflict.getCause().getCause());
      Assertions.assertTrue(writeConflict.getMessage().contains("[a, b]"));
      Assertions.assertTrue(writeConflict.getMessage().contains(table.nameIdentifier().toString()));

      OptimisticLockException dropConflict =
          Assertions.assertThrows(
              OptimisticLockException.class,
              () ->
                  failing.batchDeleteStatisticPOs(
                      table.nameIdentifier(), Entity.EntityType.TABLE, List.of("a")));
      Assertions.assertSame(failure, dropConflict.getCause().getCause());
    }

    // A duplicate key on a drop is a conflict. On a write it is a conflict only if a live row now
    // has one of the inserted names (see testConcurrentFirstStatisticWritesReportConflict);
    // otherwise the names collided with each other in the backend collation.
    SQLException duplicate = new SQLException("duplicate key", "23505");
    StatisticMetaService duplicating = failingStatements(new PersistenceException(duplicate));
    IllegalStatisticNameException collision =
        Assertions.assertThrows(
            IllegalStatisticNameException.class,
            () ->
                duplicating.writeStatisticsWithVersion(
                    List.of(createNamedStatistic("b", 2L)),
                    table.nameIdentifier(),
                    Entity.EntityType.TABLE));
    Assertions.assertSame(duplicate, collision.getCause().getCause());
    OptimisticLockException dropDuplicate =
        Assertions.assertThrows(
            OptimisticLockException.class,
            () ->
                duplicating.batchDeleteStatisticPOs(
                    table.nameIdentifier(), Entity.EntityType.TABLE, List.of("a")));
    Assertions.assertSame(duplicate, dropDuplicate.getCause().getCause());

    PersistenceException connectionFailure =
        new PersistenceException(new SQLException("connection lost", "08006"));
    StatisticMetaService failing = failingStatements(connectionFailure);
    Assertions.assertSame(
        connectionFailure,
        Assertions.assertThrows(
            PersistenceException.class,
            () ->
                failing.writeStatisticsWithVersion(
                    List.of(createNamedStatistic("a", 3L)),
                    table.nameIdentifier(),
                    Entity.EntityType.TABLE)));
    Assertions.assertSame(
        connectionFailure,
        Assertions.assertThrows(
            PersistenceException.class,
            () ->
                failing.batchDeleteStatisticPOs(
                    table.nameIdentifier(), Entity.EntityType.TABLE, List.of("a"))));

    Map<String, StatisticEntity> remaining = statisticsByName(table);
    Assertions.assertEquals(1, remaining.size());
    Assertions.assertEquals(1L, remaining.get("a").value().value());
  }

  /** Verifies a write of a name equal to an existing one under a padding collation is illegal. */
  @TestTemplate
  public void testWriteOfNameDifferingOnlyInTrailingSpaces() throws Exception {
    TableEntity table = createBatchConflictTable("trailing_space_write");
    statisticMetaService.writeStatisticsWithVersion(
        List.of(createNamedStatistic("name", 1L)), table.nameIdentifier(), Entity.EntityType.TABLE);

    List<StatisticEntity> padded = List.of(createNamedStatistic("name ", 2L));
    if ("mysql".equalsIgnoreCase(backendType)) {
      // MySQL's collation treats "name " as "name", so the write can never succeed.
      Assertions.assertThrows(
          IllegalStatisticNameException.class,
          () ->
              statisticMetaService.writeStatisticsWithVersion(
                  padded, table.nameIdentifier(), Entity.EntityType.TABLE));
      Assertions.assertEquals(1, statisticsByName(table).size());
    } else {
      statisticMetaService.writeStatisticsWithVersion(
          padded, table.nameIdentifier(), Entity.EntityType.TABLE);
      Assertions.assertEquals(2L, statisticsByName(table).get("name ").value().value());
    }
    Assertions.assertEquals(1L, statisticsByName(table).get("name").value().value());
  }

  /** Verifies names that collide only under a padding collation in one request are illegal. */
  @TestTemplate
  public void testWriteOfTrailingSpaceAliasesInOneRequest() throws Exception {
    TableEntity table = createBatchConflictTable("trailing_space_batch");
    List<StatisticEntity> bothNew =
        List.of(createNamedStatistic("x", 1L), createNamedStatistic("x ", 2L));
    statisticMetaService.writeStatisticsWithVersion(
        List.of(createNamedStatistic("y", 1L)), table.nameIdentifier(), Entity.EntityType.TABLE);
    List<StatisticEntity> updateAndAlias =
        List.of(createNamedStatistic("y", 10L), createNamedStatistic("y ", 20L));

    if ("mysql".equalsIgnoreCase(backendType)) {
      // MySQL's unique key treats "x " as "x", so these requests can never succeed.
      for (List<StatisticEntity> request : List.of(bothNew, updateAndAlias)) {
        Assertions.assertThrows(
            IllegalStatisticNameException.class,
            () ->
                statisticMetaService.writeStatisticsWithVersion(
                    request, table.nameIdentifier(), Entity.EntityType.TABLE));
      }
      Map<String, StatisticEntity> current = statisticsByName(table);
      Assertions.assertEquals(1, current.size());
      Assertions.assertEquals(1L, current.get("y").value().value());
    } else {
      statisticMetaService.writeStatisticsWithVersion(
          bothNew, table.nameIdentifier(), Entity.EntityType.TABLE);
      statisticMetaService.writeStatisticsWithVersion(
          updateAndAlias, table.nameIdentifier(), Entity.EntityType.TABLE);
      Map<String, StatisticEntity> current = statisticsByName(table);
      Assertions.assertEquals(4, current.size());
      Assertions.assertEquals(10L, current.get("y").value().value());
      Assertions.assertEquals(20L, current.get("y ").value().value());
    }
  }

  /**
   * Verifies a drop matches only the exact requested name, even under a padding collation. Only the
   * MySQL backend, whose collation ignores trailing spaces, can return "name" for "name ".
   */
  @TestTemplate
  public void testDropDoesNotDropNameWithoutTrailingSpace() throws Exception {
    TableEntity table = createBatchConflictTable("trailing_space");
    statisticMetaService.writeStatisticsWithVersion(
        List.of(createNamedStatistic("name", 1L)), table.nameIdentifier(), Entity.EntityType.TABLE);

    Assertions.assertEquals(
        0,
        statisticMetaService.batchDeleteStatisticPOs(
            table.nameIdentifier(), Entity.EntityType.TABLE, List.of("name ")));
    Assertions.assertEquals(1L, statisticsByName(table).get("name").value().value());
  }

  /** Verifies a write or delete based on a stale version fails without touching the newer value. */
  @TestTemplate
  public void testStaleStatisticWriteAndDeleteReportConflict() throws Exception {
    String metalake = "statistic_stale_service_metalake";
    String catalog = "statistic_stale_service_catalog";
    String schema = "statistic_stale_service_schema";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    createParentEntities(metalake, catalog, schema, auditInfo);
    TableEntity table =
        createAndInsertTableEntity(Namespace.of(metalake, catalog, schema), "stale_service");
    statisticMetaService.writeStatisticsWithVersion(
        List.of(createStatisticEntity(auditInfo, 1L)),
        table.nameIdentifier(),
        Entity.EntityType.TABLE);

    OptimisticLockException staleWrite =
        Assertions.assertThrows(
            OptimisticLockException.class,
            () ->
                updateAfterSnapshot(table, auditInfo, 2L)
                    .writeStatisticsWithVersion(
                        List.of(createStatisticEntity(auditInfo, 3L)),
                        table.nameIdentifier(),
                        Entity.EntityType.TABLE));
    Assertions.assertTrue(staleWrite.getMessage().contains("retry the operation"));
    Assertions.assertEquals(2L, singleStatisticValue(table));

    Assertions.assertThrows(
        OptimisticLockException.class,
        () ->
            updateAfterSnapshot(table, auditInfo, 4L)
                .batchDeleteStatisticPOs(
                    table.nameIdentifier(), Entity.EntityType.TABLE, List.of("test")));
    Assertions.assertEquals(4L, singleStatisticValue(table));
  }

  /** Verifies a late write conflict rolls back earlier updates and inserts in the batch. */
  @TestTemplate
  public void testStatisticWriteConflictRollsBackWholeBatch() throws Exception {
    TableEntity table = createBatchConflictTable("write");
    StatisticEntity unchanged = createNamedStatistic("a_existing", 10L);
    statisticMetaService.writeStatisticsWithVersion(
        List.of(unchanged, createStatisticEntity(AUDIT_INFO, 1L)),
        table.nameIdentifier(),
        Entity.EntityType.TABLE);

    OptimisticLockException conflict =
        Assertions.assertThrows(
            OptimisticLockException.class,
            () ->
                updateAfterSnapshot(table, AUDIT_INFO, 2L)
                    .writeStatisticsWithVersion(
                        // A stale update in the batch must also roll back the insert of b_new.
                        List.of(
                            createStatisticEntity(AUDIT_INFO, 3L),
                            createNamedStatistic("b_new", 30L),
                            createNamedStatistic("a_existing", 20L)),
                        table.nameIdentifier(),
                        Entity.EntityType.TABLE));
    assertStatisticConflict(conflict, table);
    Map<String, StatisticEntity> remaining = statisticsByName(table);
    Assertions.assertEquals(2, remaining.size());
    Assertions.assertEquals(unchanged.fields(), remaining.get("a_existing").fields());
    Assertions.assertEquals(2L, remaining.get("test").value().value());
  }

  /** Verifies a late delete conflict restores statistics deleted earlier in the batch. */
  @TestTemplate
  public void testStatisticDeleteConflictRollsBackWholeBatch() throws Exception {
    TableEntity table = createBatchConflictTable("delete");
    StatisticEntity unchanged = createNamedStatistic("a_existing", 10L);
    statisticMetaService.writeStatisticsWithVersion(
        List.of(unchanged, createStatisticEntity(AUDIT_INFO, 1L)),
        table.nameIdentifier(),
        Entity.EntityType.TABLE);

    OptimisticLockException conflict =
        Assertions.assertThrows(
            OptimisticLockException.class,
            () ->
                updateAfterSnapshot(table, AUDIT_INFO, 2L)
                    .batchDeleteStatisticPOs(
                        table.nameIdentifier(),
                        Entity.EntityType.TABLE,
                        List.of("test", "a_existing")));
    assertStatisticConflict(conflict, table);
    Map<String, StatisticEntity> remaining = statisticsByName(table);
    Assertions.assertEquals(2, remaining.size());
    Assertions.assertEquals(unchanged.fields(), remaining.get("a_existing").fields());
    Assertions.assertEquals(2L, remaining.get("test").value().value());
  }

  /** Verifies a stale write or delete cannot affect a same-name replacement at version one. */
  @TestTemplate
  public void testRecreatedStatisticIsNotChangedByStaleWriteOrDelete() throws Exception {
    TableEntity table = createBatchConflictTable("recreated");
    statisticMetaService.writeStatisticsWithVersion(
        List.of(createStatisticEntity(AUDIT_INFO, 1L)),
        table.nameIdentifier(),
        Entity.EntityType.TABLE);
    for (boolean delete : List.of(false, true)) {
      StatisticEntity replacement = createStatisticEntity(AUDIT_INFO, 2L);
      StatisticMetaService staleService =
          new StatisticMetaService() {
            @Override
            List<StatisticPO> listStatisticPOs(NamespacedEntityId endpoint, List<String> names) {
              List<StatisticPO> rows = super.listStatisticPOs(endpoint, names);
              Assertions.assertEquals(1L, rows.get(0).getCurrentVersion());
              Assertions.assertEquals(
                  1,
                  statisticMetaService.batchDeleteStatisticPOs(
                      table.nameIdentifier(), Entity.EntityType.TABLE, names));
              statisticMetaService.writeStatisticsWithVersion(
                  List.of(replacement), table.nameIdentifier(), Entity.EntityType.TABLE);
              return rows;
            }
          };
      OptimisticLockException conflict =
          Assertions.assertThrows(
              OptimisticLockException.class,
              () -> {
                if (delete) {
                  staleService.batchDeleteStatisticPOs(
                      table.nameIdentifier(), Entity.EntityType.TABLE, List.of("test"));
                } else {
                  staleService.writeStatisticsWithVersion(
                      List.of(createStatisticEntity(AUDIT_INFO, 3L)),
                      table.nameIdentifier(),
                      Entity.EntityType.TABLE);
                }
              });
      assertStatisticConflict(conflict, table);
      Assertions.assertEquals(replacement.fields(), statisticsByName(table).get("test").fields());
    }
  }

  private TableEntity createBatchConflictTable(String suffix) throws Exception {
    String metalake = "batch_conflict_metalake_" + suffix;
    String catalog = "batch_conflict_catalog";
    String schema = "batch_conflict_schema";
    createParentEntities(metalake, catalog, schema, AUDIT_INFO);
    return createAndInsertTableEntity(Namespace.of(metalake, catalog, schema), "batch_conflict");
  }

  private StatisticEntity createNamedStatistic(String name, long value) {
    return TableStatisticEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withValue(StatisticValues.longValue(value))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private Map<String, StatisticEntity> statisticsByName(TableEntity table) {
    return statisticMetaService
        .listStatisticsByEntity(table.nameIdentifier(), Entity.EntityType.TABLE)
        .stream()
        .collect(Collectors.toMap(StatisticEntity::name, statistic -> statistic));
  }

  private void assertStatisticConflict(OptimisticLockException conflict, TableEntity table) {
    Assertions.assertTrue(conflict.getMessage().contains("test"));
    Assertions.assertTrue(conflict.getMessage().contains(table.nameIdentifier().toString()));
    Assertions.assertTrue(conflict.getMessage().contains("retry the operation"));
  }

  private static StatisticMetaService failingStatements(RuntimeException failure) {
    return new StatisticMetaService() {
      @Override
      int executeStatement(Function<StatisticMetaMapper, Integer> statement) {
        throw failure;
      }
    };
  }

  private StatisticMetaService updateAfterSnapshot(
      TableEntity table, AuditInfo auditInfo, long value) {
    return new StatisticMetaService() {
      @Override
      List<StatisticPO> listStatisticPOs(NamespacedEntityId endpoint, List<String> names) {
        List<StatisticPO> rows = super.listStatisticPOs(endpoint, names);
        statisticMetaService.writeStatisticsWithVersion(
            List.of(createStatisticEntity(auditInfo, value)),
            table.nameIdentifier(),
            Entity.EntityType.TABLE);
        return rows;
      }
    };
  }

  private Object singleStatisticValue(TableEntity table) {
    List<StatisticEntity> statistics =
        statisticMetaService.listStatisticsByEntity(
            table.nameIdentifier(), Entity.EntityType.TABLE);
    Assertions.assertEquals(1, statistics.size());
    return statistics.get(0).value().value();
  }

  private StatisticMetaService dropTableAfterSnapshot(TableEntity table) {
    return new StatisticMetaService() {
      @Override
      List<StatisticPO> listStatisticPOs(NamespacedEntityId endpoint, List<String> names) {
        List<StatisticPO> rows = super.listStatisticPOs(endpoint, names);
        Assertions.assertTrue(TableMetaService.getInstance().deleteTable(table.nameIdentifier()));
        return rows;
      }
    };
  }

  @TestTemplate
  public void testTableStatisticWriteWaitsForConcurrentSchemaDelete() throws Exception {
    String metalakeName = "metalake_for_statistic_schema_fence";
    String catalogName = "catalog_for_statistic_schema_fence";
    String schemaName = "schema_for_statistic_schema_fence";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    createParentEntities(metalakeName, catalogName, schemaName, auditInfo);
    SchemaPO observedSchemaPO = selectSchemaPO(metalakeName, catalogName, schemaName);

    assertTableStatisticWriteWaitsForConcurrentSchemaDelete(
        metalakeName,
        catalogName,
        schemaName,
        auditInfo,
        () -> {
          int deleted =
              SessionUtils.getWithoutCommit(
                  SchemaMetaMapper.class,
                  mapper ->
                      mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                          observedSchemaPO.getSchemaId(), observedSchemaPO.getCurrentVersion()));
          Assertions.assertEquals(1, deleted);
        });
  }

  @TestTemplate
  public void testNestedSchemaTableStatisticWriteWaitsForAncestorCascadeDelete() throws Exception {
    String metalakeName = "metalake_for_nested_statistic_fence";
    String catalogName = "catalog_for_nested_statistic_fence";
    String ancestorName = "fence_anc_a";
    String nestedName = ancestorName + ":fence_anc_b";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    // Inserting the nested leaf also creates the ancestor row.
    SchemaMetaService.getInstance()
        .insertSchema(
            createSchemaEntity(
                RandomIdGenerator.INSTANCE.nextId(),
                Namespace.of(metalakeName, catalogName),
                nestedName,
                auditInfo),
            false);
    SchemaPO observedAncestorPO = selectSchemaPO(metalakeName, catalogName, ancestorName);
    SchemaPO observedNestedPO = selectSchemaPO(metalakeName, catalogName, nestedName);

    // The statistic only locks its direct parent, the nested schema. A cascade delete of the
    // ancestor soft-deletes every descendant schema row in the same transaction, so the write
    // must still wait for it and then fail once the nested schema is gone.
    assertTableStatisticWriteWaitsForConcurrentSchemaDelete(
        metalakeName,
        catalogName,
        nestedName,
        auditInfo,
        () -> {
          int deletedAncestor =
              SessionUtils.getWithoutCommit(
                  SchemaMetaMapper.class,
                  mapper ->
                      mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                          observedAncestorPO.getSchemaId(),
                          observedAncestorPO.getCurrentVersion()));
          Assertions.assertEquals(1, deletedAncestor);
          int deletedDescendants =
              SessionUtils.getWithoutCommit(
                  SchemaMetaMapper.class,
                  mapper -> mapper.softDeleteSchemaMetasWithVersion(List.of(observedNestedPO)));
          Assertions.assertEquals(1, deletedDescendants);
        });
  }

  private SchemaPO selectSchemaPO(String metalakeName, String catalogName, String schemaName) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(metalakeName, catalogName, schemaName), Entity.EntityType.SCHEMA);
    return SessionUtils.getWithoutCommit(
        SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(schemaId));
  }

  /**
   * Runs {@code schemaDeleteStep} in an uncommitted transaction, then verifies that a statistic
   * upsert on a table below {@code schemaName} blocks until that transaction commits and fails with
   * {@link NoSuchEntityException} afterwards.
   */
  private void assertTableStatisticWriteWaitsForConcurrentSchemaDelete(
      String metalakeName,
      String catalogName,
      String schemaName,
      AuditInfo auditInfo,
      Runnable schemaDeleteStep)
      throws Exception {
    Long metalakeId =
        EntityIdService.getEntityId(NameIdentifier.of(metalakeName), Entity.EntityType.METALAKE);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalogName, schemaName),
            "table",
            auditInfo);
    backend.insert(table, false);
    StatisticEntity statistic =
        TableStatisticEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("test")
            .withNamespace(Namespace.of(metalakeName, catalogName, schemaName, table.name()))
            .withValue(StatisticValues.longValue(100L))
            .withAuditInfo(auditInfo)
            .build();

    CountDownLatch schemaDeleteLocked = new CountDownLatch(1);
    CountDownLatch allowDeleteCommit = new CountDownLatch(1);
    CountDownLatch statisticWriteStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> deleteResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () -> {
                      schemaDeleteStep.run();
                      schemaDeleteLocked.countDown();
                      try {
                        Assertions.assertTrue(allowDeleteCommit.await(30, TimeUnit.SECONDS));
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                      }
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });
    try {
      Assertions.assertTrue(schemaDeleteLocked.await(30, TimeUnit.SECONDS));
      Future<Throwable> statisticWriteResult =
          executor.submit(
              () -> {
                statisticWriteStarted.countDown();
                try {
                  backend.batchPut(List.of(statistic), true);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      Assertions.assertTrue(statisticWriteStarted.await(30, TimeUnit.SECONDS));
      Assertions.assertThrows(
          TimeoutException.class, () -> statisticWriteResult.get(500, TimeUnit.MILLISECONDS));

      allowDeleteCommit.countDown();
      Assertions.assertNull(deleteResult.get(30, TimeUnit.SECONDS));
      Assertions.assertInstanceOf(
          NoSuchEntityException.class, statisticWriteResult.get(30, TimeUnit.SECONDS));
    } finally {
      allowDeleteCommit.countDown();
      executor.shutdownNow();
    }

    Assertions.assertEquals(0, countActiveStats(metalakeId));
  }

  @TestTemplate
  public void testStatisticsLifeCycle() throws Exception {
    String metalakeName = "metalake";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);

    List<StatisticEntity> statisticEntities = Lists.newArrayList();
    StatisticEntity statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);

    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, table.nameIdentifier(), Entity.EntityType.TABLE);

    List<StatisticEntity> listEntities =
        statisticMetaService.listStatisticsByEntity(
            table.nameIdentifier(), Entity.EntityType.TABLE);
    Assertions.assertEquals(1, listEntities.size());
    Assertions.assertEquals("test", listEntities.get(0).name());
    Assertions.assertEquals(100L, listEntities.get(0).value().value());

    // Update the duplicated key
    statisticEntity = createStatisticEntity(auditInfo, 200L);
    statisticEntities.clear();
    statisticEntities.add(statisticEntity);
    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, table.nameIdentifier(), Entity.EntityType.TABLE);

    listEntities =
        statisticMetaService.listStatisticsByEntity(
            table.nameIdentifier(), Entity.EntityType.TABLE);
    Assertions.assertEquals(1, listEntities.size());
    Assertions.assertEquals("test", listEntities.get(0).name());
    Assertions.assertEquals(200L, listEntities.get(0).value().value());

    List<String> names = Lists.newArrayList(statisticEntity.name());
    statisticMetaService.batchDeleteStatisticPOs(table.nameIdentifier(), table.type(), names);
    listEntities =
        statisticMetaService.listStatisticsByEntity(table.nameIdentifier(), table.type());
    Assertions.assertEquals(0, listEntities.size());
  }

  @TestTemplate
  public void testDeleteMetadataObject() throws Exception {
    String metalakeName = "metalake";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    backend.insert(topic, false);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "model",
            "comment",
            1,
            null,
            auditInfo);
    backend.insert(model, false);

    // insert stats
    List<StatisticEntity> statisticEntities = Lists.newArrayList();
    StatisticEntity statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, table.nameIdentifier(), Entity.EntityType.TABLE);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, topic.nameIdentifier(), Entity.EntityType.TOPIC);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, fileset.nameIdentifier(), Entity.EntityType.FILESET);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, model.nameIdentifier(), Entity.EntityType.MODEL);

    // assert stats
    Assertions.assertEquals(4, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete model
    ModelMetaService.getInstance().deleteModel(model.nameIdentifier());

    // assert stats
    Assertions.assertEquals(3, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete table
    TableMetaService.getInstance().deleteTable(table.nameIdentifier());
    // assert stats
    Assertions.assertEquals(2, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete topic
    TopicMetaService.getInstance().deleteTopic(topic.nameIdentifier());
    // assert stats
    Assertions.assertEquals(1, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete fileset
    FilesetMetaService.getInstance().deleteFileset(fileset.nameIdentifier());
    // assert stats
    Assertions.assertEquals(0, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete schema
    SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), false);
    // assert stats
    Assertions.assertEquals(0, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete catalog
    CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), false);
    // assert stats
    Assertions.assertEquals(0, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete catalog with cascade mode
    catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);
    table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);

    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    backend.insert(topic, false);

    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "model",
            "comment",
            1,
            null,
            auditInfo);
    backend.insert(model, false);
    // insert stats
    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, table.nameIdentifier(), Entity.EntityType.TABLE);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, topic.nameIdentifier(), Entity.EntityType.TOPIC);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, fileset.nameIdentifier(), Entity.EntityType.FILESET);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, model.nameIdentifier(), Entity.EntityType.MODEL);

    // assert stats
    Assertions.assertEquals(4, countActiveStats(metalake.id()));
    Assertions.assertEquals(8, countAllStats(metalake.id()));

    CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), true);

    // assert stats
    Assertions.assertEquals(0, countActiveStats(metalake.id()));
    Assertions.assertEquals(8, countAllStats(metalake.id()));

    // Test to delete schema with cascade mode
    catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);
    table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);
    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    backend.insert(topic, false);
    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "model",
            "comment",
            1,
            null,
            auditInfo);
    backend.insert(model, false);

    // insert stats
    statisticEntities = Lists.newArrayList();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, table.nameIdentifier(), Entity.EntityType.TABLE);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, topic.nameIdentifier(), Entity.EntityType.TOPIC);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, fileset.nameIdentifier(), Entity.EntityType.FILESET);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.writeStatisticsWithVersion(
        statisticEntities, model.nameIdentifier(), Entity.EntityType.MODEL);

    // assert stats count
    Assertions.assertEquals(4, countActiveStats(metalake.id()));
    Assertions.assertEquals(12, countAllStats(metalake.id()));

    // delete object
    SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), true);

    // assert stats count
    Assertions.assertEquals(0, countActiveStats(metalake.id()));
    Assertions.assertEquals(12, countAllStats(metalake.id()));
  }

  private StatisticEntity createStatisticEntity(AuditInfo auditInfo, long value) {
    return TableStatisticEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName("test")
        .withValue(StatisticValues.longValue(value))
        .withAuditInfo(auditInfo)
        .build();
  }

  private Integer countAllStats(Long metalakeId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM statistic_meta WHERE metalake_id = %d", metalakeId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  private Integer countActiveStats(Long metalakeId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM statistic_meta WHERE metalake_id = %d AND deleted_at = 0",
                    metalakeId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }
}
