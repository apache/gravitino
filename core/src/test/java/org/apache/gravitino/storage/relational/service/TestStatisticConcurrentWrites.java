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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.StatisticEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TableStatisticEntity;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.RaceTestUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/**
 * Concurrent table-level statistic writes, which do not take the table tree lock. Every outcome
 * must be a success or a conflict the caller can retry; a deadlock must never surface as an
 * internal error, and no statistic may outlive its table.
 */
public class TestStatisticConcurrentWrites extends TestJDBCBackend {

  private static final String METALAKE = "metalake_statistic_race";
  private static final String CATALOG = "catalog_statistic_race";
  private static final String SCHEMA = "schema_statistic_race";
  private static final List<String> NAMES = ImmutableList.of("s0", "s1", "s2", "s3", "s4");
  private static final int ROUNDS = 20;

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

  @TestTemplate
  public void testOverlappingUpsertsInOppositeOrderSucceedOrConflict() throws Exception {
    raiseDefaultLockTimeout();
    createParentEntities(METALAKE, CATALOG, SCHEMA, auditInfo);
    TableEntity table = createTable("table_upsert_race");

    for (int round = 0; round < ROUNDS; round++) {
      AtomicInteger writer = new AtomicInteger();
      long base = round * 100L;
      List<Object> outcomes =
          RaceTestUtils.runTogether(
              8,
              () -> {
                int index = writer.getAndIncrement();
                upsert(table, index % 2 == 0 ? NAMES : Lists.reverse(NAMES), base + index);
                return null;
              });

      // The version check lets one writer of a statistic win and reports the others as conflicts.
      // Locking the rows in name order keeps opposite request orders from deadlocking.
      Assertions.assertTrue(outcomes.contains(null), "One writer must win: " + outcomes);
      for (Object outcome : outcomes) {
        Assertions.assertTrue(
            outcome == null || outcome instanceof OptimisticLockException,
            "Unexpected outcome: " + outcome);
      }
      Assertions.assertEquals(NAMES, liveNames(table));
    }
  }

  @TestTemplate
  public void testUpsertsRacingStatisticDropsSucceedOrConflict() throws Exception {
    raiseDefaultLockTimeout();
    createParentEntities(METALAKE, CATALOG, SCHEMA, auditInfo);
    TableEntity table = createTable("table_drop_race");

    for (int round = 0; round < ROUNDS; round++) {
      upsert(table, NAMES, round);
      AtomicInteger worker = new AtomicInteger();
      long value = round * 100L;
      List<Object> outcomes =
          RaceTestUtils.runTogether(
              8,
              () -> {
                int index = worker.getAndIncrement();
                if (index % 2 == 0) {
                  upsert(table, Lists.reverse(NAMES), value + index);
                  return null;
                }
                return StatisticMetaService.getInstance()
                    .batchDeleteStatisticPOs(
                        table.nameIdentifier(), Entity.EntityType.TABLE, NAMES);
              });

      for (Object outcome : outcomes) {
        Assertions.assertTrue(
            outcome == null
                || outcome instanceof Integer
                || outcome instanceof OptimisticLockException,
            "Unexpected outcome: " + outcome);
      }
      // Each upsert and each drop is all-or-nothing, so a round ends with every name or none.
      List<String> live = liveNames(table);
      Assertions.assertTrue(live.isEmpty() || live.equals(NAMES), "Partial batch: " + live);
    }
  }

  @TestTemplate
  public void testUpsertsRacingTableDropLeaveNoStatistics() throws Exception {
    raiseDefaultLockTimeout();
    createParentEntities(METALAKE, CATALOG, SCHEMA, auditInfo);

    for (int round = 0; round < ROUNDS; round++) {
      TableEntity table = createTable("table_cascade_race_" + round);
      // Insert in reverse name order, so the row ids of the cascade delete and the names of the
      // upserts order the statistic rows differently.
      for (String name : Lists.reverse(NAMES)) {
        upsert(table, ImmutableList.of(name), 0);
      }
      AtomicInteger worker = new AtomicInteger();
      List<Object> outcomes =
          RaceTestUtils.runTogether(
              8,
              (Callable<Object>)
                  () -> {
                    if (worker.getAndIncrement() == 0) {
                      return TableMetaService.getInstance().deleteTable(table.nameIdentifier());
                    }
                    upsert(table, NAMES, 1);
                    return null;
                  });

      Assertions.assertTrue(
          outcomes.contains(Boolean.TRUE), "The table drop must win: " + outcomes);
      for (Object outcome : outcomes) {
        Assertions.assertTrue(
            outcome == null
                || Boolean.TRUE.equals(outcome)
                || outcome instanceof NoSuchEntityException
                || outcome instanceof OptimisticLockException,
            "Unexpected outcome: " + outcome);
        // The drop and the writers queue on the table row, so a conflict here is a version check
        // that lost, never a deadlock mapped to a conflict.
        Assertions.assertFalse(isDeadlock(outcome), "Deadlock between drop and writer: " + outcome);
      }
      Assertions.assertEquals(0, countLiveStatistics(table.id()));
    }
  }

  private static boolean isDeadlock(Object outcome) {
    for (Throwable cause = outcome instanceof Throwable ? (Throwable) outcome : null;
        cause != null;
        cause = cause.getCause()) {
      if (cause instanceof SQLException) {
        SQLException sql = (SQLException) cause;
        if ("40001".equals(sql.getSQLState())
            || "40P01".equals(sql.getSQLState())
            || sql.getErrorCode() == 1213) {
          return true;
        }
      }
    }
    return false;
  }

  private TableEntity createTable(String name) throws Exception {
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE, CATALOG, SCHEMA),
            name,
            auditInfo);
    backend.insert(table, false);
    return table;
  }

  private void upsert(TableEntity table, List<String> names, long value) {
    List<StatisticEntity> statistics =
        names.stream()
            .map(
                name ->
                    (StatisticEntity)
                        TableStatisticEntity.builder()
                            .withId(RandomIdGenerator.INSTANCE.nextId())
                            .withName(name)
                            .withValue(StatisticValues.longValue(value))
                            .withAuditInfo(auditInfo)
                            .build())
            .collect(Collectors.toList());
    StatisticMetaService.getInstance()
        .writeStatisticsWithVersion(statistics, table.nameIdentifier(), Entity.EntityType.TABLE);
  }

  private List<String> liveNames(TableEntity table) {
    return StatisticMetaService.getInstance()
        .listStatisticsByEntity(table.nameIdentifier(), Entity.EntityType.TABLE)
        .stream()
        .map(StatisticEntity::name)
        .sorted()
        .collect(Collectors.toList());
  }

  private int countLiveStatistics(long tableId) throws SQLException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Statement statement = session.getConnection().createStatement();
        ResultSet rs =
            statement.executeQuery(
                "SELECT COUNT(*) FROM statistic_meta WHERE metadata_object_id = "
                    + tableId
                    + " AND deleted_at = 0")) {
      rs.next();
      return rs.getInt(1);
    }
  }

  /**
   * H2 gives new sessions a short lock timeout, so writers that queue behind each other would fail
   * on a slow machine. The other backends wait long enough by default. The database default only
   * reaches sessions opened later, so the sessions already idle in the pool are raised one by one:
   * they are all borrowed at once, which hands out every idle connection exactly once.
   */
  private void raiseDefaultLockTimeout() throws SQLException {
    if (!"h2".equals(backendType)) {
      return;
    }
    BasicDataSource dataSource =
        (BasicDataSource)
            SqlSessionFactoryHelper.getInstance()
                .getSqlSessionFactory()
                .getConfiguration()
                .getEnvironment()
                .getDataSource();
    List<Connection> idle = new ArrayList<>();
    try {
      for (int i = Math.max(1, dataSource.getNumIdle()); i > 0; i--) {
        idle.add(dataSource.getConnection());
      }
      for (Connection connection : idle) {
        try (Statement statement = connection.createStatement()) {
          statement.execute("SET DEFAULT_LOCK_TIMEOUT 30000");
          statement.execute("SET LOCK_TIMEOUT 30000");
        }
      }
    } finally {
      for (Connection connection : idle) {
        connection.close();
      }
    }
  }
}
