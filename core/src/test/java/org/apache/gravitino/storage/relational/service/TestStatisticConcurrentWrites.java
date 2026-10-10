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
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
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

/** Concurrent table-level statistic writes, which no longer take the table tree lock. */
public class TestStatisticConcurrentWrites extends TestJDBCBackend {

  private static final List<String> NAMES = ImmutableList.of("s0", "s1", "s2", "s3", "s4");

  @TestTemplate
  public void testOverlappingUpsertsInOppositeOrderDoNotDeadlock() throws Exception {
    raiseDefaultLockTimeout();
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    createParentEntities(
        "metalake_statistic_race", "catalog_statistic_race", "schema_statistic_race", auditInfo);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(
                "metalake_statistic_race", "catalog_statistic_race", "schema_statistic_race"),
            "table_statistic_race",
            auditInfo);
    backend.insert(table, false);

    for (int round = 0; round < 10; round++) {
      int base = round * 100;
      AtomicInteger writer = new AtomicInteger();
      List<Object> outcomes =
          RaceTestUtils.runTogether(
              8,
              () -> {
                int index = writer.getAndIncrement();
                // Half of the writers send the names in reverse order, which is the order that
                // deadlocks when the rows are not locked in a stable order.
                List<String> names = index % 2 == 0 ? NAMES : Lists.reverse(NAMES);
                List<StatisticEntity> statistics =
                    names.stream()
                        .map(name -> statistic(name, base + index, auditInfo))
                        .collect(Collectors.toList());
                StatisticMetaService.getInstance()
                    .batchInsertStatisticPOsOnDuplicateKeyUpdate(
                        statistics, table.nameIdentifier(), Entity.EntityType.TABLE);
                return null;
              });

      outcomes.forEach(Assertions::assertNull);
      List<StatisticEntity> stored =
          StatisticMetaService.getInstance()
              .listStatisticsByEntity(table.nameIdentifier(), Entity.EntityType.TABLE);
      Assertions.assertEquals(
          NAMES, stored.stream().map(StatisticEntity::name).sorted().collect(Collectors.toList()));
    }
  }

  private static StatisticEntity statistic(String name, long value, AuditInfo auditInfo) {
    return TableStatisticEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withValue(StatisticValues.longValue(value))
        .withAuditInfo(auditInfo)
        .build();
  }

  /**
   * H2 gives new sessions a short lock timeout, so writers that queue behind each other would fail
   * on a slow machine. The other backends wait long enough by default.
   */
  private void raiseDefaultLockTimeout() throws SQLException {
    if (!"h2".equals(backendType)) {
      return;
    }
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Statement statement = session.getConnection().createStatement()) {
      statement.execute("SET DEFAULT_LOCK_TIMEOUT 30000");
    }
  }
}
