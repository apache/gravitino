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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermBindingMapper;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService.TargetType;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/**
 * Exercises {@code glossary_term_binding} storage constraints and lifecycle across H2, MySQL and
 * PostgreSQL. Covers T11.12.
 */
public class TestGlossaryTermBindingService extends TestJDBCBackend {
  private static final String LAKE = "glossary";
  private static final long LAKE_ID = 100L;
  private static final Namespace NS =
      Namespace.of(LAKE, Entity.SYSTEM_CATALOG_RESERVED_NAME, "glossary_term");
  private final GlossaryTermBindingService service = GlossaryTermBindingService.getInstance();

  @AfterEach
  public void checkTransactionClean() {
    Assertions.assertFalse(SessionUtils.isInTransaction());
  }

  /**
   * Covers T11.12 (guard): duplicate live keys fail for TABLE, COLUMN and METRIC; different metric
   * IDs on the same model work; NULL in any key column fails; soft-deleted bindings do not block a
   * new live binding.
   */
  @TestTemplate
  public void testUniqueKeyNonNullAndSoftDeleteLifecycle() throws Exception {
    setup();
    for (TargetType type : List.of(TargetType.TABLE, TargetType.COLUMN, TargetType.METRIC)) {
      long targetId = 500L + type.ordinal();
      run(() -> service.add(LAKE_ID, 101L, type, targetId));
      Assertions.assertThrows(
          RuntimeException.class, () -> run(() -> service.add(LAKE_ID, 101L, type, targetId)));
      Assertions.assertEquals(
          Set.of(101L), read(() -> service.listTermIds(LAKE_ID, type, targetId)));

      // Soft-deleting allows a new live binding for the same (term_id, target_type, target_id).
      Assertions.assertTrue(read(() -> service.remove(101L, type, targetId)));
      Assertions.assertTrue(read(() -> service.listTermIds(LAKE_ID, type, targetId)).isEmpty());
      run(() -> service.add(LAKE_ID, 101L, type, targetId));
      Assertions.assertEquals(
          Set.of(101L), read(() -> service.listTermIds(LAKE_ID, type, targetId)));
    }

    // Different metric IDs on the same semantic model can both be bound to the same term.
    run(() -> service.add(LAKE_ID, 101L, TargetType.METRIC, 9001L));
    run(() -> service.add(LAKE_ID, 101L, TargetType.METRIC, 9002L));
    Assertions.assertEquals(
        Set.of(101L), read(() -> service.listTermIds(LAKE_ID, TargetType.METRIC, 9001L)));
    Assertions.assertEquals(
        Set.of(101L), read(() -> service.listTermIds(LAKE_ID, TargetType.METRIC, 9002L)));

    // NULL in any key column is rejected by the database schema.
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection conn = session.getConnection();
        Statement stmt = conn.createStatement()) {
      Assertions.assertThrows(
          SQLException.class,
          () ->
              stmt.executeUpdate(
                  "INSERT INTO glossary_term_binding (metalake_id, term_id, target_type, target_id, deleted_at) "
                      + "VALUES (100, NULL, 'TABLE', 701, 0)"));
      Assertions.assertThrows(
          SQLException.class,
          () ->
              stmt.executeUpdate(
                  "INSERT INTO glossary_term_binding (metalake_id, term_id, target_type, target_id, deleted_at) "
                      + "VALUES (100, 101, NULL, 701, 0)"));
      Assertions.assertThrows(
          SQLException.class,
          () ->
              stmt.executeUpdate(
                  "INSERT INTO glossary_term_binding (metalake_id, term_id, target_type, target_id, deleted_at) "
                      + "VALUES (100, 101, 'TABLE', NULL, 0)"));
      Assertions.assertThrows(
          SQLException.class,
          () ->
              stmt.executeUpdate(
                  "INSERT INTO glossary_term_binding (metalake_id, term_id, target_type, target_id, deleted_at) "
                      + "VALUES (100, 101, 'TABLE', 701, NULL)"));
    }
  }

  /** Covers T11.12: repeated removals advance deleted_at and GC purges expired tombstones. */
  @TestTemplate
  public void testRepeatedRemovalTermDeleteAndGc() throws Exception {
    setup();
    long retained = Long.MAX_VALUE - 5;
    run(() -> service.add(LAKE_ID, 101L, TargetType.TABLE, 801L));
    SessionUtils.doWithCommit(
        GlossaryTermBindingMapper.class,
        mapper -> mapper.remove(101L, TargetType.TABLE.name(), 801L, retained));
    for (int i = 1; i <= 2; i++) {
      run(() -> service.add(LAKE_ID, 101L, TargetType.TABLE, 801L));
      Assertions.assertTrue(read(() -> service.remove(101L, TargetType.TABLE, 801L)));
      Assertions.assertEquals(
          retained + i, scalar("SELECT MAX(deleted_at) FROM glossary_term_binding"));
    }
    run(() -> service.add(LAKE_ID, 101L, TargetType.TABLE, 802L));
    run(() -> service.add(LAKE_ID, 102L, TargetType.TABLE, 802L));
    Assertions.assertTrue(
        GlossaryTermMetaService.getInstance().delete(NameIdentifier.of(NS, "Term101")));
    Assertions.assertEquals(
        Set.of(102L), read(() -> service.listTermIds(LAKE_ID, TargetType.TABLE, 802L)));

    run(() -> GlossaryTermMetaService.getInstance().dropMetalake(LAKE_ID));
    Assertions.assertEquals(
        0, scalar("SELECT COUNT(*) FROM glossary_term_binding WHERE deleted_at = 0"));
    GlossaryTermMetaService.getInstance().collect(Long.MAX_VALUE, 1000);
    Assertions.assertEquals(0, scalar("SELECT COUNT(*) FROM glossary_term_binding"));
  }

  private void setup() throws Exception {
    backend.insert(createBaseMakeLake(LAKE_ID, LAKE, AUDIT_INFO), false);
    for (long id = 101; id <= 102; id++) {
      backend.insert(
          GlossaryTermEntity.builder()
              .withId(id)
              .withName("Term" + id)
              .withNamespace(NS)
              .withAuditInfo(AUDIT_INFO)
              .build(),
          false);
    }
  }

  private static void run(Runnable work) {
    SessionUtils.doMultipleWithCommit(work);
  }

  private static <T> T read(Supplier<T> work) {
    return SessionUtils.doWithCommitAndFetchResult(
        GlossaryTermBindingMapper.class, ignored -> work.get());
  }

  private static long scalar(String sql) throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        Connection conn = session.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rows = stmt.executeQuery(sql)) {
      Assertions.assertTrue(rows.next());
      return rows.getLong(1);
    }
  }
}
