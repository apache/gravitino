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
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermRelMapper;
import org.apache.gravitino.storage.relational.po.GlossaryTermRelPO;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService.RelationType;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.function.Executable;
import org.mockito.AdditionalAnswers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Exercises glossary edges and mutexes with real backend transactions. */
public class TestGlossaryTermRelService extends TestJDBCBackend {
  private static final Namespace NS =
      Namespace.of("glossary", Entity.SYSTEM_CATALOG_RESERVED_NAME, "glossary_term");
  private final GlossaryTermRelService service = GlossaryTermRelService.getInstance();

  /** Checks the T3 endpoint, canonical-pair and transaction-lifecycle invariants. */
  @AfterEach
  public void checkInvariants() throws Exception {
    Assertions.assertFalse(
        SessionUtils.isInTransaction(), "Storage operations must close owned transactions");
    GlossaryInvariants.assertDefinitions();
    GlossaryInvariants.assertRelations();
  }

  /** T3.1/T3.2: relation directions, canonical RELATED pairs and idempotent mutations. */
  @TestTemplate
  public void testRelationsAndCanonicalPairs() throws Exception {
    setupTerms(4);
    Assertions.assertTrue(service.add(100, 101, 102, RelationType.BROADER));
    Assertions.assertFalse(service.add(100, 101, 102, RelationType.BROADER));
    Assertions.assertTrue(service.add(100, 103, 101, RelationType.RELATED));
    Assertions.assertFalse(service.add(100, 101, 103, RelationType.RELATED));
    Assertions.assertEquals(Set.of(102L), service.listBroader(100, 101));
    Assertions.assertEquals(Set.of(101L), service.listNarrower(100, 102));
    Assertions.assertEquals(Set.of(103L), service.listRelated(100, 101));
    Assertions.assertEquals(Set.of(101L), service.listRelated(100, 103));
    Assertions.assertEquals(
        1,
        scalar(
            "SELECT COUNT(*) FROM glossary_term_rel WHERE rel_type='RELATED' AND from_term_id=101 AND to_term_id=103 AND deleted_at=0"));
    Assertions.assertTrue(service.remove(100, 103, 101, RelationType.RELATED));
    Assertions.assertFalse(service.remove(100, 101, 103, RelationType.RELATED));
    Assertions.assertTrue(service.listRelated(100, 101).isEmpty());
    Assertions.assertTrue(service.remove(100, 101, 102, RelationType.BROADER));
    Assertions.assertTrue(service.listNarrower(100, 102).isEmpty());
    Assertions.assertTrue(service.listBroader(100, 104).isEmpty());
  }

  /** A RELATED edge touched by two frontier batches is returned exactly once. */
  @TestTemplate
  public void testRelatedEdgesAcrossFrontierBatches() throws Exception {
    setupTerms(4);
    service.add(100, 101, 102, RelationType.RELATED);
    service.add(100, 103, 101, RelationType.RELATED);
    service.add(100, 104, 102, RelationType.RELATED);
    Set<Long> frontier = new LinkedHashSet<>();
    frontier.add(102L);
    for (long unused = 1000; unused < 1499; unused++) {
      frontier.add(unused);
    }
    frontier.add(101L);
    List<GlossaryTermRelPO> edges = service.listRelatedEdges(100, frontier);
    Assertions.assertEquals(
        List.of(101L, 101L, 102L), edges.stream().map(GlossaryTermRelPO::getFromTermId).toList());
    Assertions.assertEquals(
        List.of(102L, 103L, 104L), edges.stream().map(GlossaryTermRelPO::getToTermId).toList());
  }

  /** T3.1: rejects invalid input and missing or cross-metalake endpoints without writes. */
  @TestTemplate
  public void testRejectInvalidEndpoints() throws Exception {
    setupTerms(2);
    backend.insert(createBaseMakeLake(200L, "other", AUDIT_INFO), false);
    backend.insert(
        term(
            201,
            "Other",
            Namespace.of("other", Entity.SYSTEM_CATALOG_RESERVED_NAME, "glossary_term")),
        false);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> service.add(100, 101, 101, RelationType.BROADER));
    Assertions.assertThrows(IllegalArgumentException.class, () -> service.add(100, 101, 102, null));
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> service.add(100, 101, 999, RelationType.BROADER));
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> service.add(100, 101, 201, RelationType.RELATED));
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> service.withHierarchyLock(999, () -> null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> service.withHierarchyLock(100, null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> service.listBroaderEdges(100, null));
    Set<Long> invalid = new HashSet<>();
    invalid.add(null);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> service.listBroaderEdges(100, invalid));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            service.withHierarchyLock(
                100,
                Set.of(101L, 201L),
                () -> Assertions.fail("Missing endpoint must prevent callback")));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> service.collect(Long.MAX_VALUE, -1));
    Assertions.assertEquals(0, scalar("SELECT COUNT(*) FROM glossary_term_rel"));
    Assertions.assertEquals(
        0, scalar("SELECT COUNT(*) FROM glossary_hierarchy_lock WHERE metalake_id=999"));
  }

  /** T3.3/T3.4: deduplicates diamonds and guards one real mapper query per frontier. */
  @TestTemplate
  public void testBatchedWalkAndDiamond() throws Exception {
    setupTerms(8);
    // Five frontiers: {101}, {102,103}, {104,105}, {106,107}, {108}.
    for (long[] pair :
        new long[][] {
          {101, 102},
          {101, 103},
          {102, 104},
          {103, 104},
          {103, 105},
          {104, 106},
          {105, 107},
          {106, 108},
          {107, 108}
        }) {
      service.add(100, pair[0], pair[1], RelationType.BROADER);
    }
    AtomicInteger queries = new AtomicInteger();
    // A default answer avoids invoking getMapper while configuring a static stub: that would
    // increment the thread-local session count and leak a transaction into the next test.
    try (MockedStatic<SqlSessions> sessions =
        Mockito.mockStatic(
            SqlSessions.class,
            call -> {
              Object result = call.callRealMethod();
              if (!call.getMethod().getName().equals("getMapper")
                  || call.getArgument(0) != GlossaryTermRelMapper.class) {
                return result;
              }
              return Mockito.mock(
                  GlossaryTermRelMapper.class,
                  invocation -> {
                    if (invocation.getMethod().getName().equals("listEdges")) {
                      queries.incrementAndGet();
                    }
                    return AdditionalAnswers.delegatesTo(result).answer(invocation);
                  });
            })) {
      Assertions.assertEquals(
          Set.of(102L, 103L, 104L, 105L, 106L, 107L, 108L), service.ancestors(100, 101));
      Assertions.assertEquals(5, queries.get(), "One mapper query per frontier, not per term");
      queries.set(0);
      Assertions.assertEquals(
          Set.of(101L, 102L, 103L, 104L, 105L, 106L, 107L), service.descendants(100, 108));
      Assertions.assertEquals(5, queries.get());
      Set<Long> wide = new LinkedHashSet<>();
      wide.add(107L);
      for (long id = 1000; id < 1499; id++) {
        wide.add(id);
      }
      wide.add(101L);
      queries.set(0);
      Assertions.assertEquals(
          List.of(101L, 101L, 107L),
          service.listBroaderEdges(100, wide).stream()
              .map(GlossaryTermRelPO::getFromTermId)
              .toList());
      Assertions.assertEquals(2, queries.get());
      queries.set(0);
      Assertions.assertEquals(
          List.of(105L),
          service.listNarrowerEdges(100, wide).stream()
              .map(GlossaryTermRelPO::getFromTermId)
              .toList());
      Assertions.assertEquals(2, queries.get());
      queries.set(0);
      Assertions.assertTrue(service.listBroaderEdges(100, Set.of()).isEmpty());
      Assertions.assertTrue(service.listNarrowerEdges(100, Set.of()).isEmpty());
      Assertions.assertEquals(0, queries.get());
    }
    // T5 rejects cycles. The storage walk must still terminate on malformed graphs.
    service.add(100, 108, 101, RelationType.BROADER);
    Assertions.assertEquals(7, service.ancestors(100, 101).size());
    service.remove(100, 108, 101, RelationType.BROADER);
  }

  /** T3.5: transaction ownership and fresh reads after acquiring the hierarchy mutex. */
  @TestTemplate
  public void testRollbackAndFreshReadsUnderMutex() throws Exception {
    repeatRace(
        () -> {
          setupTerms(3);
          Assertions.assertThrows(
              IllegalStateException.class,
              () ->
                  service.withHierarchyLock(
                      100,
                      () -> {
                        service.add(100, 101, 102, RelationType.BROADER);
                        throw new IllegalStateException("rollback graph");
                      }));
          Assertions.assertTrue(service.listBroader(100, 101).isEmpty());
          Assertions.assertEquals(0, scalar("SELECT COUNT(*) FROM glossary_hierarchy_lock"));
          SessionUtils.beginTransaction();
          try {
            Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                    service.withHierarchyLock(
                        100,
                        () -> {
                          service.add(100, 101, 102, RelationType.BROADER);
                          throw new IllegalStateException("nested callback failed");
                        }));
          } finally {
            // SessionUtils has no savepoints. A caller catching a nested failure owns the rollback.
            SessionUtils.rollbackTransaction();
          }
          Assertions.assertTrue(service.listBroader(100, 101).isEmpty());
          ExecutorService writer = Executors.newSingleThreadExecutor();
          try {
            SessionUtils.beginTransaction();
            try {
              Assertions.assertTrue(service.listBroader(100, 101).isEmpty());
              writer
                  .submit(() -> service.add(100, 101, 102, RelationType.BROADER))
                  .get(10, TimeUnit.SECONDS);
              service.withHierarchyLock(
                  100,
                  () -> {
                    Assertions.assertEquals(Set.of(102L), service.listBroader(100, 101));
                    return null;
                  });
              SessionUtils.commitTransaction();
            } finally {
              if (SessionUtils.isInTransaction()) SessionUtils.rollbackTransaction();
            }
            Assertions.assertEquals(1, scalar("SELECT COUNT(*) FROM glossary_hierarchy_lock"));
            service.withHierarchyLock(100, () -> null);
            Assertions.assertEquals(1, scalar("SELECT COUNT(*) FROM glossary_hierarchy_lock"));
          } finally {
            writer.shutdownNow();
            Assertions.assertTrue(writer.awaitTermination(10, TimeUnit.SECONDS));
          }
        });
  }

  /** A pre-mutex read sees later commits when checking duplicate edges and retiring a term. */
  @TestTemplate
  public void testPeerReplacementBeforeHierarchyLock() throws Exception {
    setupTerms(2);
    long future = 4102444800000L;
    execute("INSERT INTO glossary_term_rel VALUES (100,101,102,'BROADER'," + future + ")");
    ExecutorService writer = Executors.newSingleThreadExecutor();
    try {
      SessionUtils.beginTransaction();
      try {
        Assertions.assertTrue(service.listBroader(100, 101).isEmpty());
        writer
            .submit(
                () -> {
                  Assertions.assertTrue(service.add(100, 101, 102, RelationType.BROADER));
                  Assertions.assertTrue(service.remove(100, 101, 102, RelationType.BROADER));
                  Assertions.assertTrue(service.add(100, 101, 102, RelationType.BROADER));
                })
            .get(10, TimeUnit.SECONDS);
        Assertions.assertFalse(service.add(100, 101, 102, RelationType.BROADER));
        Assertions.assertTrue(
            GlossaryTermMetaService.getInstance().delete(NameIdentifier.of(NS, "Term102")));
        SessionUtils.commitTransaction();
      } finally {
        if (SessionUtils.isInTransaction()) SessionUtils.rollbackTransaction();
      }
      Assertions.assertEquals(future + 2, scalar("SELECT MAX(deleted_at) FROM glossary_term_rel"));
    } finally {
      writer.shutdownNow();
      Assertions.assertTrue(writer.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  /** T3.5: two independent transactions concurrently initialize the same mutex, 20 rounds. */
  @TestTemplate
  public void testConcurrentFirstUse() throws Exception {
    repeatRace(
        () -> {
          setupTerms(3);
          CyclicBarrier ready = new CyclicBarrier(2);
          AtomicInteger inside = new AtomicInteger();
          ExecutorService writers = Executors.newFixedThreadPool(2);
          try {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
              final long to = 102L + i;
              results.add(
                  writers.submit(
                      () -> {
                        ready.await(10, TimeUnit.SECONDS);
                        return service.withHierarchyLock(
                            100,
                            () -> {
                              Assertions.assertEquals(1, inside.incrementAndGet());
                              try {
                                return service.add(100, 101, to, RelationType.BROADER);
                              } finally {
                                inside.decrementAndGet();
                              }
                            });
                      }));
            }
            for (Future<Boolean> result : results)
              Assertions.assertTrue(result.get(20, TimeUnit.SECONDS));
            Assertions.assertEquals(Set.of(102L, 103L), service.listBroader(100, 101));
            Assertions.assertEquals(1, scalar("SELECT COUNT(*) FROM glossary_hierarchy_lock"));
          } finally {
            writers.shutdownNow();
            Assertions.assertTrue(writers.awaitTermination(10, TimeUnit.SECONDS));
          }
        });
  }

  /**
   * T3.5: a returned operation retains its lock through outer commit while reads remain unlocked.
   */
  @TestTemplate
  public void testLockHeldThroughOuterCommitAndReadersDoNotBlock() throws Exception {
    repeatRace(
        () -> {
          setupTerms(3);
          ExecutorService worker = Executors.newFixedThreadPool(2);
          CountDownLatch attempted = new CountDownLatch(1);
          try {
            SessionUtils.beginTransaction();
            Future<Boolean> waiting;
            try {
              service.add(100, 101, 102, RelationType.BROADER);
              waiting =
                  worker.submit(
                      () -> {
                        attempted.countDown();
                        return service.add(100, 101, 103, RelationType.BROADER);
                      });
              Assertions.assertTrue(attempted.await(10, TimeUnit.SECONDS));
              Assertions.assertThrows(
                  TimeoutException.class, () -> waiting.get(200, TimeUnit.MILLISECONDS));
              Assertions.assertTrue(
                  worker
                      .submit(() -> service.listBroader(100, 101))
                      .get(10, TimeUnit.SECONDS)
                      .isEmpty());
              SessionUtils.commitTransaction();
            } finally {
              if (SessionUtils.isInTransaction()) SessionUtils.rollbackTransaction();
            }
            Assertions.assertTrue(waiting.get(10, TimeUnit.SECONDS));
            Assertions.assertEquals(Set.of(102L, 103L), service.listBroader(100, 101));
          } finally {
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
          }
        });
  }

  /** T3.1: repeated edge generations, strict expiry cutoff and bounded garbage collection. */
  @TestTemplate
  public void testRetiredGenerationsAndBoundedGc() throws Exception {
    setupTerms(2);
    for (int i = 0; i < 3; i++) {
      Assertions.assertTrue(service.add(100, 101, 102, RelationType.BROADER));
      // Force all removals behind a retained future tombstone, independently of clock precision.
      if (i == 0) {
        execute("INSERT INTO glossary_term_rel VALUES (100,101,102,'BROADER',4102444800000)");
      }
      Assertions.assertTrue(service.remove(100, 101, 102, RelationType.BROADER));
    }
    Assertions.assertEquals(4, scalar("SELECT COUNT(DISTINCT deleted_at) FROM glossary_term_rel"));
    Assertions.assertTrue(service.add(100, 101, 102, RelationType.BROADER));
    Assertions.assertEquals(0, service.collect(4102444800000L, 20));
    Assertions.assertEquals(0, service.collect(Long.MAX_VALUE, 0));
    Assertions.assertEquals(2, service.collect(Long.MAX_VALUE, 2));
    Assertions.assertEquals(2, service.collect(Long.MAX_VALUE, 2));
    Assertions.assertEquals(0, service.collect(Long.MAX_VALUE, 2));
    Assertions.assertEquals(Set.of(102L), service.listBroader(100, 101));
  }

  /** T3.1: definition and metalake deletion retire edges and collect their tombstones. */
  @TestTemplate
  public void testTermAndMetalakeCleanup() throws Exception {
    setupTerms(3);
    service.add(100, 101, 102, RelationType.BROADER);
    service.add(100, 103, 102, RelationType.RELATED);
    Assertions.assertTrue(
        GlossaryTermMetaService.getInstance().delete(NameIdentifier.of(NS, "Term102")));
    Assertions.assertTrue(service.listBroader(100, 101).isEmpty());
    Assertions.assertTrue(service.listRelated(100, 103).isEmpty());
    Assertions.assertEquals(0, scalar("SELECT COUNT(*) FROM glossary_term_rel WHERE deleted_at=0"));
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> service.add(100, 101, 102, RelationType.BROADER));
    service.add(100, 101, 103, RelationType.RELATED);
    backend.delete(NameIdentifier.of("glossary"), Entity.EntityType.METALAKE, true);
    Assertions.assertEquals(0, scalar("SELECT COUNT(*) FROM glossary_term_rel WHERE deleted_at=0"));
    Assertions.assertEquals(0, scalar("SELECT COUNT(*) FROM glossary_hierarchy_lock"));
    Assertions.assertTrue(service.ancestors(100, 101).isEmpty());
    // The existing GLOSSARY_TERM GC entry point also drains relation tombstones.
    while (GlossaryTermMetaService.getInstance().collect(Long.MAX_VALUE, 1) != 0) {}
    Assertions.assertEquals(0, scalar("SELECT COUNT(*) FROM glossary_term_rel"));
    Assertions.assertEquals(0, scalar("SELECT COUNT(*) FROM glossary_term_meta"));
  }

  /**
   * Non-cascading metalake deletion preserves glossary state on rejection and retires it on
   * success.
   */
  @TestTemplate
  public void testNonCascadeMetalakeCleanup() throws Exception {
    setupTerms(2);
    NameIdentifier metalake = NameIdentifier.of("glossary");
    NameIdentifier catalog = createAndInsertCatalog("glossary", "catalog").nameIdentifier();
    Assertions.assertTrue(service.add(100, 101, 102, RelationType.BROADER));

    Assertions.assertThrows(
        NonEmptyEntityException.class,
        () -> backend.delete(metalake, Entity.EntityType.METALAKE, false));
    Assertions.assertEquals(Set.of(102L), service.listBroader(100, 101));
    Assertions.assertEquals(
        1, scalar("SELECT COUNT(*) FROM glossary_hierarchy_lock WHERE metalake_id=100"));

    Assertions.assertTrue(backend.delete(catalog, Entity.EntityType.CATALOG, false));
    Assertions.assertTrue(backend.delete(metalake, Entity.EntityType.METALAKE, false));
    // Joins against the retired parent would hide a leaked live edge. Check the stored generations.
    Assertions.assertEquals(
        1, scalar("SELECT COUNT(*) FROM glossary_term_rel WHERE metalake_id=100 AND deleted_at>0"));
    Assertions.assertEquals(
        0, scalar("SELECT COUNT(*) FROM glossary_term_rel WHERE metalake_id=100 AND deleted_at=0"));
    Assertions.assertEquals(
        0, scalar("SELECT COUNT(*) FROM glossary_hierarchy_lock WHERE metalake_id=100"));
  }

  /** T3.5: the committed mutex itself excludes a writer independently of the parent fence. */
  @TestTemplate
  public void testMutexExcludesIndependentDatabaseWriter() throws Exception {
    repeatRace(
        () -> {
          setupTerms(2);
          service.withHierarchyLock(100, () -> null);
          ExecutorService worker = Executors.newSingleThreadExecutor();
          CountDownLatch attempted = new CountDownLatch(1);
          try {
            SessionUtils.beginTransaction();
            Future<Long> waiting;
            try {
              service.withHierarchyLock(100, () -> null);
              waiting =
                  worker.submit(
                      () ->
                          SessionUtils.doWithCommitAndFetchResult(
                              GlossaryTermRelMapper.class,
                              mapper -> {
                                attempted.countDown();
                                // Deliberately skip the parent fence to prove the mutex itself,
                                // including
                                // on H2.
                                mapper.ensureLock(100);
                                return mapper.lockHierarchy(100);
                              }));
              Assertions.assertTrue(attempted.await(10, TimeUnit.SECONDS));
              Assertions.assertThrows(
                  TimeoutException.class, () -> waiting.get(200, TimeUnit.MILLISECONDS));
              SessionUtils.commitTransaction();
            } finally {
              if (SessionUtils.isInTransaction()) SessionUtils.rollbackTransaction();
            }
            Assertions.assertEquals(100L, waiting.get(10, TimeUnit.SECONDS));
          } finally {
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
          }
        });
  }

  /**
   * T3.1/T3.5: deletion rollback restores edges and committed deletion fences concurrent additions.
   */
  @TestTemplate
  public void testDeleteFencesNewEdgesAndRollbackRestoresRelations() throws Exception {
    repeatRace(
        () -> {
          setupTerms(3);
          service.add(100, 101, 102, RelationType.BROADER);
          Assertions.assertThrows(
              IllegalStateException.class,
              () ->
                  SessionUtils.doMultipleWithCommit(
                      () -> {
                        GlossaryTermMetaService.getInstance()
                            .delete(NameIdentifier.of(NS, "Term102"));
                        throw new IllegalStateException("rollback deletion");
                      }));
          Assertions.assertEquals(Set.of(102L), service.listBroader(100, 101));
          ExecutorService writer = Executors.newSingleThreadExecutor();
          CountDownLatch attempted = new CountDownLatch(1);
          try {
            SessionUtils.beginTransaction();
            Future<?> waiting;
            try {
              Assertions.assertTrue(
                  GlossaryTermMetaService.getInstance().delete(NameIdentifier.of(NS, "Term102")));
              waiting =
                  writer.submit(
                      () -> {
                        attempted.countDown();
                        Assertions.assertThrows(
                            NoSuchEntityException.class,
                            () -> service.add(100, 103, 102, RelationType.RELATED));
                      });
              Assertions.assertTrue(attempted.await(10, TimeUnit.SECONDS));
              Assertions.assertThrows(
                  TimeoutException.class, () -> waiting.get(200, TimeUnit.MILLISECONDS));
              SessionUtils.commitTransaction();
            } finally {
              if (SessionUtils.isInTransaction()) SessionUtils.rollbackTransaction();
            }
            waiting.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(
                0, scalar("SELECT COUNT(*) FROM glossary_term_rel WHERE deleted_at=0"));
          } finally {
            writer.shutdownNow();
            Assertions.assertTrue(writer.awaitTermination(10, TimeUnit.SECONDS));
          }
        });
  }

  private void repeatRace(Executable race) {
    Assertions.assertTimeoutPreemptively(
        Duration.ofMinutes(2),
        () -> {
          for (int round = 0; round < 20; round++) {
            race.execute();
            checkInvariants();
            // Reset only this test's completed fixture, so each round starts without a mutex row.
            // All worker futures have finished and owned transactions are closed before this reset.
            execute("DELETE FROM glossary_term_rel WHERE metalake_id=100");
            execute("DELETE FROM glossary_hierarchy_lock WHERE metalake_id=100");
            execute("DELETE FROM glossary_term_synonym WHERE metalake_id=100");
            execute("DELETE FROM glossary_term_meta WHERE metalake_id=100");
            execute("DELETE FROM metalake_meta WHERE metalake_id=100");
          }
        });
  }

  private void setupTerms(int count) throws Exception {
    backend.insert(createBaseMakeLake(100L, "glossary", AUDIT_INFO), false);
    for (int i = 1; i <= count; i++) backend.insert(term(100 + i, "Term" + (100 + i), NS), false);
  }

  private GlossaryTermEntity term(long id, String name, Namespace ns) {
    return GlossaryTermEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(ns)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private long scalar(String sql) throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(sql)) {
      Assertions.assertTrue(result.next());
      return result.getLong(1);
    }
  }

  private void execute(String sql) throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }
}
