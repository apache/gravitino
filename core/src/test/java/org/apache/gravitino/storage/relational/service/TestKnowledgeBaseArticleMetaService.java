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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.ArticleRevisionConflictException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.kb.ArticleBody;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.KnowledgeBaseArticleMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.KnowledgeBaseArticlePO;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService.ExpectedRevision;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.function.Executable;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Article storage contracts shared by the H2, MySQL and PostgreSQL test lanes. */
public class TestKnowledgeBaseArticleMetaService extends TestJDBCBackend {
  private static final EntityType TYPE = EntityType.KNOWLEDGE_BASE_ARTICLE;
  private static final Binding TABLE = new Binding("TABLE", 123);
  private static final Binding TERM = new Binding("GLOSSARY_TERM", 123);
  private static final KnowledgeBaseArticleMetaService STORE =
      KnowledgeBaseArticleMetaService.getInstance();

  @TestTemplate
  void testIdentityAndUnicodeRoundTrip() throws IOException {
    createAndInsertMakeLake("lake");
    String name = "𠀀".repeat(127) + "é";
    String body = "数据".repeat(180000);
    KnowledgeBaseArticleEntity entity =
        article(31, name).toBuilder()
            .withTitle("𠀀".repeat(512))
            .withComment("𠀀".repeat(256))
            .withSourceReference("𠀀".repeat(1024))
            .withBodyUpdate(ArticleBody.of(ArticleBody.Format.MARKDOWN, body))
            .build();
    backend.insert(entity, false);
    NameIdentifier nfd = NameIdentifier.of("lake", "𠀀".repeat(127) + "e\u0301");
    assertEquals(entity, backend.get(nfd, TYPE));
    assertEquals(body, STORE.getContent(nfd).body().content());
    assertTrue(backend.exists(nfd, TYPE));
    assertEquals(31, STORE.getIds(nfd).entityId());
    assertThrows(
        EntityAlreadyExistsException.class,
        () -> backend.insert(entity.toBuilder().withId(32L).build(), false));
    assertThrows(
        IllegalArgumentException.class,
        () -> backend.insert(entity.toBuilder().withId(32L).build(), true));
    KnowledgeBaseArticleEntity second =
        entity.toBuilder().withName("𠀀".repeat(127) + "É").withId(32L).build();
    backend.insert(second, false);
    assertEquals(List.of(entity, second), backend.list(entity.namespace(), TYPE, true));
    assertEquals(
        List.of(entity), backend.batchGet(List.of(NameIdentifier.of("lake", "absent"), nfd), TYPE));
    assertEquals(
        List.of(second, entity),
        backend.batchGet(
            List.of(second.nameIdentifier(), NameIdentifier.of("lake", "absent"), nfd), TYPE));
  }

  @TestTemplate
  void testSeparateContentAndRevision() throws IOException {
    long lake = createAndInsertMakeLake("lake").id();
    var entity = article(31, "guide");
    var name = entity.nameIdentifier();
    STORE.insert(entity, false);
    var initial = STORE.getContent(name);
    assertEquals(entity.bodyUpdate(), initial.body());
    assertEquals(new ExpectedRevision(lake, 31, 1), initial.revision());
    assertNull(STORE.get(name).bodyUpdate());
    SessionUtils.doMultipleWithCommit(
        () -> {
          var metadata =
              STORE.update(
                  name,
                  initial.revision(),
                  a -> a.toBuilder().withTitle("New").build(),
                  (before, after) -> assertNull(after.bodyUpdate()));
          var content = STORE.getContent(name);
          assertEquals(initial.body(), content.body());
          assertEquals(metadata.revision(), content.revision().revision());
          assertThrows(
              ArticleRevisionConflictException.class,
              () ->
                  STORE.update(
                      name,
                      initial.revision(),
                      a -> a.toBuilder().withBodyUpdate(initial.body()).build(),
                      (before, after) -> {}));
        });
    long revision = 2;
    for (ArticleBody body :
        List.of(
            ArticleBody.of(ArticleBody.Format.MARKDOWN, "Case"),
            ArticleBody.of(ArticleBody.Format.MARKDOWN, "case"),
            ArticleBody.of(ArticleBody.Format.MARKDOWN, "case "),
            ArticleBody.of(ArticleBody.Format.HTML, "case "),
            ArticleBody.of(ArticleBody.Format.MARKDOWN, "é𠀀 "))) {
      var changed =
          STORE.update(
              name, (KnowledgeBaseArticleEntity a) -> a.toBuilder().withBodyUpdate(body).build());
      assertEquals(++revision, changed.revision());
      assertNull(changed.bodyUpdate());
      assertEquals(body, STORE.getContent(name).body());
      assertEquals(
          revision,
          STORE
              .update(
                  name,
                  null,
                  a -> a.toBuilder().withBodyUpdate(body).build(),
                  (before, after) -> fail("no-op"))
              .revision());
    }
    var last = STORE.getContent(name);
    assertThrows(
        IllegalStateException.class,
        () ->
            STORE.update(
                name,
                last.revision(),
                a -> a.toBuilder().withBodyUpdate(initial.body()).build(),
                (before, after) -> {
                  throw new IllegalStateException("rollback");
                }));
    assertEquals(last, STORE.getContent(name));
  }

  @TestTemplate
  void testEffectiveChangesAndRecreation() throws IOException {
    long lake = createAndInsertMakeLake("lake").id();
    KnowledgeBaseArticleEntity initial = article(11, "article");
    NameIdentifier ident = initial.nameIdentifier();
    STORE.insert(initial, false);
    ExpectedRevision token = new ExpectedRevision(lake, 11, 1);
    assertEquals(
        1,
        STORE.update(ident, token, a -> a, (old, next) -> fail("no-op invoked hook")).revision());
    KnowledgeBaseArticleEntity edited =
        STORE.update(
            ident,
            token,
            a -> a.toBuilder().withTitle("Changed").withBindings(Set.of(TERM)).build(),
            (old, next) -> {
              assertEquals(initial, old);
              assertEquals(2, next.revision());
            });
    assertEquals(edited, STORE.get(ident));
    assertThrows(
        ArticleRevisionConflictException.class,
        () -> STORE.update(ident, token, a -> a, (old, next) -> {}));
    assertTrue(STORE.delete(ident));
    assertFalse(STORE.delete(ident));
    STORE.insert(initial.toBuilder().withId(12L).withBindings(Set.of()).build(), false);
    assertTrue(STORE.get(ident).bindings().isEmpty());
    assertThrows(
        ArticleRevisionConflictException.class,
        () -> STORE.update(ident, token, a -> a, (old, next) -> {}));
    assertEquals(3, backend.hardDeleteLegacyData(TYPE, Long.MAX_VALUE));
    assertEquals(0, STORE.collect(Long.MAX_VALUE, 10));
    assertEquals(12, STORE.getIds(ident).entityId());
  }

  @TestTemplate
  void testExtensionRollback() throws IOException {
    createAndInsertMakeLake("lake");
    KnowledgeBaseArticleEntity entity = article(11, "article");
    NameIdentifier ident = entity.nameIdentifier();
    assertThrows(
        IllegalStateException.class,
        () ->
            STORE.insert(
                entity,
                false,
                (before, after) -> {
                  assertEquals(entity, STORE.get(ident));
                  throw new IllegalStateException("owner write failed");
                }));
    assertFalse(backend.exists(ident, TYPE));
    STORE.insert(entity, false);
    assertThrows(
        IllegalStateException.class,
        () ->
            STORE.update(
                ident,
                null,
                a -> a.toBuilder().withBindings(Set.of()).withTitle("replaced").build(),
                (before, after) -> {
                  assertEquals(after, STORE.get(ident));
                  throw new IllegalStateException("extension failed");
                }));
    assertEquals(entity, STORE.get(ident));
    assertThrows(
        IllegalStateException.class,
        () ->
            STORE.delete(
                ident,
                (before, after) -> {
                  assertEquals(entity, before);
                  throw new IllegalStateException("extension failed");
                }));
    assertEquals(entity, STORE.get(ident));
  }

  @TestTemplate
  void testSnapshotAcrossConcurrentCommit() throws IOException {
    long lake = createAndInsertMakeLake("lake").id();
    KnowledgeBaseArticleEntity entity = article(11, "article");
    STORE.insert(entity, false);
    SqlSessions.readSnapshot(
        () ->
            SessionUtils.getWithoutCommit(
                KnowledgeBaseArticleMapper.class,
                mapper -> {
                  assertEquals(1, mapper.get(lake, entity.name()).getRevision());
                  CompletableFuture.runAsync(
                          () ->
                              STORE.update(
                                  entity.nameIdentifier(),
                                  (KnowledgeBaseArticleEntity a) ->
                                      a.toBuilder()
                                          .withBindings(Set.of())
                                          .withTitle("New title")
                                          .build()))
                      .orTimeout(15, TimeUnit.SECONDS)
                      .join();
                  assertEquals(entity.bindings(), Set.copyOf(mapper.bindings(entity.id())));
                  SessionUtils.clearSessionCache();
                  assertEquals(1, mapper.get(lake, entity.name()).getRevision());
                  return null;
                }));
    assertFalse(SqlSessions.hasActiveSession());
    assertEquals(Set.of(), STORE.get(entity.nameIdentifier()).bindings());
    KnowledgeBaseArticlePO summary =
        SessionUtils.getWithoutCommit(
            KnowledgeBaseArticleMapper.class, m -> m.summaries(lake).get(0));
    assertEquals("New title", summary.getTitle());
    assertNull(summary.getBodyContent());
  }

  @TestTemplate
  void testNestedFailureLeavesOuterTransactionUsable() throws IOException {
    createAndInsertMakeLake("lake");
    KnowledgeBaseArticleEntity original = article(11, "one");
    STORE.insert(original, false);
    SessionUtils.doMultipleWithCommit(
        () -> {
          STORE.update(
              original.nameIdentifier(),
              (KnowledgeBaseArticleEntity current) ->
                  current.toBuilder().withTitle("outer edit").build());
          assertThrows(
              IllegalStateException.class,
              () ->
                  STORE.update(
                      original.nameIdentifier(),
                      null,
                      current -> current.toBuilder().withBindings(Set.of()).build(),
                      (old, next) -> {
                        throw new IllegalStateException("failed extension");
                      }));
        });
    KnowledgeBaseArticleEntity actual = STORE.get(original.nameIdentifier());
    assertEquals("outer edit", actual.title());
    assertEquals(original.bindings(), actual.bindings());
    assertEquals(2, actual.revision());
  }

  @TestTemplate
  void testConcurrentEditsPreserveBothChanges() throws Exception {
    createAndInsertMakeLake("lake");
    KnowledgeBaseArticleEntity initial = article(11, "article");
    STORE.insert(initial, false);
    CompletableFuture<Void> title =
        CompletableFuture.runAsync(
            () ->
                STORE.update(
                    initial.nameIdentifier(),
                    (KnowledgeBaseArticleEntity a) ->
                        a.toBuilder().withTitle("new title").build()));
    CompletableFuture<Void> bindings =
        CompletableFuture.runAsync(
            () ->
                STORE.update(
                    initial.nameIdentifier(),
                    (KnowledgeBaseArticleEntity a) ->
                        a.toBuilder().withBindings(Set.of(TERM)).build()));
    CompletableFuture.allOf(title, bindings).get(20, TimeUnit.SECONDS);
    KnowledgeBaseArticleEntity actual = STORE.get(initial.nameIdentifier());
    assertEquals("new title", actual.title());
    assertEquals(Set.of(TERM), actual.bindings());
    assertEquals(3, actual.revision());
  }

  @TestTemplate
  void testMetalakeCascade() throws IOException {
    createAndInsertMakeLake("lake");
    KnowledgeBaseArticleEntity entity = article(11, "article");
    STORE.insert(entity, false);
    assertTrue(backend.delete(NameIdentifier.of("lake"), EntityType.METALAKE, true));
    assertFalse(backend.exists(entity.nameIdentifier(), TYPE));
    assertEquals(3, STORE.collect(Long.MAX_VALUE, 10));
  }

  @TestTemplate
  void testNestedIdLookupsObserveRecreation() throws IOException {
    createAndInsertMakeLake("lake");
    var initial = article(71, "guide");
    var ident = initial.nameIdentifier();
    STORE.insert(initial, false);
    SessionUtils.beginTransaction();
    try {
      assertEquals(71, STORE.getIds(ident).entityId());
      assertTimeoutPreemptively(
          Duration.ofSeconds(15),
          () -> {
            assertTrue(STORE.delete(ident));
          });
      assertFalse(backend.exists(ident, TYPE));
      assertTimeoutPreemptively(
          Duration.ofSeconds(15),
          () -> {
            STORE.insert(article(72, "guide"), false);
          });
      assertEquals(72, STORE.getIds(ident).entityId());
    } finally {
      SessionUtils.rollbackTransaction();
    }
  }

  @TestTemplate
  void testNestedListDistinguishesMissingMetalake() throws IOException {
    createAndInsertMakeLake("lake");
    var absent = Namespace.of("missing");
    var topLevel = assertThrows(NoSuchEntityException.class, () -> STORE.list(absent));
    SessionUtils.doMultipleWithCommit(
        () -> {
          assertEquals(List.of(), STORE.list(Namespace.of("lake")));
          var nested = assertThrows(NoSuchEntityException.class, () -> STORE.list(absent));
          assertEquals(topLevel.getMessage(), nested.getMessage());
        });
  }

  @TestTemplate
  void testBatchLookupAcrossParameterLimit() throws IOException {
    createAndInsertMakeLake("lake");
    var first = article(31, "café");
    var last = article(32, "last");
    STORE.insert(first, false);
    STORE.insert(last, false);
    List<NameIdentifier> names = new ArrayList<>();
    names.add(first.nameIdentifier());
    int missing = backendType.equals("postgresql") ? 65535 : 501;
    for (int i = 0; i < missing; i++) {
      names.add(NameIdentifier.of("lake", "missing-" + i));
    }
    names.add(last.nameIdentifier());
    names.add(NameIdentifier.of("lake", "cafe\u0301"));
    assertEquals(List.of(first, last, first), STORE.batchGet(names));
    SessionUtils.doMultipleWithCommit(
        () -> assertEquals(List.of(first, last, first), STORE.batchGet(names)));
  }

  @TestTemplate
  void testNestedConditionalEditChecksLockedRevision() throws IOException {
    assumeFalse("h2".equals(backendType), "H2 serializes these writers on the parent row");
    long lake = createAndInsertMakeLake("lake").id();
    var initial = article(71, "guide");
    STORE.insert(initial, false);
    var ident = initial.nameIdentifier();
    var revision = new ExpectedRevision(lake, initial.id(), initial.revision());
    SessionUtils.doMultipleWithCommit(
        () -> {
          assertThrows(
              ArticleRevisionConflictException.class,
              () ->
                  STORE.update(
                      ident,
                      revision,
                      before -> {
                        // Commit a peer edit after observation and before the Article lock.
                        assertTimeoutPreemptively(
                            Duration.ofSeconds(15),
                            () -> {
                              STORE.update(
                                  ident,
                                  (KnowledgeBaseArticleEntity a) ->
                                      a.toBuilder().withTitle("Peer").build());
                            });
                        return before;
                      },
                      (before, after) -> fail("stale edit invoked hook")));
          assertEquals("Peer", STORE.get(ident).title());
        });
  }

  @TestTemplate
  void testDeleteLosingTheLockReturnsFalse() throws IOException {
    assumeFalse("h2".equals(backendType), "H2 serializes these writers on the parent row");
    createAndInsertMakeLake("lake");
    for (boolean recreate : List.of(false, true)) {
      var initial = article(recreate ? 72 : 71, "guide");
      var ident = initial.nameIdentifier();
      STORE.insert(initial, false);
      // Change only the timing: the peer commits after this thread reads the live Article.
      try (MockedStatic<SessionUtils> timing =
          interleaveArticleReads(
              () -> {},
              () -> {
                assertTrue(STORE.delete(ident));
                if (recreate) {
                  STORE.insert(article(73, "guide"), false);
                }
              })) {
        assertFalse(STORE.delete(ident, (before, after) -> fail("losing delete invoked hook")));
      }
      if (recreate) {
        assertEquals(73, STORE.getIds(ident).entityId());
      }
    }
  }

  @TestTemplate
  void testRetriedDeleteKeepsOriginalIdentity() throws IOException {
    assumeFalse("h2".equals(backendType), "H2 serializes these writers on the parent row");
    createAndInsertMakeLake("lake");
    var initial = article(71, "guide");
    var ident = initial.nameIdentifier();
    STORE.insert(initial, false);
    AtomicInteger attempts = new AtomicInteger();
    try (MockedStatic<SessionUtils> timing =
        interleaveArticleReads(
            () -> {
              if (attempts.incrementAndGet() == 2) {
                assertTrue(STORE.delete(ident));
                STORE.insert(article(72, "guide"), false);
              }
            },
            () -> {
              if (attempts.get() == 1) {
                STORE.update(
                    ident,
                    (KnowledgeBaseArticleEntity a) -> a.toBuilder().withTitle("Peer edit").build());
              }
            })) {
      assertFalse(STORE.delete(ident));
    }
    assertEquals(2, attempts.get());
    assertEquals(72, STORE.getIds(ident).entityId());
  }

  @TestTemplate
  void testParentLockTimeoutRollsBackOnlyNestedWrite() throws Exception {
    assumeTrue(
        "postgresql".equals(backendType), "PostgreSQL aborts the transaction after a lock timeout");
    long blockedLake = createAndInsertMakeLake("blocked").id();
    createAndInsertMakeLake("lake");
    var earlier = article(71, "guide");
    STORE.insert(earlier, false);
    try (SqlSession holder =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession()) {
      holder.getMapper(MetalakeMetaMapper.class).selectMetalakeMetaByIdForUpdate(blockedLake);
      SessionUtils.beginTransaction();
      try {
        STORE.update(
            earlier.nameIdentifier(),
            (KnowledgeBaseArticleEntity a) -> a.toBuilder().withTitle("Earlier edit").build());
        try (Statement sql = SqlSessions.currentConnection().createStatement()) {
          sql.execute("SET LOCAL lock_timeout = '250ms'");
        }
        var failure =
            assertThrows(
                RuntimeException.class, () -> STORE.delete(NameIdentifier.of("blocked", "absent")));
        assertTrue(ExceptionUtils.isLockWaitTimeout(failure), failure.toString());
        assertEquals("Earlier edit", STORE.get(earlier.nameIdentifier()).title());
        SessionUtils.commitTransaction();
      } finally {
        if (SqlSessions.hasActiveSession()) {
          SessionUtils.rollbackTransaction();
        }
      }
    }
    assertEquals("Earlier edit", STORE.get(earlier.nameIdentifier()).title());
  }

  // The peer uses its own transaction; both the observation and the later lock still hit the DB.
  private static MockedStatic<SessionUtils> interleaveArticleReads(
      Executable before, Executable after) {
    return Mockito.mockStatic(
        SessionUtils.class,
        invocation -> {
          if (invocation.getMethod().getName().equals("getWithoutCommitFresh")
              && invocation.getArgument(0) == KnowledgeBaseArticleMapper.class) {
            assertTimeoutPreemptively(Duration.ofSeconds(15), before);
            Object rows = invocation.callRealMethod();
            assertTimeoutPreemptively(Duration.ofSeconds(15), after);
            return rows;
          }
          return invocation.callRealMethod();
        });
  }

  private static KnowledgeBaseArticleEntity article(long id, String name) {
    return KnowledgeBaseArticleEntity.builder()
        .withId(id)
        .withNamespace(Namespace.of("lake"))
        .withName(name)
        .withTitle("Title")
        .withBodyUpdate(ArticleBody.of(ArticleBody.Format.HTML, "<p>Body</p>"))
        .withProperties(Map.of("lang", "中文"))
        .withSynonyms(List.of("Article", "alternative"))
        .withBindings(Set.of(TABLE, TERM))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }
}
