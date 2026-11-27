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

package org.apache.gravitino.kb;

import static org.apache.gravitino.storage.relational.service.GlossaryDomainFixtures.insertDomain;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.ArticleRevisionConflictException;
import org.apache.gravitino.exceptions.KnowledgeBaseArticleAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchKnowledgeBaseArticleException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService.MissingArticleScopeException;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService.NameConflictException;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.TestTemplate;

class TestArticleManager extends TestJDBCBackend {
  private static final String LAKE = "lake";
  private final AtomicLong ids = new AtomicLong(100);
  private final ArticleManager manager = new ArticleManager(ids::incrementAndGet, 1024);

  @TestTemplate
  void lifecycleAndConditionalEdits() throws IOException {
    assertThrows(NoSuchMetalakeException.class, () -> manager.listArticles(LAKE));
    assertThrows(
        NoSuchMetalakeException.class, () -> manager.createArticle(LAKE, request().build()));
    assertThrows(
        NoSuchKnowledgeBaseArticleException.class,
        () ->
            manager.alterArticle(
                LAKE, "café", null, KnowledgeBaseArticleChange.updateTitle("Changed")));
    assertFalse(manager.deleteArticle(LAKE, "café"));
    createAndInsertMakeLake(LAKE);
    ArticleManager.Result created =
        manager.createArticle(LAKE, request().withName("cafe\u0301").build());
    assertEquals("café", created.article().name());
    assertEquals(ArticleStatus.PUBLISHED, created.article().status());
    assertEquals(created, manager.loadArticle(LAKE, "cafe\u0301"));
    ArticleContent content = manager.getArticleContent(LAKE, "cafe\u0301");
    assertEquals(created.revisionToken(), content.revisionToken());
    assertEquals(request().build().body(), content.body());
    assertEquals(List.of(created.article()), manager.listArticles(LAKE));

    ArticleManager.Result edited =
        manager.alterArticle(
            LAKE,
            "café",
            created.revisionToken(),
            KnowledgeBaseArticleChange.updateStatus(ArticleStatus.DRAFT),
            KnowledgeBaseArticleChange.updateTitle("Changed"),
            KnowledgeBaseArticleChange.updateSynonyms(new String[] {" one ", "ONE", "two"}),
            KnowledgeBaseArticleChange.updateComment("note"),
            KnowledgeBaseArticleChange.updateSourceReference("source"),
            KnowledgeBaseArticleChange.setProperty("k", "v"));
    assertEquals(2, edited.article().revision());
    ArticleContent afterEdit = manager.getArticleContent(LAKE, "café");
    assertEquals(edited.revisionToken(), afterEdit.revisionToken());
    assertEquals(content.body(), afterEdit.body());
    assertEquals("café", edited.article().name());
    assertEquals(ArticleStatus.DRAFT, edited.article().status());
    assertEquals(List.of("one", "two"), edited.article().synonyms());
    assertEquals("note", edited.article().comment());
    assertEquals("source", edited.article().sourceReference());
    assertEquals(Map.of("k", "v"), edited.article().properties());
    assertEquals(
        edited,
        manager.alterArticle(
            LAKE,
            "café",
            edited.revisionToken(),
            KnowledgeBaseArticleChange.removeProperty("k"),
            KnowledgeBaseArticleChange.setProperty("k", "v")));
    assertThrows(
        ArticleRevisionConflictException.class,
        () -> manager.alterArticle(LAKE, "café", created.revisionToken()));
    assertThrows(
        IllegalArgumentException.class, () -> manager.alterArticle(LAKE, "café", "not a token"));
    assertThrows(
        IllegalArgumentException.class,
        () -> manager.alterArticle(LAKE, "café", "A".repeat(31) + "="));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            manager.alterArticle(
                LAKE,
                "café",
                null,
                KnowledgeBaseArticleChange.updateStatus(ArticleStatus.DEPRECATED),
                KnowledgeBaseArticleChange.updateBody(
                    ArticleBody.of(ArticleBody.Format.MARKDOWN, "a".repeat(1025)))));
    assertEquals(edited, manager.loadArticle(LAKE, "café"));

    assertTrue(manager.deleteArticle(LAKE, "cafe\u0301"));
    assertFalse(manager.deleteArticle(LAKE, "café"));
    assertThrows(
        NoSuchKnowledgeBaseArticleException.class, () -> manager.getArticleContent(LAKE, "café"));
    assertThrows(
        NoSuchKnowledgeBaseArticleException.class, () -> manager.loadArticle(LAKE, "café"));
    ArticleManager.Result replacement =
        manager.createArticle(LAKE, request().withName("café").build());
    assertNotEquals(created.article().id(), replacement.article().id());
    assertThrows(
        ArticleRevisionConflictException.class,
        () -> manager.alterArticle(LAKE, "café", created.revisionToken()));
  }

  @TestTemplate
  void staleBodySavePreservesTheFirstEditorsContent() throws IOException {
    createAndInsertMakeLake(LAKE);
    var created = manager.createArticle(LAKE, request().build());
    assertNull(created.article().owner());
    var first = manager.getArticleContent(LAKE, "guide");
    var second = manager.getArticleContent(LAKE, "guide");
    var body = ArticleBody.of(ArticleBody.Format.MARKDOWN, "First edit");
    var saved =
        manager.alterArticle(
            LAKE, "guide", first.revisionToken(), KnowledgeBaseArticleChange.updateBody(body));
    assertThrows(
        ArticleRevisionConflictException.class,
        () ->
            manager.alterArticle(
                LAKE,
                "guide",
                second.revisionToken(),
                KnowledgeBaseArticleChange.updateBody(
                    ArticleBody.of(ArticleBody.Format.MARKDOWN, "Second edit"))));
    var retained = manager.getArticleContent(LAKE, "guide");
    assertEquals(body, retained.body());
    assertEquals(saved.revisionToken(), retained.revisionToken());
  }

  @TestTemplate
  void legacyBlankPropertyCanBeRemovedByItsExactKey() throws Exception {
    createAndInsertMakeLake(LAKE);
    var created = manager.createArticle(LAKE, request().build());
    try (var session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        var update =
            session
                .getConnection()
                .prepareStatement("UPDATE kb_article_meta SET properties=? WHERE article_id=?")) {
      update.setString(1, "{\" \":\"legacy\",\"keep\":\"v\"}");
      update.setLong(2, created.article().id());
      assertEquals(1, update.executeUpdate());
    }
    var loaded = manager.loadArticle(LAKE, "guide");
    assertEquals(Map.of(" ", "legacy", "keep", "v"), loaded.article().properties());
    var edited =
        manager.alterArticle(
            LAKE,
            "guide",
            loaded.revisionToken(),
            KnowledgeBaseArticleChange.setProperty("keep", "updated"));
    assertEquals("legacy", edited.article().properties().get(" "));
    manager.alterArticle(
        LAKE, "guide", edited.revisionToken(), KnowledgeBaseArticleChange.removeProperty(" "));
    var stored = manager.loadArticle(LAKE, "guide").article();
    assertEquals(Map.of("keep", "updated"), stored.properties());
    assertEquals(3, stored.revision());
  }

  @TestTemplate
  void conditionalEditAcceptsStoredNegativeIds() throws IOException {
    // MySQL stores metalake IDs in an unsigned column.
    long metalakeId = "mysql".equals(backendType) ? 1L : -1L;
    backend.insert(createBaseMakeLake(metalakeId, LAKE, AUDIT_INFO), false);
    ArticleManager manager = new ArticleManager(() -> -2L, 1024);
    var created = manager.createArticle(LAKE, request().build());
    var edited =
        manager.alterArticle(
            LAKE,
            "guide",
            created.revisionToken(),
            KnowledgeBaseArticleChange.updateTitle("Edited"));
    assertEquals("Edited", edited.article().title());
    assertEquals(2, edited.article().revision());
  }

  @TestTemplate
  void generatedNamesUseAvailableSuffixesInCallerTransaction() throws IOException {
    createAndInsertMakeLake(LAKE);
    manager.createArticle(LAKE, request().build());
    SessionUtils.doMultipleWithCommit(
        () -> {
          assertEquals("guide-2", manager.createArticle(LAKE, request().build()).article().name());
          assertEquals("guide-3", manager.createArticle(LAKE, request().build()).article().name());
          assertThrows(
              KnowledgeBaseArticleAlreadyExistsException.class,
              () -> manager.createArticle(LAKE, request().withName("guide").build()));
        });
    assertEquals("guide-2", manager.loadArticle(LAKE, "guide-2").article().name());
    assertEquals("guide-3", manager.loadArticle(LAKE, "guide-3").article().name());
  }

  @TestTemplate
  void generatedNameLostToAnotherWriterRequiresCallerTransactionRetry() throws IOException {
    createAndInsertMakeLake(LAKE);
    manager.createArticle(LAKE, request().build());
    ArticleManager racing =
        new ArticleManager(
            () -> {
              CompletableFuture.runAsync(
                      () -> manager.createArticle(LAKE, request().withName("guide-2").build()))
                  .orTimeout(15, TimeUnit.SECONDS)
                  .join();
              return ids.incrementAndGet();
            },
            1024);
    assertThrows(
        OptimisticLockException.class,
        () ->
            SessionUtils.doMultipleWithCommit(() -> racing.createArticle(LAKE, request().build())));
    SessionUtils.doMultipleWithCommit(
        () ->
            assertEquals(
                "guide-3", manager.createArticle(LAKE, request().build()).article().name()));
  }

  @TestTemplate
  void generatedNameRetryIsLimitedToLiveNameConflicts() throws IOException {
    createAndInsertMakeLake(LAKE);
    ArticleManager.Result first = manager.createArticle(LAKE, request().build());
    for (int index = 2; index <= 8; index++) {
      assertEquals(
          "guide-" + index, manager.createArticle(LAKE, request().build()).article().name());
    }
    assertThrows(
        KnowledgeBaseArticleAlreadyExistsException.class,
        () -> manager.createArticle(LAKE, request().withName("guide").build()));
    ArticleManager duplicateId = new ArticleManager(() -> first.article().id(), 1024);
    assertThrows(
        EntityAlreadyExistsException.class,
        () -> duplicateId.createArticle(LAKE, request().withTitle("another").build()));
    assertEquals(8, manager.listArticles(LAKE).size());
  }

  @TestTemplate
  void generatedNameRetryRejectsAReplacementMetalake() throws IOException {
    createAndInsertMakeLake(LAKE);
    manager.createArticle(LAKE, request().build());
    AtomicLong attempts = new AtomicLong();
    ArticleManager racing =
        new ArticleManager(
            () -> {
              if (attempts.incrementAndGet() == 2) {
                try {
                  assertTrue(backend.delete(NameIdentifier.of(LAKE), EntityType.METALAKE, true));
                  createAndInsertMakeLake(LAKE);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              }
              return ids.incrementAndGet();
            },
            1024);
    assertThrows(
        NoSuchMetalakeException.class, () -> racing.createArticle(LAKE, request().build()));
    assertEquals(2, attempts.get());
    assertTrue(manager.listArticles(LAKE).isEmpty());
  }

  @TestTemplate
  void hookLookupFailurePropagatesAndRollsBackEachWrite() throws IOException {
    createAndInsertMakeLake(LAKE);
    var original = manager.createArticle(LAKE, request().build());
    var sibling = NameIdentifier.of(LAKE, "absent-sibling");
    var guarded =
        new ArticleManager(
            ids::incrementAndGet,
            1024,
            (before, after) -> KnowledgeBaseArticleMetaService.getInstance().getContent(sibling));
    assertThrows(
        MissingArticleScopeException.class,
        () -> guarded.createArticle(LAKE, request().withName("other").build()));
    assertThrows(
        MissingArticleScopeException.class,
        () ->
            guarded.alterArticle(
                LAKE, "guide", null, KnowledgeBaseArticleChange.updateTitle("Changed")));
    assertThrows(MissingArticleScopeException.class, () -> guarded.deleteArticle(LAKE, "guide"));
    assertEquals(List.of(original.article()), manager.listArticles(LAKE));
    assertEquals(original, manager.loadArticle(LAKE, "guide"));
  }

  @TestTemplate
  void hookInsertConflictDoesNotRetryTheOuterCreate() throws IOException {
    createAndInsertMakeLake(LAKE);
    var sibling = manager.createArticle(LAKE, request().withName("sibling").build());
    var duplicate =
        sibling.article().toBuilder()
            .withId(ids.incrementAndGet())
            .withBodyUpdate(request().build().body())
            .build();
    AtomicLong calls = new AtomicLong();
    var guarded =
        new ArticleManager(
            ids::incrementAndGet,
            1024,
            (before, after) -> {
              calls.incrementAndGet();
              try {
                KnowledgeBaseArticleMetaService.getInstance().insert(duplicate, false);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });
    assertThrows(NameConflictException.class, () -> guarded.createArticle(LAKE, request().build()));
    assertEquals(1, calls.get());
    assertEquals(List.of(sibling.article()), manager.listArticles(LAKE));
  }

  @TestTemplate
  void retriesAtMostFiveConcurrentNameLosses() throws IOException {
    createAndInsertMakeLake(LAKE);
    AtomicLong attempts = new AtomicLong();
    ArticleManager racing =
        new ArticleManager(
            () -> {
              // A competing create commits after candidate selection and before the row insert.
              long attempt = attempts.incrementAndGet();
              String name = attempt == 1 ? "guide" : "guide-" + attempt;
              manager.createArticle(LAKE, request().withName(name).build());
              return ids.incrementAndGet();
            },
            1024);
    assertThrows(
        KnowledgeBaseArticleAlreadyExistsException.class,
        () -> racing.createArticle(LAKE, request().build()));
    assertEquals(6, attempts.get());
    assertEquals(6, manager.listArticles(LAKE).size());
  }

  @TestTemplate
  void concurrentCreatesAllocateDifferentNames() throws Exception {
    createAndInsertMakeLake(LAKE);
    CountDownLatch start = new CountDownLatch(1);
    var first =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                assertTrue(start.await(10, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                throw new AssertionError(e);
              }
              return manager.createArticle(LAKE, request().build()).article().name();
            });
    start.countDown();
    String second = manager.createArticle(LAKE, request().build()).article().name();
    assertEquals(Set.of("guide", "guide-2"), Set.of(first.get(15, TimeUnit.SECONDS), second));
  }

  @TestTemplate
  void concurrentIndependentEditsKeepBothChanges() throws Exception {
    createAndInsertMakeLake(LAKE);
    manager.createArticle(LAKE, request().build());
    var edit =
        CompletableFuture.runAsync(
            () ->
                manager.alterArticle(
                    LAKE, "guide", null, KnowledgeBaseArticleChange.updateTitle("New title")));
    manager.alterArticle(
        LAKE, "guide", null, KnowledgeBaseArticleChange.updateComment("New comment"));
    edit.get(15, TimeUnit.SECONDS);
    var article = manager.loadArticle(LAKE, "guide").article();
    assertEquals(3, article.revision());
    assertEquals("New title", article.title());
    assertEquals("New comment", article.comment());
  }

  @TestTemplate
  void domainReadsWithoutAuthorizationRequireASnapshot() throws Exception {
    createAndInsertMakeLake(LAKE);
    insertDomain(LAKE, "Sales", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    var created = manager.createArticle(LAKE, request().addDomains("Sales").build());
    SqlSessions.getSqlSession();
    try {
      assertThrows(IllegalStateException.class, () -> manager.loadArticle(LAKE, "guide"));
      assertThrows(IllegalStateException.class, () -> manager.listArticles(LAKE, "Sales"));
      assertEquals(
          created.revisionToken(), manager.getArticleContent(LAKE, "guide").revisionToken());
    } finally {
      SqlSessions.closeSqlSession();
    }
    SqlSessions.readSnapshot(
        () -> {
          assertEquals(List.of("Sales"), manager.loadArticle(LAKE, "guide").article().domains());
          CompletableFuture.runAsync(() -> manager.detachDomain(LAKE, "guide", "Sales"))
              .orTimeout(15, TimeUnit.SECONDS)
              .join();
          assertEquals(List.of("Sales"), manager.listArticles(LAKE, "Sales").get(0).domains());
          return null;
        });
    assertTrue(manager.listArticles(LAKE, "Sales").isEmpty());
  }

  @TestTemplate
  void concurrentDomainReplacementsWithoutTokensFollowCommitOrder() throws Exception {
    createAndInsertMakeLake(LAKE);
    insertDomain(LAKE, "one", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    insertDomain(LAKE, "two", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    manager.createArticle(LAKE, request().build());
    var first =
        CompletableFuture.supplyAsync(
            () ->
                manager.alterArticle(
                    LAKE,
                    "guide",
                    null,
                    KnowledgeBaseArticleChange.updateDomains(new String[] {"one"}),
                    KnowledgeBaseArticleChange.updateTitle("Edited")));
    var second =
        manager.alterArticle(
            LAKE,
            "guide",
            null,
            KnowledgeBaseArticleChange.updateDomains(new String[] {"two"}),
            KnowledgeBaseArticleChange.updateComment("Changed"));
    var peer = first.get(15, TimeUnit.SECONDS);
    assertEquals(Set.of(2L, 3L), Set.of(peer.article().revision(), second.article().revision()));
    var current = manager.loadArticle(LAKE, "guide");
    assertEquals(
        peer.article().revision() == 3 ? List.of("one") : List.of("two"),
        current.article().domains());
    assertEquals("Edited", current.article().title());
    assertEquals("Changed", current.article().comment());
  }

  @TestTemplate
  void policyParticipatesInCreateAlterAndDrop() throws IOException {
    createAndInsertMakeLake(LAKE);
    ArticleManager veto =
        new ArticleManager(
            ids::incrementAndGet,
            1024,
            (before, after) -> {
              throw new IllegalStateException("rejected");
            });
    assertThrows(IllegalStateException.class, () -> veto.createArticle(LAKE, request().build()));
    ArticleManager.Result created = manager.createArticle(LAKE, request().build());
    assertEquals("guide", created.article().name());
    assertEquals(
        created,
        veto.alterArticle(
            LAKE,
            "guide",
            created.revisionToken(),
            KnowledgeBaseArticleChange.updateTitle("Guide")));
    assertThrows(
        IllegalStateException.class,
        () ->
            veto.alterArticle(
                LAKE, "guide", null, KnowledgeBaseArticleChange.updateTitle("Rejected")));
    assertThrows(IllegalStateException.class, () -> veto.deleteArticle(LAKE, "guide"));
    ArticleManager missingPolicyTarget =
        new ArticleManager(
            ids::incrementAndGet,
            1024,
            (before, after) -> {
              throw new NoSuchEntityException("Policy target is missing");
            });
    assertThrows(
        NoSuchEntityException.class, () -> missingPolicyTarget.deleteArticle(LAKE, "guide"));
    assertEquals(created, manager.loadArticle(LAKE, "guide"));
  }

  @TestTemplate
  void unrelatedEditsPreserveStoredBindings() throws IOException {
    createAndInsertMakeLake(LAKE);
    var created = manager.createArticle(LAKE, request().build());
    var bindings = Set.of(new KnowledgeBaseArticleEntity.Binding("TABLE", 991));
    KnowledgeBaseArticleMetaService.getInstance()
        .update(
            created.article().nameIdentifier(),
            null,
            article -> article.toBuilder().withBindings(bindings).build(),
            (before, after) -> {});
    var edited =
        manager.alterArticle(
            LAKE, "guide", null, KnowledgeBaseArticleChange.updateStatus(ArticleStatus.DEPRECATED));
    assertEquals(Set.of(), edited.article().bindings());
    assertEquals(
        bindings,
        KnowledgeBaseArticleMetaService.getInstance()
            .getScoped(edited.article().nameIdentifier())
            .article()
            .bindings());
    assertEquals(3, edited.article().revision());
  }

  private static ArticleCreate.Builder request() {
    return ArticleCreate.builder()
        .withTitle("Guide")
        .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "# Guide"));
  }
}
