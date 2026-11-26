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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.exceptions.ArticleRevisionConflictException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.kb.ArticleBody;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.DomainMemberMapper;
import org.apache.gravitino.storage.relational.mapper.DomainMemberMapper.MemberType;
import org.apache.gravitino.storage.relational.mapper.KnowledgeBaseArticleMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.KnowledgeBaseArticlePO;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService.ExpectedRevision;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService.NameConflictException;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService.StoredContent;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService.TransactionHook;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.ibatis.exceptions.PersistenceException;
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
  void testSupplementaryCharactersInArticleBody() throws IOException {
    createAndInsertMakeLake("lake");
    var markdown = ArticleBody.of(ArticleBody.Format.MARKDOWN, "a😀b");
    var original = article(31, "guide").toBuilder().withBodyUpdate(markdown).build();
    var ident = original.nameIdentifier();
    STORE.insert(original, false);
    assertEquals(markdown, STORE.getContent(ident).body());

    var html = ArticleBody.of(ArticleBody.Format.HTML, "<p>𠀀😀</p>");
    STORE.update(
        ident,
        (KnowledgeBaseArticleEntity current) -> current.toBuilder().withBodyUpdate(html).build());
    assertEquals(html, STORE.getContent(ident).body());
  }

  @TestTemplate
  void testNullCharacterMetadataRejectedOnInsertAndUpdate() throws IOException {
    createAndInsertMakeLake("lake");
    var original = article(31, "guide");
    var fresh = article(32, "other");
    STORE.insert(original, false);
    String invalid = "a\0b";
    for (var edit :
        List.<UnaryOperator<KnowledgeBaseArticleEntity>>of(
            a -> a.toBuilder().withTitle(invalid).build(),
            a -> a.toBuilder().withComment(invalid).build(),
            a -> a.toBuilder().withSourceReference(invalid).build())) {
      var createError =
          assertThrows(
              IllegalArgumentException.class, () -> STORE.insert(edit.apply(fresh), false));
      assertTrue(createError.getMessage().contains("U+0000"));
      var editError =
          assertThrows(
              IllegalArgumentException.class, () -> STORE.update(original.nameIdentifier(), edit));
      assertEquals(createError.getMessage(), editError.getMessage());
    }
    assertFalse(backend.exists(fresh.nameIdentifier(), TYPE));
    assertEquals(original, STORE.get(original.nameIdentifier()));
  }

  @TestTemplate
  void testStoredNullCharacterMetadataCanBeRepaired() throws Exception {
    assumeFalse("postgresql".equals(backendType), "PostgreSQL text columns cannot store U+0000");
    createAndInsertMakeLake("lake");
    var original = article(31, "guide");
    STORE.insert(original, false);
    String legacy = "a\0b";
    try (var session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        var statement =
            session
                .getConnection()
                .prepareStatement(
                    "UPDATE kb_article_meta SET title=?, article_comment=?, source_reference=? WHERE article_id=?")) {
      statement.setString(1, legacy);
      statement.setString(2, legacy);
      statement.setString(3, legacy);
      statement.setLong(4, original.id());
      assertEquals(1, statement.executeUpdate());
    }
    var name = original.nameIdentifier();
    var stored = STORE.get(name);
    assertEquals(legacy, stored.title());
    assertEquals(legacy, stored.comment());
    assertEquals(legacy, stored.sourceReference());
    STORE.update(
        name,
        (KnowledgeBaseArticleEntity current) -> current.toBuilder().withTitle("Guide").build());
    String reference = "literal \\u0000";
    STORE.update(
        name,
        (KnowledgeBaseArticleEntity current) ->
            current.toBuilder().withComment(null).withSourceReference(reference).build());
    var repaired = STORE.get(name);
    assertEquals("Guide", repaired.title());
    assertNull(repaired.comment());
    assertEquals(reference, repaired.sourceReference());
    assertEquals(3, repaired.revision());
  }

  @TestTemplate
  void testNullCharacterBodyUpdatePreservesStoredContent() throws IOException {
    createAndInsertMakeLake("lake");
    var article = article(31, "guide");
    STORE.insert(article, false);
    var name = article.nameIdentifier();
    var before = STORE.getContent(name);
    var error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                STORE.update(
                    name,
                    (KnowledgeBaseArticleEntity current) ->
                        current.toBuilder()
                            .withBodyUpdate(ArticleBody.of(ArticleBody.Format.MARKDOWN, "a\0b"))
                            .build()));
    assertTrue(error.getMessage().contains("U+0000"));
    assertEquals(before, STORE.getContent(name));
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
  void testCreateDistinguishesNameAndIdConflicts() throws IOException {
    createAndInsertMakeLake("lake");
    STORE.insert(article(31, "guide"), false);
    assertThrows(NameConflictException.class, () -> STORE.insert(article(32, "guide"), false));
    var idConflict =
        assertThrowsExactly(
            EntityAlreadyExistsException.class, () -> STORE.insert(article(31, "another"), false));
    assertTrue(idConflict.getMessage().contains("Article ID 31"));
    assertFalse(backend.exists(NameIdentifier.of("lake", "another"), TYPE));
  }

  @TestTemplate
  void testLocalizedMySQLInsertConflicts() throws Exception {
    assumeTrue("mysql".equals(backendType), "lc_messages is a MySQL session setting");
    createAndInsertMakeLake("lake");
    STORE.insert(article(71, "guide"), false);
    SessionUtils.beginTransaction();
    try (Statement sql = SqlSessions.currentConnection().createStatement()) {
      sql.execute("SET @original_locale = @@SESSION.lc_messages");
      try {
        sql.execute("SET SESSION lc_messages = 'de_DE'");
        assertThrows(NameConflictException.class, () -> STORE.insert(article(72, "guide"), false));
        var duplicateId =
            assertThrowsExactly(
                EntityAlreadyExistsException.class,
                () -> STORE.insert(article(71, "other"), false));
        assertTrue(duplicateId.getMessage().contains("Article ID 71"));
      } finally {
        sql.execute("SET SESSION lc_messages = @original_locale");
      }
    } finally {
      SessionUtils.rollbackTransaction();
    }
  }

  @TestTemplate
  void testLocalizedPostgreSQLInsertConflicts() throws Exception {
    assumeTrue("postgresql".equals(backendType));
    // The PostgreSQL 13 image includes German translations but only generates the English locale.
    var locale =
        ContainerSuite.getInstance()
            .getPostgreSQLContainer()
            .executeInContainer(
                "localedef", "--no-archive", "-i", "de_DE", "-f", "UTF-8", "de_DE.utf8");
    assertEquals(0, locale.getExitCode(), locale.getStderr());
    createAndInsertMakeLake("lake");
    STORE.insert(article(71, "guide"), false);
    SessionUtils.beginTransaction();
    try (Statement sql = SqlSessions.currentConnection().createStatement()) {
      sql.execute("SET LOCAL lc_messages = 'de_DE.utf8'");
      assertAll(
          () ->
              assertThrowsExactly(
                  NameConflictException.class, () -> STORE.insert(article(72, "guide"), false)),
          () ->
              assertThrowsExactly(
                  EntityAlreadyExistsException.class,
                  () -> STORE.insert(article(71, "other"), false)));
      sql.execute("CREATE UNIQUE INDEX test_unique_title ON kb_article_meta (title)");
      assertThrowsExactly(
          PersistenceException.class, () -> STORE.insert(article(73, "third"), false));
    } finally {
      SessionUtils.rollbackTransaction();
    }
  }

  @TestTemplate
  void testConcurrentOverwriteUsesWinningInsertIdentity() throws IOException {
    assumeFalse("h2".equals(backendType), "H2 serializes these writers on the parent row");
    createAndInsertMakeLake("lake");
    var store = RelationalEntityStore.forOfflineMigration(backend, new Config(false) {});
    for (boolean sameId : List.of(true, false)) {
      var requested = article(sameId ? 71 : 72, sameId ? "guide" : "another");
      var peer = requested.toBuilder().withId(sameId ? 71L : 73L).withTitle("Peer").build();
      AtomicInteger reads = new AtomicInteger();
      try (MockedStatic<SessionUtils> timing =
          interleaveArticleReads(
              () -> {},
              () -> {
                if (reads.incrementAndGet() == 1) {
                  STORE.insert(peer, false);
                }
              })) {
        if (sameId) {
          store.put(requested, true);
        } else {
          assertThrows(NameConflictException.class, () -> store.put(requested, true));
        }
      }
      var stored = STORE.get(requested.nameIdentifier());
      assertEquals(peer.id(), stored.id());
      assertEquals(sameId ? requested.title() : peer.title(), stored.title());
      assertEquals(sameId ? 2L : 1L, stored.revision());
    }
  }

  @TestTemplate
  void testOverwriteWithSameIdAtAnotherNamePreservesConflict() throws IOException {
    createAndInsertMakeLake("lake");
    var original = article(71, "guide");
    STORE.insert(original, false);
    var other = article(71, "other");
    var conflict =
        assertThrowsExactly(
            EntityAlreadyExistsException.class,
            () -> STORE.insert(other, true, (before, after) -> fail("Unexpected overwrite hook")));
    assertTrue(conflict.getMessage().contains("Article ID 71"));
    assertFalse(backend.exists(other.nameIdentifier(), TYPE));
    assertEquals(1L, STORE.get(original.nameIdentifier()).revision());
  }

  @TestTemplate
  void testCallerRetriesConcurrentOverwriteAfterTransactionEnds() throws IOException {
    assumeFalse("h2".equals(backendType), "H2 serializes these writers on the parent row");
    createAndInsertMakeLake("lake");
    var requested = article(71, "guide");
    var peer = requested.toBuilder().withTitle("Peer").build();
    AtomicInteger hooks = new AtomicInteger();
    SessionUtils.beginTransaction();
    try (MockedStatic<SessionUtils> timing =
        interleaveArticleReads(() -> {}, () -> STORE.insert(peer, false))) {
      assertThrows(
          EntityAlreadyExistsException.class,
          () -> STORE.insert(requested, true, (before, after) -> hooks.incrementAndGet()));
      assertEquals(0, hooks.get());
    } finally {
      SessionUtils.rollbackTransaction();
    }
    STORE.insert(requested, true, (before, after) -> hooks.incrementAndGet());
    assertEquals(1, hooks.get());
    assertEquals(requested.title(), STORE.get(requested.nameIdentifier()).title());
  }

  @TestTemplate
  void testDuplicateBindingInHookPreservesFailureAndRollsBack() throws IOException {
    long lake = createAndInsertMakeLake("lake").id();
    var initial = article(31, "guide");
    var fromHook = new AtomicReference<PersistenceException>();
    var failure =
        assertThrows(
            PersistenceException.class,
            () ->
                STORE.insert(
                    initial,
                    false,
                    (before, after) -> {
                      // The initial bindings already include TABLE when the hook runs.
                      var duplicate =
                          assertThrows(
                              PersistenceException.class,
                              () ->
                                  SessionUtils.doWithoutCommit(
                                      KnowledgeBaseArticleMapper.class,
                                      m -> m.bind(m.get(lake, after.name()), TABLE, "{}")));
                      fromHook.set(duplicate);
                      throw duplicate;
                    }));
    assertSame(fromHook.get(), failure);
    assertTrue(ExceptionUtils.isDuplicateKey(failure));
    assertFalse(backend.exists(initial.nameIdentifier(), TYPE));
    assertEquals(
        List.of(),
        SessionUtils.getWithoutCommit(
            KnowledgeBaseArticleMapper.class, m -> m.bindings(initial.id())));
  }

  @TestTemplate
  void testBeforeChangeReadsLockedBodyAndBindings() throws IOException {
    long lake = createAndInsertMakeLake("lake").id();
    var original = article(11, "guide");
    var ident = original.nameIdentifier();
    List<StoredContent> captured = new ArrayList<>();
    List<Set<Binding>> bindings = new ArrayList<>();
    TransactionHook hook =
        new TransactionHook() {
          @Override
          public void beforeChange(
              KnowledgeBaseArticleEntity before, KnowledgeBaseArticleEntity after) {
            assertNull(before.bodyUpdate());
            var current = STORE.get(before.nameIdentifier());
            assertEquals(before.bindings(), current.bindings());
            bindings.add(current.bindings());
            captured.add(STORE.getContent(before.nameIdentifier()));
          }

          @Override
          public void onChange(
              KnowledgeBaseArticleEntity before, KnowledgeBaseArticleEntity after) {}
        };
    STORE.insert(original, false, hook);
    STORE.update(ident, null, current -> current, hook);
    assertTrue(captured.isEmpty());

    var updatedBody = ArticleBody.of(ArticleBody.Format.MARKDOWN, "Updated");
    STORE.update(
        ident,
        null,
        current ->
            current.toBuilder().withBodyUpdate(updatedBody).withBindings(Set.of(TERM)).build(),
        hook);
    assertEquals(updatedBody, STORE.getContent(ident).body());
    var overwrittenBody = ArticleBody.of(ArticleBody.Format.HTML, "<p>Overwritten</p>");
    STORE.insert(
        original.toBuilder().withBodyUpdate(overwrittenBody).withBindings(Set.of(TABLE)).build(),
        true,
        hook);
    assertTrue(STORE.delete(ident, hook));
    assertFalse(STORE.delete(ident, hook));
    assertThrows(NoSuchEntityException.class, () -> STORE.getContent(ident));
    assertEquals(List.of(original.bindings(), Set.of(TERM), Set.of(TABLE)), bindings);
    assertEquals(
        List.of(
            new StoredContent(original.bodyUpdate(), new ExpectedRevision(lake, original.id(), 1)),
            new StoredContent(updatedBody, new ExpectedRevision(lake, original.id(), 2)),
            new StoredContent(overwrittenBody, new ExpectedRevision(lake, original.id(), 3))),
        captured);
  }

  @TestTemplate
  void testUpdateConflictsWhenBeforeChangeDeletesArticle() throws IOException {
    assertBeforeChangeConflict(ident -> assertTrue(STORE.delete(ident)));
  }

  @TestTemplate
  void testUpdateConflictsWhenBeforeChangeEditsArticle() throws IOException {
    assertBeforeChangeConflict(
        ident ->
            STORE.update(
                ident,
                (KnowledgeBaseArticleEntity current) ->
                    current.toBuilder().withTitle("Inner edit").build()));
  }

  @TestTemplate
  void testBeforeChangeFailureRollsBackExtensionWrite() throws IOException {
    createAndInsertMakeLake("lake");
    var original = article(11, "guide");
    var marker = article(12, "marker");
    STORE.insert(original, false);
    STORE.insert(marker, false);
    var ident = original.nameIdentifier();
    var initialContent = STORE.getContent(ident);
    var failure = new IllegalStateException("Cannot save old content");
    TransactionHook hook =
        new TransactionHook() {
          @Override
          public void beforeChange(
              KnowledgeBaseArticleEntity before, KnowledgeBaseArticleEntity after) {
            STORE.update(
                marker.nameIdentifier(),
                (KnowledgeBaseArticleEntity current) ->
                    current.toBuilder().withTitle("Extension write").build());
            throw failure;
          }

          @Override
          public void onChange(
              KnowledgeBaseArticleEntity before, KnowledgeBaseArticleEntity after) {
            fail("Write continued after beforeChange failed");
          }
        };
    assertSame(failure, assertThrows(IllegalStateException.class, () -> STORE.delete(ident, hook)));
    assertEquals(initialContent, STORE.getContent(ident));
    assertEquals(original.bindings(), STORE.get(ident).bindings());
    assertEquals(marker.title(), STORE.get(marker.nameIdentifier()).title());
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
  void testFailedEditDiscardsOnlyItsAfterCommitCallbacks() throws IOException {
    createAndInsertMakeLake("lake");
    var article = article(11, "guide");
    STORE.insert(article, false);
    List<String> events = new ArrayList<>();
    var failure = new IllegalStateException("extension failed");
    SessionUtils.doMultipleWithCommit(
        () -> {
          SqlSessions.afterCommit(() -> events.add("before edit"));
          assertSame(
              failure,
              assertThrows(
                  IllegalStateException.class,
                  () ->
                      STORE.update(
                          article.nameIdentifier(),
                          null,
                          current -> current.toBuilder().withTitle("new title").build(),
                          (before, after) -> {
                            SqlSessions.afterCommit(() -> events.add("failed edit"));
                            throw failure;
                          })));
          SqlSessions.afterCommit(() -> events.add("after edit"));
          assertTrue(events.isEmpty());
        });
    var stored = STORE.get(article.nameIdentifier());
    assertEquals(article.title(), stored.title());
    assertEquals(1, stored.revision());
    assertEquals(List.of("before edit", "after edit"), events);
  }

  @TestTemplate
  void testOuterSavepointRollbackDiscardsSuccessfulNestedEditCallback() throws IOException {
    createAndInsertMakeLake("lake");
    var article = article(11, "guide");
    STORE.insert(article, false);
    var name = article.nameIdentifier();
    List<String> events = new ArrayList<>();
    var failure = new IllegalStateException("outer operation failed");
    SessionUtils.doMultipleWithCommit(
        () -> {
          assertSame(
              failure,
              assertThrows(
                  IllegalStateException.class,
                  () ->
                      SqlSessions.withSavepoint(
                          () -> {
                            STORE.update(
                                name,
                                null,
                                current -> current.toBuilder().withTitle("discarded").build(),
                                (before, after) ->
                                    SqlSessions.afterCommit(() -> events.add(after.title())));
                            throw failure;
                          })));
          assertEquals(article.title(), STORE.get(name).title());
          STORE.update(
              name,
              null,
              current -> current.toBuilder().withTitle("retained").build(),
              (before, after) -> SqlSessions.afterCommit(() -> events.add(after.title())));
          assertTrue(events.isEmpty());
        });
    assertEquals(List.of("retained"), events);
    assertEquals(2, STORE.get(name).revision());
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
  void testDeleteAndGetReturnsTheArticleSelectedByDelete() throws IOException {
    createAndInsertMakeLake("lake");
    var original = article(71, "guide");
    var ident = original.nameIdentifier();
    STORE.insert(original, false);
    var store = RelationalEntityStore.forOfflineMigration(backend, new Config(false) {});
    // Recreate the name just before the delete opens its transaction, after any standalone get.
    try (MockedStatic<SessionUtils> timing =
        Mockito.mockStatic(
            SessionUtils.class,
            invocation -> {
              if (invocation.getMethod().getName().equals("doWithCommitAndFetchResult")
                  && invocation.getArgument(0) == KnowledgeBaseArticleMapper.class) {
                assertTimeoutPreemptively(
                    Duration.ofSeconds(15),
                    () -> {
                      assertTrue(STORE.delete(ident));
                      STORE.insert(article(72, "guide"), false);
                    });
              }
              return invocation.callRealMethod();
            })) {
      var deleted = store.deleteAndGet(ident, TYPE, KnowledgeBaseArticleEntity.class).orElseThrow();
      assertEquals(72L, deleted.id());
    }
    assertFalse(store.exists(ident, TYPE));
  }

  @TestTemplate
  void testDeleteAndGetPropagatesMissingDomain() throws IOException {
    long lake = createAndInsertMakeLake("lake").id();
    var article = article(71, "guide");
    var ident = article.nameIdentifier();
    STORE.insert(article, false);
    long missingDomain = 72;
    SessionUtils.doWithCommit(
        DomainMemberMapper.class,
        m -> m.add(lake, MemberType.KNOWLEDGE_BASE_ARTICLE, article.id(), missingDomain, "{}"));
    var store = RelationalEntityStore.forOfflineMigration(backend, new Config(false) {});
    var failure =
        assertThrows(
            NoSuchEntityException.class,
            () ->
                store.deleteAndGet(
                    ident,
                    TYPE,
                    KnowledgeBaseArticleEntity.class,
                    ignored -> fail("Unexpected delete callback")));
    assertTrue(failure.getMessage().contains("Domain " + missingDomain));
    var surviving = STORE.get(ident);
    assertEquals(article.id(), surviving.id());
    assertEquals(Set.of(missingDomain), surviving.domainIds());
  }

  @TestTemplate
  void testDeleteAndGetCallbackUsesDeletedSnapshot() throws IOException {
    createAndInsertMakeLake("lake");
    var original = article(71, "guide");
    var ident = original.nameIdentifier();
    STORE.insert(original, false);
    var store = RelationalEntityStore.forOfflineMigration(backend, new Config(false) {});
    var failure = new NoSuchEntityException("Callback failed");
    assertSame(
        failure,
        assertThrows(
            NoSuchEntityException.class,
            () ->
                store.deleteAndGet(
                    ident,
                    TYPE,
                    KnowledgeBaseArticleEntity.class,
                    snapshot -> {
                      assertEquals(original.id(), snapshot.id());
                      throw failure;
                    })));
    assertEquals(original.id(), STORE.get(ident).id());

    AtomicReference<KnowledgeBaseArticleEntity> callback = new AtomicReference<>();
    var deleted =
        store
            .deleteAndGet(ident, TYPE, KnowledgeBaseArticleEntity.class, callback::set)
            .orElseThrow();
    assertSame(deleted, callback.get());
    assertEquals(original.id(), deleted.id());
    for (NameIdentifier missing : List.of(ident, NameIdentifier.of("missing-lake", "guide"))) {
      assertTrue(
          store
              .deleteAndGet(
                  missing,
                  TYPE,
                  KnowledgeBaseArticleEntity.class,
                  ignored -> fail("Unexpected callback"))
              .isEmpty());
    }
  }

  @TestTemplate
  void testDeleteAndGetPreservesReplacementAfterObservation() throws IOException {
    assumeFalse("h2".equals(backendType), "H2 serializes these writers on the parent row");
    createAndInsertMakeLake("lake");
    var initial = article(71, "guide");
    var ident = initial.nameIdentifier();
    STORE.insert(initial, false);
    var store = RelationalEntityStore.forOfflineMigration(backend, new Config(false) {});
    try (MockedStatic<SessionUtils> timing =
        interleaveArticleReads(
            () -> {},
            () -> {
              assertTrue(STORE.delete(ident));
              STORE.insert(article(72, "guide"), false);
            })) {
      assertTrue(
          store
              .deleteAndGet(
                  ident,
                  TYPE,
                  KnowledgeBaseArticleEntity.class,
                  ignored -> fail("Unexpected callback"))
              .isEmpty());
    }
    assertEquals(72L, STORE.get(ident).id());
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
  void testRetriedUpdateRejectsSameNameReplacement() throws IOException {
    assumeFalse("h2".equals(backendType), "H2 serializes these writers on the parent row");
    createAndInsertMakeLake("lake");
    var original = article(71, "guide");
    var replacement = article(72, "guide");
    var ident = original.nameIdentifier();
    STORE.insert(original, false);
    AtomicInteger observations = new AtomicInteger();
    List<Long> editedIds = new ArrayList<>();
    try (MockedStatic<SessionUtils> timing =
        interleaveArticleReads(
            () -> {
              if (observations.incrementAndGet() == 2) {
                assertTrue(STORE.delete(ident));
                STORE.insert(replacement, false);
              }
            },
            () -> {
              if (observations.get() == 1) {
                STORE.update(
                    ident,
                    (KnowledgeBaseArticleEntity a) -> a.toBuilder().withTitle("Peer edit").build());
              }
            })) {
      assertThrows(
          ArticleRevisionConflictException.class,
          () ->
              STORE.update(
                  ident,
                  (KnowledgeBaseArticleEntity a) -> {
                    editedIds.add(a.id());
                    return a.toBuilder().withTitle("Requested edit").build();
                  }));
    }
    assertEquals(List.of(original.id()), editedIds);
    var actual = STORE.get(ident);
    assertEquals(replacement.id(), actual.id());
    assertEquals(replacement.title(), actual.title());
    assertEquals(1L, actual.revision());
  }

  @TestTemplate
  void testDeadlockVictimCannotCommitLaterWrites() throws Exception {
    assumeTrue("mysql".equals(backendType), "InnoDB rolls back the entire deadlock victim");
    createAndInsertMakeLake("lake");
    var first = article(71, "first");
    var second = article(72, "second");
    var firstMarker = article(73, "first-marker");
    var secondMarker = article(74, "second-marker");
    for (var article : List.of(first, second, firstMarker, secondMarker)) {
      STORE.insert(article, false);
    }
    var firstLocked = new CompletableFuture<Void>();
    var secondLocked = new CompletableFuture<Void>();
    var firstCommit =
        CompletableFuture.supplyAsync(
            () -> commitWithCompetingEdit(first, second, firstMarker, firstLocked, secondLocked));
    var secondCommit =
        CompletableFuture.supplyAsync(
            () -> commitWithCompetingEdit(second, first, secondMarker, secondLocked, firstLocked));
    CompletableFuture.allOf(firstCommit, secondCommit).get(30, TimeUnit.SECONDS);
    assertEquals(!firstCommit.join(), secondCommit.join());
    var winner = firstCommit.join() ? first : second;
    var victim = firstCommit.join() ? second : first;
    var winningMarker = firstCommit.join() ? firstMarker : secondMarker;
    var losingMarker = firstCommit.join() ? secondMarker : firstMarker;
    assertEquals("Before", STORE.get(winner.nameIdentifier()).title());
    assertEquals("After", STORE.get(winningMarker.nameIdentifier()).title());
    assertEquals(victim.title(), STORE.get(victim.nameIdentifier()).title());
    assertEquals(losingMarker.title(), STORE.get(losingMarker.nameIdentifier()).title());
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

  @TestTemplate
  void testPurgeSelectsOldestRetirementFirst() throws IOException {
    createAndInsertMakeLake("lake");
    var newer = article(31, "newer").toBuilder().withBindings(Set.of(TABLE)).build();
    var older = article(32, "older").toBuilder().withBindings(Set.of(TABLE)).build();
    STORE.insert(newer, false);
    STORE.insert(older, false);
    SessionUtils.doWithCommit(
        KnowledgeBaseArticleMapper.class,
        mapper -> {
          mapper.deleteBindings(newer.id(), 20);
          mapper.delete(newer.id(), newer.revision(), 20);
          mapper.deleteBindings(older.id(), 10);
          mapper.delete(older.id(), older.revision(), 10);

          assertEquals(1, mapper.purgeBindings(21, 1));
          assertEquals(0, mapper.purgeBindings(20, 1));
          assertEquals(1, mapper.purge(21, 1));
          assertEquals(0, mapper.purge(20, 1));
        });
  }

  @TestTemplate
  void testBindingAuditRecordsEachActorAndInsertionTime() throws Exception {
    createAndInsertMakeLake("lake");
    AuditInfo articleAudit =
        AuditInfo.builder().withCreator("alice").withCreateTime(Instant.EPOCH).build();
    var entity =
        article(31, "guide").toBuilder()
            .withAuditInfo(articleAudit)
            .withBindings(Set.of(TABLE))
            .build();
    long insertStart = System.currentTimeMillis();
    PrincipalUtils.doAs(
        new UserPrincipal("alice"),
        () -> {
          STORE.insert(entity, false);
          return null;
        });
    AuditInfo initial = bindingAudit(entity.id(), TABLE);
    assertEquals("alice", initial.creator());
    assertTrue(initial.createTime().toEpochMilli() >= insertStart);
    assertTrue(initial.createTime().toEpochMilli() <= System.currentTimeMillis());

    long editStart = System.currentTimeMillis();
    PrincipalUtils.doAs(
        new UserPrincipal("bob"),
        () ->
            STORE.update(
                entity.nameIdentifier(),
                (KnowledgeBaseArticleEntity a) ->
                    a.toBuilder().withBindings(Set.of(TABLE, TERM)).build()));
    AuditInfo added = bindingAudit(entity.id(), TERM);
    assertEquals("bob", added.creator());
    assertTrue(added.createTime().toEpochMilli() >= editStart);
    assertTrue(added.createTime().toEpochMilli() <= System.currentTimeMillis());
    assertEquals(initial, bindingAudit(entity.id(), TABLE));
    assertEquals(articleAudit, STORE.get(entity.nameIdentifier()).auditInfo());
  }

  private static boolean commitWithCompetingEdit(
      KnowledgeBaseArticleEntity first,
      KnowledgeBaseArticleEntity second,
      KnowledgeBaseArticleEntity marker,
      CompletableFuture<Void> locked,
      CompletableFuture<Void> peerLocked) {
    AtomicReference<RuntimeException> deadlock = new AtomicReference<>();
    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            STORE.update(
                first.nameIdentifier(),
                (KnowledgeBaseArticleEntity a) -> a.toBuilder().withTitle("Before").build());
            locked.complete(null);
            peerLocked.orTimeout(15, TimeUnit.SECONDS).join();
            try {
              STORE.update(second.nameIdentifier(), (KnowledgeBaseArticleEntity a) -> a);
            } catch (RuntimeException e) {
              assertTrue(ExceptionUtils.hasSqlException(e, Set.of(), Set.of(1213)), e.toString());
              deadlock.set(e);
            }
            STORE.update(
                marker.nameIdentifier(),
                (KnowledgeBaseArticleEntity a) -> a.toBuilder().withTitle("After").build());
          });
      assertNull(deadlock.get(), "A transaction aborted by InnoDB reported commit success");
      return true;
    } catch (IllegalStateException e) {
      assertTrue(deadlock.get() != null);
      assertSame(deadlock.get(), e.getCause());
      return false;
    }
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

  private static AuditInfo bindingAudit(long articleId, Binding binding)
      throws SQLException, IOException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        PreparedStatement query =
            session
                .getConnection()
                .prepareStatement(
                    "SELECT audit_info FROM kb_article_relation_meta WHERE article_id=? AND target_type=? AND target_id=? AND deleted_at=0")) {
      query.setLong(1, articleId);
      query.setString(2, binding.targetType());
      query.setLong(3, binding.targetId());
      try (ResultSet rows = query.executeQuery()) {
        assertTrue(rows.next());
        return JsonUtils.anyFieldMapper().readValue(rows.getString(1), AuditInfo.class);
      }
    }
  }

  private void assertBeforeChangeConflict(Consumer<NameIdentifier> change) throws IOException {
    createAndInsertMakeLake("lake");
    var original = article(11, "guide");
    var ident = original.nameIdentifier();
    STORE.insert(original, false);
    var initialContent = STORE.getContent(ident);
    TransactionHook hook =
        new TransactionHook() {
          @Override
          public void beforeChange(
              KnowledgeBaseArticleEntity before, KnowledgeBaseArticleEntity after) {
            change.accept(ident);
          }

          @Override
          public void onChange(
              KnowledgeBaseArticleEntity before, KnowledgeBaseArticleEntity after) {
            fail("onChange ran after the Article changed in beforeChange");
          }
        };
    assertThrows(
        ArticleRevisionConflictException.class,
        () ->
            STORE.update(
                ident,
                null,
                current ->
                    current.toBuilder().withTitle("Outer edit").withBindings(Set.of()).build(),
                hook));
    var restored = STORE.get(ident);
    assertEquals(original.title(), restored.title());
    assertEquals(original.bindings(), restored.bindings());
    assertEquals(initialContent, STORE.getContent(ident));
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
