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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.ArticleRevisionConflictException;
import org.apache.gravitino.kb.ArticleBody;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.KnowledgeBaseArticleMapper;
import org.apache.gravitino.storage.relational.po.KnowledgeBaseArticlePO;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService.ExpectedRevision;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.TestTemplate;

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
            .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, body))
            .build();
    backend.insert(entity, false);
    NameIdentifier nfd = NameIdentifier.of("lake", "𠀀".repeat(127) + "e\u0301");
    assertEquals(entity, backend.get(nfd, TYPE));
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

  private static KnowledgeBaseArticleEntity article(long id, String name) {
    return KnowledgeBaseArticleEntity.builder()
        .withId(id)
        .withNamespace(Namespace.of("lake"))
        .withName(name)
        .withTitle("Title")
        .withBody(ArticleBody.of(ArticleBody.Format.HTML, "<p>Body</p>"))
        .withProperties(Map.of("lang", "中文"))
        .withSynonyms(List.of("Article", "alternative"))
        .withBindings(Set.of(TABLE, TERM))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }
}
