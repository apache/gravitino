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

import static org.apache.gravitino.storage.relational.mapper.DomainMemberMapper.MemberType.GLOSSARY_TERM;
import static org.apache.gravitino.storage.relational.mapper.DomainMemberMapper.MemberType.KNOWLEDGE_BASE_ARTICLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.ArticleRevisionConflictException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyDomainException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.kb.ArticleBody;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.DomainMemberMapper;
import org.apache.gravitino.storage.relational.mapper.DomainMemberMapper.Membership;
import org.apache.gravitino.storage.relational.mapper.KnowledgeBaseArticleMapper;
import org.apache.gravitino.storage.relational.po.KnowledgeBaseArticlePO;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService.TransactionHook;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.TestTemplate;

/** Shared Domain membership storage contracts on each relational backend. */
public class TestArticleDomainMembership extends TestJDBCBackend {
  private static final KnowledgeBaseArticleMetaService ARTICLES =
      KnowledgeBaseArticleMetaService.getInstance();

  @TestTemplate
  void testJoinedLookupsReturnWideMetadataOncePerArticle() throws IOException {
    createAndInsertMakeLake("lake");
    var domain = domain("lake", 20);
    var bindings =
        LongStream.range(0, 64)
            .mapToObj(id -> new Binding("TABLE", id + 100))
            .collect(Collectors.toSet());
    var wide =
        article(Set.of(domain.id())).toBuilder()
            .withProperties(Map.of("payload", "x".repeat(65536)))
            .withBindings(bindings)
            .build();
    var plain = article(Set.of()).toBuilder().withId(61L).withName("plain").build();
    ARTICLES.insert(wide, false);
    ARTICLES.insert(plain, false);
    SessionUtils.doMultipleWithCommit(
        () -> {
          SessionUtils.doWithoutCommit(
              KnowledgeBaseArticleMapper.class,
              mapper -> {
                for (var rows :
                    List.of(
                        mapper.joined("lake", "guide", null, true),
                        mapper.joinedBatch("lake", List.of("guide", "absent")))) {
                  assertEquals(1, rows.stream().filter(row -> row.getProperties() != null).count());
                  assertEquals(66, rows.size());
                  assertEquals(64, rows.stream().filter(row -> row.getTargetId() != null).count());
                  assertEquals(
                      List.of(domain.id()),
                      rows.stream()
                          .map(KnowledgeBaseArticlePO::getDomainId)
                          .filter(Objects::nonNull)
                          .toList());
                }
              });
          assertEquals(wide, ARTICLES.get(wide.nameIdentifier()));
          assertEquals(List.of(wide, plain), ARTICLES.list(wide.namespace()));
          assertEquals(
              List.of(plain, wide, plain),
              ARTICLES.batchGet(
                  List.of(plain.nameIdentifier(), wide.nameIdentifier(), plain.nameIdentifier())));
        });
  }

  @TestTemplate
  void testTypedMembershipLifecycle() throws IOException {
    long lake = createAndInsertMakeLake("lake").id();
    DomainEntity domain = domain("lake", 20);
    DomainEntity other = domain("lake", 21);
    KnowledgeBaseArticleEntity article = article(Set.of(domain.id(), other.id()));
    ARTICLES.insert(article, false);
    assertEquals(article, ARTICLES.get(article.nameIdentifier()));
    assertEquals(List.of(article), ARTICLES.list(article.namespace()));

    // The Glossary branch supplies the Term writer. Exercise its side of the shared key.
    SessionUtils.doWithCommit(
        DomainMemberMapper.class, m -> m.add(lake, GLOSSARY_TERM, article.id(), domain.id(), "{}"));
    assertEquals(
        List.of(
            new Membership(GLOSSARY_TERM, article.id(), domain.id()),
            new Membership(KNOWLEDGE_BASE_ARTICLE, article.id(), domain.id())),
        members(lake, domain.id()));
    assertThrows(
        NonEmptyDomainException.class,
        () -> DomainMetaService.getInstance().deleteDomain(domain.nameIdentifier()));
    KnowledgeBaseArticleEntity edited =
        ARTICLES.update(
            article.nameIdentifier(),
            (KnowledgeBaseArticleEntity a) ->
                a.toBuilder().withDomainIds(Set.of(domain.id())).withTitle("Edited").build());
    assertEquals(2, edited.revision());
    ARTICLES.insert(edited, true);
    assertEquals(2, ARTICLES.get(article.nameIdentifier()).revision());
    assertTrue(DomainMetaService.getInstance().deleteDomain(other.nameIdentifier()));
    DomainEntity renamed =
        DomainMetaService.getInstance()
            .updateDomain(
                domain.nameIdentifier(),
                (DomainEntity d) -> d.toBuilder().withName("renamed").build());
    assertEquals(Set.of(renamed.id()), ARTICLES.get(article.nameIdentifier()).domainIds());
    assertTrue(ARTICLES.delete(article.nameIdentifier()));
    ARTICLES.insert(article(Set.of()).toBuilder().withId(61L).build(), false);
    assertTrue(ARTICLES.get(article.nameIdentifier()).domainIds().isEmpty());
    assertEquals(
        List.of(new Membership(GLOSSARY_TERM, article.id(), renamed.id())),
        members(lake, renamed.id()));
    assertThrows(
        NonEmptyDomainException.class,
        () -> DomainMetaService.getInstance().deleteDomain(renamed.nameIdentifier()));
    assertTrue(backend.delete(NameIdentifier.of("lake"), EntityType.METALAKE, true));
    assertTrue(members(lake, domain.id()).isEmpty());
    DomainMetaService.getInstance().deleteDomainMetasByLegacyTimeline(Long.MAX_VALUE, 100);
    assertEquals(
        0,
        SessionUtils.getWithoutCommit(DomainMemberMapper.class, m -> m.purge(Long.MAX_VALUE, 100))
            .intValue());
  }

  @TestTemplate
  void testDeleteConflictsWhenBeforeChangeAddsDomain() throws IOException {
    long lake = createAndInsertMakeLake("lake").id();
    var domain = domain("lake", 20);
    var original = article(Set.of());
    var ident = original.nameIdentifier();
    ARTICLES.insert(original, false);
    TransactionHook hook =
        new TransactionHook() {
          @Override
          public void beforeChange(
              KnowledgeBaseArticleEntity before, KnowledgeBaseArticleEntity after) {
            ARTICLES.update(
                ident,
                (KnowledgeBaseArticleEntity a) ->
                    a.toBuilder().withDomainIds(Set.of(domain.id())).build());
          }

          @Override
          public void onChange(
              KnowledgeBaseArticleEntity before, KnowledgeBaseArticleEntity after) {
            fail("Deletion continued after the Article revision changed");
          }
        };

    assertThrows(ArticleRevisionConflictException.class, () -> ARTICLES.delete(ident, hook));
    assertThrows(ArticleRevisionConflictException.class, () -> ARTICLES.deleteAndGet(ident, hook));
    assertEquals(original, ARTICLES.get(ident));
    assertTrue(members(lake, domain.id()).isEmpty());
    assertTrue(DomainMetaService.getInstance().deleteDomain(domain.nameIdentifier()));
  }

  @TestTemplate
  void testReattachmentAfterRetainedGeneration() throws Exception {
    long lake = createAndInsertMakeLake("lake").id();
    long domain = domain("lake", 20).id();
    KnowledgeBaseArticleEntity article = article(Set.of(domain));
    ARTICLES.insert(article, false);
    long retained = Long.MAX_VALUE - 2;
    SessionUtils.doWithCommit(
        DomainMemberMapper.class,
        m -> m.retire(lake, KNOWLEDGE_BASE_ARTICLE, article.id(), domain, retained));

    PrincipalUtils.doAs(
        new UserPrincipal("editor"),
        () ->
            ARTICLES.update(
                article.nameIdentifier(),
                (KnowledgeBaseArticleEntity a) ->
                    a.toBuilder().withDomainIds(Set.of(domain)).build()));
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        PreparedStatement query =
            session
                .getConnection()
                .prepareStatement(
                    "SELECT audit_info FROM domain_member_rel WHERE member_type='KNOWLEDGE_BASE_ARTICLE' AND member_id=? AND domain_id=? AND deleted_at=0")) {
      query.setLong(1, article.id());
      query.setLong(2, domain);
      try (ResultSet rows = query.executeQuery()) {
        assertTrue(rows.next());
        assertEquals(
            "editor",
            JsonUtils.anyFieldMapper().readValue(rows.getString(1), AuditInfo.class).creator());
      }
    }
    assertTrue(ARTICLES.delete(article.nameIdentifier()));
    assertEquals(
        retained + 1,
        SessionUtils.getWithoutCommit(
                DomainMemberMapper.class,
                m -> m.lastRetirement(lake, KNOWLEDGE_BASE_ARTICLE, article.id()))
            .longValue());
  }

  @TestTemplate
  void testInvalidDomainAndExtensionFailureRollBack() throws IOException {
    long lake = createAndInsertMakeLake("lake").id();
    createAndInsertMakeLake("elsewhere");
    DomainEntity foreign = domain("elsewhere", 21);
    DomainEntity local = domain("lake", 20);
    KnowledgeBaseArticleEntity original = article(Set.of(local.id()));
    assertThrows(
        NoSuchEntityException.class, () -> ARTICLES.insert(article(Set.of(foreign.id())), false));
    assertThrows(NoSuchEntityException.class, () -> ARTICLES.insert(article(Set.of(999L)), false));
    ARTICLES.insert(original, false);
    assertThrows(
        NoSuchEntityException.class,
        () ->
            ARTICLES.update(
                original.nameIdentifier(),
                (KnowledgeBaseArticleEntity a) ->
                    a.toBuilder().withDomainIds(Set.of(foreign.id())).build()));
    assertThrows(
        IllegalStateException.class,
        () ->
            ARTICLES.update(
                original.nameIdentifier(),
                null,
                a -> a.toBuilder().withDomainIds(Set.of()).withTitle("Replaced").build(),
                (before, after) -> {
                  assertEquals(original, before);
                  assertEquals(after, ARTICLES.get(original.nameIdentifier()));
                  throw new IllegalStateException("failed extension");
                }));
    assertEquals(original, ARTICLES.get(original.nameIdentifier()));
    assertEquals(1, members(lake, local.id()).size());
  }

  @TestTemplate
  void testMembershipAndRevisionShareOneSnapshot() throws IOException {
    long lake = createAndInsertMakeLake("lake").id();
    DomainEntity first = domain("lake", 20);
    DomainEntity second = domain("lake", 21);
    KnowledgeBaseArticleEntity article = article(Set.of(first.id()));
    ARTICLES.insert(article, false);
    SqlSessions.readSnapshot(
        () -> {
          assertEquals(
              1,
              SessionUtils.getWithoutCommit(
                      KnowledgeBaseArticleMapper.class,
                      m -> m.get(lake, article.name()).getRevision())
                  .longValue());
          CompletableFuture.runAsync(
                  () ->
                      ARTICLES.update(
                          article.nameIdentifier(),
                          (KnowledgeBaseArticleEntity a) ->
                              a.toBuilder().withDomainIds(Set.of(second.id())).build()))
              .orTimeout(15, TimeUnit.SECONDS)
              .join();
          assertEquals(
              List.of(first.id()),
              SessionUtils.getWithoutCommit(
                  DomainMemberMapper.class,
                  m -> m.domains(lake, KNOWLEDGE_BASE_ARTICLE, article.id())));
          return null;
        });
    KnowledgeBaseArticleEntity latest = ARTICLES.get(article.nameIdentifier());
    assertEquals(Set.of(second.id()), latest.domainIds());
    assertEquals(2, latest.revision());
  }

  @TestTemplate
  void testAddCommitsBeforeDrop() throws Exception {
    createAndInsertMakeLake("lake");
    DomainEntity domain = domain("lake", 20);
    KnowledgeBaseArticleEntity article = article(Set.of());
    ARTICLES.insert(article, false);
    CountDownLatch written = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CompletableFuture<?> add =
        CompletableFuture.runAsync(
            () ->
                ARTICLES.update(
                    article.nameIdentifier(),
                    null,
                    a -> a.toBuilder().withDomainIds(Set.of(domain.id())).build(),
                    (old, next) -> {
                      written.countDown();
                      await(release);
                    }));
    await(written);
    CompletableFuture<?> drop =
        CompletableFuture.runAsync(
            () ->
                assertThrows(
                    NonEmptyDomainException.class,
                    () -> DomainMetaService.getInstance().deleteDomain(domain.nameIdentifier())));
    try {
      assertThrows(TimeoutException.class, () -> drop.get(150, TimeUnit.MILLISECONDS));
    } finally {
      release.countDown();
    }
    CompletableFuture.allOf(add, drop).get(15, TimeUnit.SECONDS);
    assertEquals(Set.of(domain.id()), ARTICLES.get(article.nameIdentifier()).domainIds());
  }

  @TestTemplate
  void testDropCommitsBeforeAdd() throws Exception {
    createAndInsertMakeLake("lake");
    DomainEntity domain = domain("lake", 20);
    KnowledgeBaseArticleEntity article = article(Set.of());
    ARTICLES.insert(article, false);
    CountDownLatch locked = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CompletableFuture<Boolean> drop =
        CompletableFuture.supplyAsync(
            () ->
                DomainMetaService.getInstance()
                    .deleteDomain(
                        domain.nameIdentifier(),
                        d -> {
                          locked.countDown();
                          await(release);
                        }));
    await(locked);
    CompletableFuture<?> add =
        CompletableFuture.runAsync(
            () ->
                assertThrows(
                    NoSuchEntityException.class,
                    () ->
                        ARTICLES.update(
                            article.nameIdentifier(),
                            (KnowledgeBaseArticleEntity a) ->
                                a.toBuilder().withDomainIds(Set.of(domain.id())).build())));
    try {
      assertThrows(TimeoutException.class, () -> add.get(150, TimeUnit.MILLISECONDS));
    } finally {
      release.countDown();
    }
    assertTrue(drop.get(15, TimeUnit.SECONDS));
    add.get(15, TimeUnit.SECONDS);
    assertEquals(article, ARTICLES.get(article.nameIdentifier()));
  }

  private static List<Membership> members(long lake, long domain) {
    return SessionUtils.getWithoutCommit(
        DomainMemberMapper.class, m -> m.members(lake, domain, 20, 0));
  }

  private DomainEntity domain(String lake, long id) throws IOException {
    DomainEntity domain =
        DomainEntity.builder()
            .withNamespace(NamespaceUtil.ofDomain(lake))
            .withName("domain" + id)
            .withStatus(Domain.Status.DRAFT)
            .withId(id)
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(domain, false);
    return domain;
  }

  private static KnowledgeBaseArticleEntity article(Set<Long> domains) {
    return KnowledgeBaseArticleEntity.builder()
        .withNamespace(Namespace.of("lake"))
        .withId(60L)
        .withName("guide")
        .withTitle("Guide")
        .withBodyUpdate(ArticleBody.of(ArticleBody.Format.MARKDOWN, "Body"))
        .withDomainIds(domains)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(15, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
