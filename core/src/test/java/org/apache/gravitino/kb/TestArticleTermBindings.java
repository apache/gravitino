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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.NoSuchKnowledgeBaseArticleException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.UnapprovedTermException;
import org.apache.gravitino.glossary.GlossaryManager;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.glossary.TermChange;
import org.apache.gravitino.glossary.TermSpec;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

class TestArticleTermBindings extends TestJDBCBackend {
  private static final String LAKE = "lake";
  private final AtomicLong ids = new AtomicLong(100);
  private final GlossaryManager glossary = new GlossaryManager(ids::incrementAndGet, false);
  private final ArticleManager articles = new ArticleManager(ids::incrementAndGet, 1024);

  @TestTemplate
  void mixedEditsCommitOnceAndPreserveOtherBindings() throws Exception {
    createAndInsertMakeLake(LAKE);
    insertDomain(LAKE, "Sales", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    term("Net Revenue");
    term("Cost");
    glossary.createTerm(
        LAKE, TermSpec.builder("Draft").withStatus(GlossaryTerm.Status.DRAFT).build());
    assertThrows(
        UnapprovedTermException.class,
        () -> create("invalid", ArticleStatus.PUBLISHED, "Net Revenue", "Draft"));
    assertThrows(
        NoSuchKnowledgeBaseArticleException.class, () -> articles.loadArticle(LAKE, "invalid"));
    var created = create("guide", ArticleStatus.PUBLISHED, " net  revenue ", "NET REVENUE");
    assertEquals(
        List.of("Net Revenue"),
        created.article().terms().stream().map(ArticleTermBinding::name).toList());
    assertEquals(
        created.revisionToken(),
        articles
            .alterArticle(
                LAKE,
                "guide",
                null,
                KnowledgeBaseArticleChange.addTerms("Net Revenue"),
                KnowledgeBaseArticleChange.removeTerms("Cost"))
            .revisionToken());
    var body =
        KnowledgeBaseArticleChange.updateBody(
            ArticleBody.of(ArticleBody.Format.MARKDOWN, "changed"));
    var domains = KnowledgeBaseArticleChange.updateDomains(new String[] {"Sales"});
    assertThrows(
        NoSuchTermException.class,
        () ->
            articles.alterArticle(
                LAKE,
                "guide",
                created.revisionToken(),
                body,
                domains,
                KnowledgeBaseArticleChange.addTerms("Cost", "Missing")));
    assertEquals(created.revisionToken(), articles.loadArticle(LAKE, "guide").revisionToken());
    assertEquals("body", articles.getArticleContent(LAKE, "guide").body().content());
    assertTrue(articles.loadArticle(LAKE, "guide").article().domains().isEmpty());
    var edited =
        articles.alterArticle(
            LAKE,
            "guide",
            created.revisionToken(),
            body,
            domains,
            KnowledgeBaseArticleChange.removeTerms("Net Revenue"),
            KnowledgeBaseArticleChange.addTerms("Cost"));
    assertEquals(2, edited.article().revision());
    assertEquals(List.of("Sales"), edited.article().domains());
    assertEquals("Cost", edited.article().terms().get(0).name());
    assertEquals("changed", articles.getArticleContent(LAKE, "guide").body().content());
    assertEquals(
        edited.revisionToken(),
        articles
            .alterArticle(
                LAKE,
                "guide",
                null,
                KnowledgeBaseArticleChange.removeTerms("Cost"),
                KnowledgeBaseArticleChange.addTerms("Cost"))
            .revisionToken());
  }

  @TestTemplate
  void cancellingEditsOnlyRequireApprovalForNewBindings() throws Exception {
    createAndInsertMakeLake(LAKE);
    term("Existing");
    term("Other");
    var created = create("guide", ArticleStatus.PUBLISHED, "Existing");
    for (String name : List.of("Existing", "Other")) {
      glossary.alterTerm(LAKE, name, TermChange.setStatus(GlossaryTerm.Status.DEPRECATED));
    }
    var edited =
        articles.alterArticle(
            LAKE,
            "guide",
            created.revisionToken(),
            KnowledgeBaseArticleChange.removeTerms("Existing"),
            KnowledgeBaseArticleChange.addTerms("Other", "existing"),
            KnowledgeBaseArticleChange.removeTerms("OTHER"));
    assertEquals(created.revisionToken(), edited.revisionToken());
    assertEquals(
        List.of("Existing"),
        edited.article().terms().stream().map(ArticleTermBinding::name).toList());
    assertThrows(
        UnapprovedTermException.class,
        () ->
            articles.alterArticle(
                LAKE,
                "guide",
                edited.revisionToken(),
                KnowledgeBaseArticleChange.removeTerms("Existing"),
                KnowledgeBaseArticleChange.addTerms("Other")));
    assertEquals(created.revisionToken(), articles.loadArticle(LAKE, "guide").revisionToken());
  }

  @TestTemplate
  void termPagesFollowLiveIdsAndReversePagesFilterBeforeOffset() throws Exception {
    createAndInsertMakeLake(LAKE);
    term("Zed");
    term("Alpha");
    create("b", ArticleStatus.PUBLISHED, "Alpha", "Zed");
    create("a", ArticleStatus.PUBLISHED, "Zed");
    create("old", ArticleStatus.DEPRECATED, "Zed");
    create("draft", ArticleStatus.DRAFT, "Zed");
    var first = articles.listTermBindings(LAKE, "b", 1, 0);
    assertEquals("Zed", first.terms().get(0).name());
    assertTrue(first.hasMore());
    assertEquals("Alpha", articles.loadArticle(LAKE, "b", 1, 1).article().terms().get(0).name());
    assertFalse(articles.listTermBindings(LAKE, "b", 1, 1).hasMore());
    assertTrue(articles.listTermBindings(LAKE, "b", 1, Integer.MAX_VALUE).terms().isEmpty());
    var reverse = articles.listArticlesForTerm(LAKE, "Zed", false, 1, 0);
    assertEquals("a", reverse.articles().get(0).name());
    assertTrue(reverse.hasMore());
    assertEquals(
        "b", articles.listArticlesForTerm(LAKE, "Zed", false, 1, 1).articles().get(0).name());
    assertFalse(articles.listArticlesForTerm(LAKE, "Zed", false, 1, 1).hasMore());
    assertEquals(
        List.of("a", "b", "old"),
        articles.listArticlesForTerm(LAKE, "Zed", true, 10, 0).articles().stream()
            .map(ArticleSummary::name)
            .toList());
    var token = articles.loadArticle(LAKE, "b").revisionToken();
    glossary.alterTerm(
        LAKE,
        "Zed",
        TermChange.rename("Revenue"),
        TermChange.setStatus(GlossaryTerm.Status.DEPRECATED));
    assertEquals(
        token,
        articles
            .alterArticle(LAKE, "b", token, KnowledgeBaseArticleChange.addTerms("Revenue"))
            .revisionToken());
    var current = articles.listTermBindings(LAKE, "b", 1, 0).terms().get(0);
    assertEquals("Revenue", current.name());
    assertEquals(ArticleTermBinding.Status.DEPRECATED, current.status());
    assertThrows(
        UnapprovedTermException.class, () -> create("new", ArticleStatus.PUBLISHED, "Revenue"));
    assertTrue(glossary.dropTerm(LAKE, "Revenue"));
    term("Revenue");
    assertTrue(articles.listArticlesForTerm(LAKE, "Revenue", true, 10, 0).articles().isEmpty());
    var live = articles.listTermBindings(LAKE, "b", 1, 0);
    assertEquals("Alpha", live.terms().get(0).name());
    assertFalse(live.hasMore());
    assertEquals(token, articles.loadArticle(LAKE, "b").revisionToken());
    assertThrows(IllegalArgumentException.class, () -> articles.listTermBindings(LAKE, "b", 0, 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> articles.listArticlesForTerm(LAKE, "Revenue", true, 1, -1));
  }

  @TestTemplate
  void previewsAndContinuationSpanMultipleStorageBatches() throws Exception {
    createAndInsertMakeLake(LAKE);
    String[] names =
        IntStream.range(0, 130).mapToObj(index -> "term-" + index).toArray(String[]::new);
    for (String name : names) {
      term(name);
    }
    var created = create("guide", ArticleStatus.PUBLISHED, names);
    assertEquals(ArticleListFilter.DEFAULT_LIMIT, created.article().terms().size());
    assertTrue(created.article().termsHasMore());
    var last = articles.listTermBindings(LAKE, "guide", 2, 128);
    assertEquals(
        List.of(names[128], names[129]),
        last.terms().stream().map(ArticleTermBinding::name).toList());
    assertFalse(last.hasMore());
    assertTrue(articles.listTermBindings(LAKE, "guide", 1, 128).hasMore());
    var updated =
        articles.alterArticle(
            LAKE,
            "guide",
            created.revisionToken(),
            KnowledgeBaseArticleChange.updateTitle("Edited"));
    assertTrue(updated.article().termsHasMore());
    assertEquals(
        List.of(names[129]),
        articles.loadArticle(LAKE, "guide", 1, 129).article().terms().stream()
            .map(ArticleTermBinding::name)
            .toList());
    insertDomain(LAKE, "Sales", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    create(
        "second", ArticleStatus.PUBLISHED, Arrays.copyOf(names, ArticleListFilter.DEFAULT_LIMIT));
    articles.alterArticle(
        LAKE, "second", null, KnowledgeBaseArticleChange.updateDomains(new String[] {"Sales"}));
    var summaries = articles.listArticlesForTerm(LAKE, names[0], false, 2, 0).articles();
    assertEquals(List.of("guide", "second"), summaries.stream().map(ArticleSummary::name).toList());
    var second = summaries.get(1);
    assertEquals(List.of("Sales"), List.of(second.domains()));
    assertEquals(ArticleListFilter.DEFAULT_LIMIT, second.terms().length);
    assertEquals(
        names[ArticleListFilter.DEFAULT_LIMIT - 1],
        second.terms()[ArticleListFilter.DEFAULT_LIMIT - 1].name());
    assertFalse(second.termsHasMore());
    var summary = summaries.get(0);
    assertTrue(List.of(summary.domains()).isEmpty());
    assertEquals(ArticleListFilter.DEFAULT_LIMIT, summary.terms().length);
    assertTrue(summary.termsHasMore());
  }

  @TestTemplate
  void revalidateApprovalAndIdentityBetweenResolutionAndLock() throws Exception {
    createAndInsertMakeLake(LAKE);
    var initial = create("guide", ArticleStatus.PUBLISHED);
    var service = GlossaryTermMetaService.getInstance();
    var peer = Executors.newSingleThreadExecutor();
    try {
      for (boolean replace : List.of(false, true)) {
        String name = replace ? "Recreated" : "Demoted";
        term(name);
        var intercepted =
            Mockito.mock(GlossaryTermMetaService.class, AdditionalAnswers.delegatesTo(service));
        Mockito.doAnswer(
                call -> {
                  var ids = service.getIdsByKeys(call.getArgument(0), call.getArgument(1));
                  peer.submit(
                          () -> {
                            if (replace) {
                              assertTrue(glossary.dropTerm(LAKE, name));
                              term(name);
                            } else {
                              glossary.alterTerm(
                                  LAKE, name, TermChange.setStatus(GlossaryTerm.Status.DEPRECATED));
                            }
                          })
                      .get(20, TimeUnit.SECONDS);
                  return ids;
                })
            .when(intercepted)
            .getIdsByKeys(Mockito.anyLong(), Mockito.anySet());
        try (var services = Mockito.mockStatic(GlossaryTermMetaService.class)) {
          services.when(GlossaryTermMetaService::getInstance).thenReturn(intercepted);
          Class<? extends RuntimeException> expected =
              replace ? NoSuchTermException.class : UnapprovedTermException.class;
          assertThrows(
              expected,
              () ->
                  articles.alterArticle(
                      LAKE,
                      "guide",
                      initial.revisionToken(),
                      KnowledgeBaseArticleChange.updateBody(
                          ArticleBody.of(ArticleBody.Format.MARKDOWN, "bad")),
                      KnowledgeBaseArticleChange.addTerms(name)));
        }
        assertEquals(initial.revisionToken(), articles.loadArticle(LAKE, "guide").revisionToken());
        assertEquals("body", articles.getArticleContent(LAKE, "guide").body().content());
        assertTrue(articles.listTermBindings(LAKE, "guide", 20, 0).terms().isEmpty());
      }
    } finally {
      peer.shutdownNow();
    }
  }

  private void term(String name) {
    glossary.createTerm(LAKE, TermSpec.builder(name).build());
  }

  private ArticleManager.Result create(String name, ArticleStatus status, String... terms) {
    return articles.createArticle(
        LAKE,
        ArticleCreate.builder()
            .withName(name)
            .withTitle("Guide")
            .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "body"))
            .withStatus(status)
            .addTerms(terms)
            .build());
  }
}
