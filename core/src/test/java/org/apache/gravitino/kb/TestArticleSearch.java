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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.glossary.GlossaryManager;
import org.apache.gravitino.glossary.TermSpec;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.junit.jupiter.api.TestTemplate;

class TestArticleSearch extends TestJDBCBackend {
  private static final String LAKE = "articles";
  private final AtomicLong ids = new AtomicLong(100);
  private final ArticleManager manager = new ArticleManager(ids::incrementAndGet, 1024);
  private final GlossaryManager glossary = new GlossaryManager(ids::incrementAndGet, false);

  @TestTemplate
  void searchesCurrentTitleIndividualSynonymsAndStoredMarkupLiterally() throws Exception {
    createAndInsertMakeLake(LAKE);
    var original =
        manager.createArticle(
            LAKE,
            article("guide", "Operations Guide")
                .addSynonyms("Profit", "Rate!%_\\\"Entry", "before\0after", "left", "right")
                .withBody(
                    ArticleBody.of(
                        ArticleBody.Format.HTML, "<a href='https://example.org/Old'>Revenue</a>"))
                .withComment("comment-only")
                .build());
    manager.createArticle(
        LAKE,
        article("decoy", "Operations").addSynonyms("Rate!XYEntry", "before\\u0000after").build());
    for (String query :
        List.of("s GuI", "roFi", "!%_\\\"e", "%", "_", "fore\0af", "Example.org/oLD")) {
      assertEquals(List.of("guide"), names(search(query)), query);
    }
    assertEquals(List.of("decoy", "guide"), names(search("AFTER")));
    assertEquals(List.of("decoy"), names(search("fore\\u0000af")));
    for (String query : List.of("left\",\"right", "comment-only", "not-found", "left\"")) {
      assertTrue(search(query).articles().isEmpty(), query);
    }

    manager.alterArticle(
        LAKE,
        "guide",
        original.revisionToken(),
        KnowledgeBaseArticleChange.updateTitle("Runbook"),
        KnowledgeBaseArticleChange.updateSynonyms(new String[] {"Margin"}),
        KnowledgeBaseArticleChange.updateBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "New")));
    for (String query : List.of("s GuI", "roFi", "Example.org/oLD")) {
      assertTrue(search(query).articles().isEmpty(), query);
    }
    assertEquals(List.of("guide"), names(search("new")));
  }

  @TestTemplate
  void malformedOwnersAreRejected() throws Exception {
    createAndInsertMakeLake(LAKE);
    for (var owner :
        List.of(
            new ArticleSearch.DirectOwner("writer", null),
            new ArticleSearch.DirectOwner(null, Owner.Type.GROUP),
            new ArticleSearch.DirectOwner("  ", Owner.Type.USER))) {
      assertThrows(
          IllegalArgumentException.class, () -> list(ArticleListFilter.builder().withOwner(owner)));
    }
  }

  @TestTemplate
  void nonAsciiCaseMatchingFollowsDatabaseLocale() throws Exception {
    createAndInsertMakeLake(LAKE);
    manager.createArticle(LAKE, article("title", "İzmir").build());
    manager.createArticle(LAKE, article("synonym", "Alias").addSynonym("İzmir").build());
    manager.createArticle(
        LAKE,
        article("body", "Place")
            .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "İzmir"))
            .build());

    // LOWER may map capital dotted I to i or to i followed by a combining dot.
    try (var session = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        var statement =
            session
                .getConnection()
                .prepareStatement(
                    "SELECT LOWER(title) LIKE LOWER(?) ESCAPE '!' FROM kb_article_meta WHERE article_name = ?")) {
      statement.setString(1, "%izmir%");
      statement.setString(2, "title");
      try (var rows = statement.executeQuery()) {
        assertTrue(rows.next());
        assertEquals(
            rows.getBoolean(1) ? Set.of("synonym", "body", "title") : Set.of(),
            Set.copyOf(names(search("izmir"))));
      }
    }
  }

  @TestTemplate
  void targetFiltersIntersectBeforePaginationAndUseLiveBindings() throws Exception {
    createAndInsertMakeLake(LAKE);
    createAndInsertCatalog(LAKE, "warehouse");
    var warehouse = ArticleObjectIdentifier.of(MetadataObject.Type.CATALOG, List.of(), "warehouse");
    insertDomain(LAKE, "Finance", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    insertDomain(LAKE, "Billing", Domain.Status.PUBLISHED, "Finance", AUDIT_INFO);
    String term = "net-revenue";
    glossary.createTerm(LAKE, TermSpec.builder(term).build());
    for (var status : ArticleStatus.values()) {
      manager.createArticle(
          LAKE,
          article(status.name(), "Refund")
              .withStatus(status)
              .addTerms(term)
              .addObjects(warehouse)
              .addDomains("Billing")
              .build());
    }
    manager.createArticle(
        LAKE, article("no-asset", "Refund").addTerms(term).addDomains("Finance").build());
    manager.createArticle(
        LAKE, article("no-term", "Refund").addObjects(warehouse).addDomains("Finance").build());
    manager.createArticle(
        LAKE, article("no-domain", "Refund").addObjects(warehouse).addTerms(term).build());
    manager.createArticle(
        LAKE,
        article("no-keyword", "Other")
            .addObjects(warehouse)
            .addTerms(term)
            .addDomains("Finance")
            .build());

    var filter =
        ArticleListFilter.builder()
            .withDomain("Finance")
            .withTerm(term)
            .withObject(warehouse)
            .withQuery("refund")
            .withLimit(1);
    var published = list(filter);
    assertEquals(List.of("PUBLISHED"), names(published));
    assertFalse(published.hasMore());
    assertEquals(List.of("Billing"), List.of(published.articles().get(0).domains()));
    assertNull(published.articles().get(0).owner());

    assertTrue(list(filter.withStatus(ArticleStatus.DEPRECATED)).articles().isEmpty());
    assertTrue(
        list(filter.withStatus(ArticleStatus.DRAFT).withIncludeDeprecated(true))
            .articles()
            .isEmpty());
    var first = list(filter.withStatus(null));
    assertEquals(List.of("DEPRECATED"), names(first));
    assertTrue(first.hasMore());
    var last = list(filter.withOffset(1));
    assertEquals(List.of("PUBLISHED"), names(last));
    assertFalse(last.hasMore());
    assertFalse(list(filter.withOffset(Integer.MAX_VALUE)).hasMore());
    assertThrows(
        NoSuchMetadataObjectException.class,
        () ->
            list(
                ArticleListFilter.builder()
                    .withTerm(term)
                    .withQuery("absent")
                    .withObject(
                        ArticleObjectIdentifier.of(
                            MetadataObject.Type.CATALOG, List.of(), "missing"))));
    assertThrows(
        NoSuchTermException.class,
        () ->
            list(
                ArticleListFilter.builder()
                    .withObject(warehouse)
                    .withQuery("absent")
                    .withTerm("missing")));
    glossary.dropTerm(LAKE, term);
    glossary.createTerm(LAKE, TermSpec.builder(term).build());
    assertTrue(list(filter.withOffset(0)).articles().isEmpty());
  }

  @TestTemplate
  void summariesPageByTitleThenNameAndBoundEachTermPreviewIndependently() throws Exception {
    createAndInsertMakeLake(LAKE);
    String[] terms =
        IntStream.rangeClosed(0, ArticleListFilter.DEFAULT_LIMIT)
            .mapToObj(index -> "term-" + index)
            .toArray(String[]::new);
    for (String term : terms) {
      glossary.createTerm(LAKE, TermSpec.builder(term).build());
    }
    manager.createArticle(LAKE, article("z", "First").addTerms(terms).build());
    manager.createArticle(LAKE, article("b", "Second").build());
    manager.createArticle(LAKE, article("a", "Second").addTerms(terms[0]).build());

    var filter = ArticleListFilter.builder().withLimit(2);
    var page = list(filter);
    assertEquals(List.of("z", "a"), names(page));
    assertTrue(page.hasMore());
    var preview = page.articles().get(0);
    assertEquals("First", preview.title());
    assertEquals(ArticleStatus.PUBLISHED, preview.status());
    assertEquals(ArticleListFilter.DEFAULT_LIMIT, preview.terms().length);
    for (int i = 0; i < preview.terms().length; i++) {
      assertEquals(terms[i], preview.terms()[i].name());
    }
    assertTrue(preview.termsHasMore());
    var single = page.articles().get(1);
    assertEquals(1, single.terms().length);
    assertEquals(terms[0], single.terms()[0].name());
    assertFalse(single.termsHasMore());
    var last = list(filter.withOffset(2));
    assertEquals(List.of("b"), names(last));
    assertFalse(last.hasMore());
    assertEquals(0, last.articles().get(0).terms().length);
    assertFalse(last.articles().get(0).termsHasMore());
  }

  private static ArticleCreate.Builder article(String name, String title) {
    return ArticleCreate.builder()
        .withName(name)
        .withTitle(title)
        .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "body"));
  }

  private ArticlePage search(String query) {
    return list(ArticleListFilter.builder().withQuery(query));
  }

  private ArticlePage list(ArticleListFilter.Builder filter) {
    return manager.listArticleSummaries(LAKE, filter.build());
  }

  private static List<String> names(ArticlePage page) {
    return page.articles().stream().map(ArticleSummary::name).toList();
  }
}
