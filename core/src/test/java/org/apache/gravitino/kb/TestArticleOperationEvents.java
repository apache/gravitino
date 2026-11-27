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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.ArticleRevisionConflictException;
import org.apache.gravitino.exceptions.NoSuchKnowledgeBaseArticleException;
import org.apache.gravitino.glossary.GlossaryManager;
import org.apache.gravitino.glossary.TermSpec;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.BaseEvent;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.OperationStatus;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.apache.gravitino.listener.api.event.article.ArticleFailureEvent;
import org.apache.gravitino.listener.api.event.article.ArticleListEvent;
import org.apache.gravitino.listener.api.event.article.ArticleMutationEvent;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

class TestArticleOperationEvents extends TestJDBCBackend {
  private static final String LAKE = "lake";
  private static final String GUIDE = "guide";
  private static final ArticleBody BODY = ArticleBody.of(ArticleBody.Format.MARKDOWN, "guide text");
  private final AtomicLong ids = new AtomicLong(100);
  private final List<BaseEvent> events = new ArrayList<>();
  private final EventBus eventBus = new EventBus(List.of(new RecordingListener()));
  private final ArticleManager articles =
      new ArticleManager(ids::incrementAndGet, 1024, false, eventBus);

  @BeforeEach
  void clearEvents() {
    events.clear();
  }

  private final class RecordingListener implements EventListenerPlugin {
    @Override
    public void init(Map<String, String> properties) {}

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void onPreEvent(PreEvent event) {
      events.add(event);
    }

    @Override
    public void onPostEvent(Event event) {
      events.add(event);
    }
  }

  @TestTemplate
  void recordsEffectiveChangesAndPreservesConflictingContent() throws IOException {
    createAndInsertMakeLake(LAKE);
    var glossary = new GlossaryManager(ids::incrementAndGet, false);
    var revenue = glossary.createTerm(LAKE, TermSpec.builder("Revenue").build());
    var created =
        articles.createArticle(
            LAKE, ArticleCreate.builder().withTitle("Guide").withBody(BODY).build());
    assertEvents(
        OperationType.CREATE_ARTICLE, OperationStatus.UNPROCESSED, OperationStatus.SUCCESS);
    assertEquals(NameIdentifier.of(LAKE), events.get(0).identifier());
    assertEquals(NameIdentifier.of(LAKE, GUIDE), events.get(1).identifier());
    assertEquals("true", ((ArticleMutationEvent) events.get(1)).customInfo().get("bodyChanged"));
    events.clear();

    var bound =
        articles.alterArticle(
            LAKE, GUIDE, created.revisionToken(), KnowledgeBaseArticleChange.addTerms("Revenue"));
    assertEvents(
        OperationType.ALTER_ARTICLE_BINDINGS, OperationStatus.UNPROCESSED, OperationStatus.SUCCESS);
    var change = (ArticleMutationEvent) events.get(1);
    assertEquals(Set.of(new Binding("GLOSSARY_TERM", revenue.id())), change.addedBindings());
    assertTrue(change.removedBindings().isEmpty());
    events.clear();
    assertEquals(
        bound.revisionToken(),
        articles
            .alterArticle(LAKE, GUIDE, null, KnowledgeBaseArticleChange.addTerms("Revenue"))
            .revisionToken());
    assertEvents(OperationType.ALTER_ARTICLE_BINDINGS, OperationStatus.UNPROCESSED);
    events.clear();

    var failure =
        assertThrows(
            ArticleRevisionConflictException.class,
            () ->
                articles.alterArticle(
                    LAKE,
                    GUIDE,
                    created.revisionToken(),
                    KnowledgeBaseArticleChange.updateBody(
                        ArticleBody.of(ArticleBody.Format.MARKDOWN, "conflicting body"))));
    assertEvents(OperationType.ALTER_ARTICLE, OperationStatus.UNPROCESSED, OperationStatus.FAILURE);
    assertSame(failure, ((ArticleFailureEvent) events.get(1)).exception());
    events.clear();
    assertEquals(BODY.content(), articles.getArticleContent(LAKE, GUIDE).body().content());
    assertEvents(
        OperationType.LOAD_ARTICLE_CONTENT, OperationStatus.UNPROCESSED, OperationStatus.SUCCESS);
    events.clear();

    articles.alterArticle(
        LAKE,
        GUIDE,
        bound.revisionToken(),
        KnowledgeBaseArticleChange.updateBody(BODY),
        KnowledgeBaseArticleChange.updateTitle("Revised"),
        KnowledgeBaseArticleChange.removeTerms("Revenue"));
    assertEvents(OperationType.ALTER_ARTICLE, OperationStatus.UNPROCESSED, OperationStatus.SUCCESS);
    var mixed = (ArticleMutationEvent) events.get(1);
    assertEquals("false", mixed.customInfo().get("bodyChanged"));
    assertEquals(Set.of(new Binding("GLOSSARY_TERM", revenue.id())), mixed.removedBindings());
    assertTrue(mixed.customInfo().get("changedFields").contains("title"));
  }

  @TestTemplate
  void countsVisibleListsAndSuppressesRepeatedDetachAndDrop() throws Exception {
    createAndInsertMakeLake(LAKE);
    long domainId = insertDomain(LAKE, "Sales", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    articles.createArticle(
        LAKE,
        ArticleCreate.builder()
            .withName(GUIDE)
            .withTitle("Guide")
            .withBody(BODY)
            .addDomain("Sales")
            .build());
    events.clear();
    articles.listArticleSummaries(LAKE, ArticleListFilter.builder().build());
    assertEvents(OperationType.LIST_ARTICLES, OperationStatus.UNPROCESSED, OperationStatus.SUCCESS);
    assertEquals(1, ((ArticleListEvent) events.get(1)).resultCount());
    events.clear();
    articles.loadArticle(LAKE, GUIDE);
    assertEvents(OperationType.LOAD_ARTICLE, OperationStatus.UNPROCESSED, OperationStatus.SUCCESS);
    events.clear();
    assertThrows(IllegalArgumentException.class, () -> articles.loadArticle(LAKE, "invalid name"));
    assertEvents(OperationType.LOAD_ARTICLE, OperationStatus.UNPROCESSED, OperationStatus.FAILURE);
    assertEquals(NameIdentifier.of(LAKE, "invalid name"), events.get(1).identifier());
    events.clear();
    assertThrows(
        IllegalArgumentException.class,
        () -> articles.listArticlesForTerm(LAKE, "Revenue", false, 0, 0));
    assertEvents(OperationType.LIST_ARTICLES, OperationStatus.UNPROCESSED, OperationStatus.FAILURE);
    events.clear();
    articles.detachDomain(LAKE, GUIDE, "Sales");
    assertEvents(
        OperationType.DETACH_ARTICLE_DOMAIN, OperationStatus.UNPROCESSED, OperationStatus.SUCCESS);
    assertEquals(Set.of(domainId), ((ArticleMutationEvent) events.get(1)).removedDomainIds());
    events.clear();
    articles.detachDomain(LAKE, GUIDE, "Sales");
    assertEvents(OperationType.DETACH_ARTICLE_DOMAIN, OperationStatus.UNPROCESSED);
    events.clear();
    assertTrue(articles.deleteArticle(LAKE, GUIDE));
    assertEvents(OperationType.DROP_ARTICLE, OperationStatus.UNPROCESSED, OperationStatus.SUCCESS);
    events.clear();
    assertFalse(articles.deleteArticle(LAKE, GUIDE));
    assertEvents(OperationType.DROP_ARTICLE, OperationStatus.UNPROCESSED);
  }

  @TestTemplate
  void rollsBackWithoutPublishingWriteSuccess() throws IOException {
    createAndInsertMakeLake(LAKE);
    SqlSessions.getSqlSession();
    try {
      articles.createArticle(
          LAKE, ArticleCreate.builder().withTitle("Guide").withBody(BODY).build());
      assertEvents(OperationType.CREATE_ARTICLE, OperationStatus.UNPROCESSED);
    } finally {
      SqlSessions.rollbackAndCloseSqlSession();
    }
    assertEvents(OperationType.CREATE_ARTICLE, OperationStatus.UNPROCESSED);
    events.clear();
    assertThrows(
        NoSuchKnowledgeBaseArticleException.class, () -> articles.loadArticle(LAKE, GUIDE));
    assertEvents(OperationType.LOAD_ARTICLE, OperationStatus.UNPROCESSED, OperationStatus.FAILURE);
  }

  private void assertEvents(OperationType operation, OperationStatus... statuses) {
    assertEquals(List.of(statuses), events.stream().map(BaseEvent::operationStatus).toList());
    assertTrue(events.stream().allMatch(event -> event.operationType() == operation));
  }
}
