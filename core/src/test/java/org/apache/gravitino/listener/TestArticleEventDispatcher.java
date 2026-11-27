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
package org.apache.gravitino.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.audit.JsonAuditFormatter;
import org.apache.gravitino.audit.SimpleFormatter;
import org.apache.gravitino.audit.v2.SimpleFormatterV2;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.kb.ArticleBody;
import org.apache.gravitino.kb.ArticleStatus;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.event.article.ArticleEvent;
import org.apache.gravitino.listener.api.event.article.ArticleFailureEvent;
import org.apache.gravitino.listener.api.event.article.ArticleListEvent;
import org.apache.gravitino.listener.api.event.article.ArticleMutationEvent;
import org.apache.gravitino.listener.api.event.article.ArticlePreEvent;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.utils.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

class TestArticleEventDispatcher {
  private static final NameIdentifier ARTICLE = NameIdentifier.of("lake", "guide");

  @AfterEach
  void clearContext() {
    RequestContext.clear();
    RequestContext.resetOperationFailureFired();
    RequestContext.resetOperationSuccessFired();
  }

  @ParameterizedTest
  @EnumSource(value = OperationType.class, mode = EnumSource.Mode.MATCH_ALL, names = ".*ARTICLE.*")
  @SuppressWarnings("deprecation")
  void recognizesArticleOperationsInBothAuditVersions(OperationType operation) throws IOException {
    for (Event event :
        List.of(
            new ArticleEvent("editor", ARTICLE, operation),
            new ArticleFailureEvent("editor", ARTICLE, operation, new IllegalStateException()))) {
      assertEquals(operation.name(), new SimpleFormatter().format(event).operation().name());
      var current = new SimpleFormatterV2().format(event);
      assertEquals(operation.name(), current.operation().name());
      assertEquals(event.operationStatus(), current.operationStatus());
      var json =
          JsonUtils.objectMapper().readTree(new JsonAuditFormatter().format(event).toString());
      assertEquals(operation.name(), json.get("operation").asText());
      assertEquals(event.operationStatus().name(), json.get("operationStatus").asText());
      assertEquals(ARTICLE.toString(), json.get("identifier").asText());
    }
  }

  @Test
  void preservesCountAndRequestContextForAsyncAudit() throws IOException {
    RequestContext.setRemoteAddress("192.0.2.7");
    RequestContext.setRequestQueryParams(Map.of("domain", "Finance"));
    var event =
        new ArticleListEvent("reader", NameIdentifier.of("lake"), OperationType.LIST_ARTICLES, 2);
    RequestContext.clear();
    var json = JsonUtils.objectMapper().readTree(new JsonAuditFormatter().format(event).toString());
    assertEquals("192.0.2.7", json.get("remoteAddress").asText());
    assertEquals("Finance", json.get("customInfo").get("domain").asText());
    assertEquals(2, json.get("resultCount").intValue());
    assertTrue(new SimpleFormatterV2().format(event).toString().contains("count=2"));
  }

  @Test
  void recordsEffectiveTargetsAndFieldNamesWithoutContent() {
    Binding table = new Binding("TABLE", 11);
    Binding term = new Binding("GLOSSARY_TERM", 17);
    var before =
        KnowledgeBaseArticleEntity.builder()
            .withId(9L)
            .withName("guide")
            .withNamespace(Namespace.of("lake"))
            .withTitle("Original")
            .withStatus(ArticleStatus.DRAFT)
            .withBindings(Set.of(table))
            .withDomainIds(Set.of(1L, 2L))
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    var after =
        before.toBuilder()
            .withTitle("Unpublished title value")
            .withStatus(ArticleStatus.PUBLISHED)
            .withProperties(Map.of("key", "Unpublished property value"))
            .withRevision(2)
            .withBodyUpdate(ArticleBody.of(ArticleBody.Format.MARKDOWN, "Unpublished body value"))
            .withBindings(Set.of(term, table))
            .withDomainIds(Set.of(2L, 3L))
            .build();
    RequestContext.setRequestQueryParams(
        Map.of("removedBindings", "untrusted", "assignedOwner", "untrusted"));
    var event =
        new ArticleMutationEvent(
            "editor", ARTICLE, OperationType.ALTER_ARTICLE, before, after, true, null);
    assertEquals(Set.of(term), event.addedBindings());
    assertEquals(Set.of(), event.removedBindings());
    assertEquals(Set.of(1L), event.removedDomainIds());
    assertEquals(Set.of(3L), event.addedDomainIds());
    assertThrows(UnsupportedOperationException.class, () -> event.addedBindings().clear());
    var facts = event.customInfo();
    assertEquals("DRAFT", facts.get("previousStatus"));
    assertEquals("PUBLISHED", facts.get("status"));
    assertEquals("true", facts.get("bodyChanged"));
    assertEquals("GLOSSARY_TERM:17", facts.get("addedBindings"));
    assertEquals("", facts.get("removedBindings"));
    assertEquals("", facts.get("assignedOwner"));
    assertEquals("title,properties", facts.get("changedFields"));
    String json = new JsonAuditFormatter().format(event).toString();
    assertFalse(json.contains("Unpublished"));
    assertFalse(json.contains("untrusted"));
  }

  @Test
  void preEventCanVetoAndFailuresPreserveTheOriginalException() {
    var listener = mock(EventListenerPlugin.class);
    var dispatcher = new ArticleEventDispatcher(new EventBus(List.of(listener)));
    var error = new IllegalStateException("store failure");
    assertSame(
        error,
        assertThrows(
            IllegalStateException.class,
            () ->
                dispatcher.execute(
                    OperationType.DROP_ARTICLE,
                    ARTICLE,
                    () -> {
                      throw error;
                    },
                    (user, result) -> null)));
    verify(listener).onPreEvent(any(ArticlePreEvent.class));
    var captured = ArgumentCaptor.forClass(Event.class);
    verify(listener).onPostEvent(captured.capture());
    assertSame(error, assertInstanceOf(ArticleFailureEvent.class, captured.getValue()).exception());

    var veto = new ForbiddenException("veto");
    doThrow(veto).when(listener).onPreEvent(any());
    assertSame(
        veto,
        assertThrows(
            ForbiddenException.class,
            () ->
                dispatcher.execute(
                    OperationType.DROP_ARTICLE,
                    ARTICLE,
                    () -> {
                      throw new AssertionError();
                    },
                    (user, result) -> null)));
    verify(listener, times(2)).onPreEvent(any(ArticlePreEvent.class));
    verifyNoMoreInteractions(listener);
  }
}
