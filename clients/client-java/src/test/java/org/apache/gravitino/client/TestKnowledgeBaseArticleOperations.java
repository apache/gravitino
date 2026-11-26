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
package org.apache.gravitino.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.authorization.OwnerDTO;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.ArticleRevisionConflictException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.KnowledgeBaseArticleAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchKnowledgeBaseArticleException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.UnapprovedTermException;
import org.apache.gravitino.kb.ArticleBody;
import org.apache.gravitino.kb.ArticleCreate;
import org.apache.gravitino.kb.ArticleListFilter;
import org.apache.gravitino.kb.ArticleObjectIdentifier;
import org.apache.gravitino.kb.ArticleStatus;
import org.apache.gravitino.kb.ArticleTermBinding;
import org.apache.gravitino.kb.KnowledgeBaseArticleChange;
import org.apache.gravitino.kb.KnowledgeBaseArticleOperations;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Test;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;

/** Tests the Knowledge Base Article client against the REST wire contract. */
class TestKnowledgeBaseArticleOperations extends TestBase {
  private static final String ROOT = "/api/metalakes/metalake/kb-articles";
  private static final String NAME = "销售-guide";
  private static final String ITEM = ROOT + "/" + RESTUtils.encodeString(NAME);
  private static final String TERM = """
      {"name":"Revenue","status":"DEPRECATED"}""";
  private static final String OBJECT =
      """
      {"type":"COLUMN","namespace":["ice.berg","sales & tax","orders+2026"],"name":"net.amount"}""";
  private static final String SUMMARY =
      """
      {"name":"销售-guide","title":"Sales","status":"PUBLISHED","domains":["Finance"],
       "owner":{"type":"group","name":"sales:team"},"terms":[%s],"termsHasMore":true,
       "audit_info":{"creator":"alice","createTime":"2026-01-01T00:00:00Z"}}"""
          .formatted(TERM);

  @Test
  void listsSummariesWithOrderedLiteralAssetComponents() {
    var filter =
        ArticleListFilter.builder()
            .withQuery("收入 + 100% & fees")
            .withDomain("Finance")
            .withStatus(ArticleStatus.PUBLISHED)
            .withOwner(OwnerDTO.builder().withName("sales:team").withType(Owner.Type.GROUP).build())
            .withTerm("Revenue")
            .withObject(column())
            .withIncludeDeprecated(true)
            .withLimit(2)
            .withOffset(5)
            .build();
    var search =
        request(ROOT)
            .withMethod("GET")
            .withQueryStringParameter("q", filter.query())
            .withQueryStringParameter("domain", "Finance")
            .withQueryStringParameter("status", "PUBLISHED")
            .withQueryStringParameter("owner", "group:sales:team")
            .withQueryStringParameter("term", "Revenue")
            .withQueryStringParameter("objectType", "COLUMN")
            .withQueryStringParameter("name", "net.amount")
            .withQueryStringParameter("includeDeprecated", "true")
            .withQueryStringParameter("limit", "2")
            .withQueryStringParameter("offset", "5");
    respond(search, """
        {"code":0,"articles":[%s],"hasMore":true}""".formatted(SUMMARY));
    var page = articles().listArticles(filter);
    var received = mockServer.retrieveRecordedRequests(search);
    assertEquals(1, received.length);
    assertEquals(
        column().namespace(), received[0].getQueryStringParameters().getValues("namespace"));
    assertTrue(page.hasMore());
    var summary = page.articles().get(0);
    assertEquals(NAME, summary.name());
    assertEquals("Sales", summary.title());
    assertEquals(ArticleStatus.PUBLISHED, summary.status());
    assertEquals(Owner.Type.GROUP, summary.owner().type());
    assertEquals("sales:team", summary.owner().name());
    assertArrayEquals(new String[] {"Finance"}, summary.domains());
    assertTrue(summary.termsHasMore());
    assertEquals(ArticleTermBinding.Status.DEPRECATED, summary.terms()[0].status());
    assertEquals("alice", summary.auditInfo().creator());
  }

  @Test
  void loadsBindingPreviewsAndDetachesAnEncodedDomain() {
    var ops = articles();
    var window =
        request(ITEM)
            .withMethod("GET")
            .withQueryStringParameter("limit", "1")
            .withQueryStringParameter("offset", "3");
    respond(
        window,
        """
        {"code":0,"article":{"name":"销售-guide","title":"Sales","status":"PUBLISHED",
         "domains":["Finance"],"owner":null,"comment":"guide","synonyms":["turnover"],
         "sourceReference":"manual","properties":{"purpose":"test"},"objects":[%s],"objectsHasMore":true,
         "terms":[%s],"termsHasMore":false,"revisionToken":"v1",
         "audit_info":{"creator":"alice","createTime":"2026-01-01T00:00:00Z"}}}"""
            .formatted(OBJECT, TERM));
    var snapshot = ops.loadArticle(NAME, 1, 3);
    assertEquals("v1", snapshot.revisionToken());
    assertEquals("guide", snapshot.comment());
    assertArrayEquals(new String[] {"turnover"}, snapshot.synonyms());
    assertEquals("manual", snapshot.sourceReference());
    assertEquals(Map.of("purpose", "test"), snapshot.properties());
    assertTrue(snapshot.objectsHasMore());
    assertFalse(snapshot.termsHasMore());
    assertEquals(column(), snapshot.objects()[0]);
    assertEquals("Revenue", snapshot.terms()[0].name());

    respond(
        request(ITEM + "/terms")
            .withMethod("GET")
            .withQueryStringParameter("limit", "3")
            .withQueryStringParameter("offset", "1"),
        """
        {"code":0,"terms":[%s],"hasMore":true}""".formatted(TERM));
    var terms = ops.listTermBindings(NAME, 3, 1);
    assertTrue(terms.hasMore());
    assertEquals(ArticleTermBinding.Status.DEPRECATED, terms.terms().get(0).status());

    respond(
        request(ITEM + "/objects")
            .withMethod("GET")
            .withQueryStringParameter("limit", "1")
            .withQueryStringParameter("offset", "4"),
        """
        {"code":0,"objects":[%s],"hasMore":false}""".formatted(OBJECT));
    var assets = ops.listAssetBindings(NAME, 1, 4);
    assertFalse(assets.hasMore());
    assertEquals(List.of(column()), assets.objects());

    String domain = "Finance/税 + 10%";
    respond(
        request(ITEM + "/domains/" + RESTUtils.encodeString(domain)).withMethod("DELETE"),
        "{\"code\":0}");
    ops.detachDomain(NAME, domain);
  }

  @Test
  void mapsMissingTargetsAndConflictsToTheirSpecificException() throws Exception {
    var ops = articles();
    for (var type :
        List.of(
            NoSuchMetalakeException.class,
            NoSuchTermException.class,
            NoSuchMetadataObjectException.class)) {
      buildMockResource(
          Method.GET, ROOT, null, ErrorResponse.notFound(type.getSimpleName(), "missing"), 404);
      assertThrows(type, () -> ops.listArticles(ArticleListFilter.builder().build()));
    }
    buildMockResource(
        Method.GET,
        ITEM + "/content",
        null,
        ErrorResponse.notFound("NoSuchKnowledgeBaseArticleException", "gone"),
        404);
    assertThrows(NoSuchKnowledgeBaseArticleException.class, () -> ops.getArticleContent(NAME));
    buildMockResource(
        Method.DELETE,
        ITEM + "/domains/Finance",
        null,
        ErrorResponse.notFound("NoSuchDomainException", "missing"),
        404);
    assertThrows(NoSuchDomainException.class, () -> ops.detachDomain(NAME, "Finance"));
    var create =
        ArticleCreate.builder()
            .withName(NAME)
            .withTitle("Sales")
            .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "guide"))
            .build();
    buildMockResource(
        Method.POST,
        ROOT,
        null,
        ErrorResponse.alreadyExists("KnowledgeBaseArticleAlreadyExistsException", "duplicate"),
        409);
    assertThrows(KnowledgeBaseArticleAlreadyExistsException.class, () -> ops.createArticle(create));
    buildMockResource(
        Method.POST,
        ROOT,
        null,
        ErrorResponse.unsupportedOperation("draft", new UnapprovedTermException("draft")),
        409);
    assertThrows(UnapprovedTermException.class, () -> ops.createArticle(create));
    buildMockResource(
        Method.PUT,
        ITEM,
        null,
        ErrorResponse.optimisticLockConflict("ArticleRevisionConflictException", "stale", null),
        409);
    assertThrows(
        ArticleRevisionConflictException.class,
        () ->
            ops.alterArticle(
                NAME,
                new KnowledgeBaseArticleChange[] {KnowledgeBaseArticleChange.updateTitle("New")},
                "stale"));
    buildMockResource(Method.DELETE, ITEM, null, ErrorResponse.forbidden("denied", null), 403);
    assertThrows(ForbiddenException.class, () -> ops.deleteArticle(NAME));
  }

  private static KnowledgeBaseArticleOperations articles() {
    Metalake metalake =
        new GravitinoMetalake(
            "metalake",
            null,
            Map.of(),
            AuditDTO.builder().withCreator("test").withCreateTime(Instant.EPOCH).build(),
            null,
            client.restClient());
    return metalake.articleOperations();
  }

  private static ArticleObjectIdentifier column() {
    return ArticleObjectIdentifier.of(
        MetadataObject.Type.COLUMN,
        List.of("ice.berg", "sales & tax", "orders+2026"),
        "net.amount");
  }

  private static void respond(HttpRequest request, String json) {
    mockServer.when(request, Times.once()).respond(response().withStatusCode(200).withBody(json));
  }
}
