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

package org.apache.gravitino.server.web.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.dto.authorization.OwnerDTO;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.ArticleRevisionConflictException;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.KnowledgeBaseArticleAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchKnowledgeBaseArticleException;
import org.apache.gravitino.exceptions.ServiceUnavailableException;
import org.apache.gravitino.exceptions.UnapprovedTermException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.kb.ArticleBody;
import org.apache.gravitino.kb.ArticleContent;
import org.apache.gravitino.kb.ArticleCreate;
import org.apache.gravitino.kb.ArticleManager;
import org.apache.gravitino.kb.ArticleObjectIdentifier;
import org.apache.gravitino.kb.ArticleTermBinding;
import org.apache.gravitino.kb.AssetBindingPage;
import org.apache.gravitino.kb.KnowledgeBaseArticleChange;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity;
import org.apache.gravitino.kb.TermBindingPage;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.server.web.mapper.JsonMappingExceptionMapper;
import org.apache.gravitino.server.web.mapper.JsonProcessingExceptionMapper;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestArticleOperations extends BaseOperationsTest {
  private static final String BASE = "/metalakes/test/kb-articles";
  private static final String V1 = "application/vnd.gravitino.v1+json";
  private final ArticleManager manager = mock(ArticleManager.class);
  private final EntityStore store = mock(EntityStore.class);
  private Object previousStore;

  private static class RequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      return mock(HttpServletRequest.class);
    }
  }

  @BeforeEach
  void installMetalake() throws Exception {
    previousStore = FieldUtils.readField(GravitinoEnv.getInstance(), "entityStore", true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", store, true);
    BaseMetalake lake = mock(BaseMetalake.class);
    PropertiesMetadata properties = mock(PropertiesMetadata.class);
    when(properties.getOrDefault(any(), any())).thenReturn(true);
    when(lake.propertiesMetadata()).thenReturn(properties);
    when(store.get(any(), any(), any())).thenReturn(lake);
  }

  @AfterEach
  void restoreStore() throws Exception {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", previousStore, true);
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(new ObjectMapperProvider());
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new ResourceConfig(
            ArticleOperations.class,
            JsonProcessingExceptionMapper.class,
            JsonMappingExceptionMapper.class)
        .register(new ObjectMapperProvider())
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(manager).to(ArticleManager.class);
                bindFactory(RequestFactory.class).to(HttpServletRequest.class);
              }
            });
  }

  @Test
  void createsAndLoadsMetadataAndContentForUnicodeName() throws IOException {
    String name = "销售-Guide";
    var ref =
        ArticleObjectIdentifier.of(
            MetadataObject.Type.COLUMN, List.of("warehouse", "sales", "orders"), "total.amount");
    var body = ArticleBody.of(ArticleBody.Format.MARKDOWN, "# Sales");
    var audit = AuditInfo.builder().withCreator("alice").withCreateTime(Instant.EPOCH).build();
    var article =
        KnowledgeBaseArticleEntity.builder()
            .withId(1L)
            .withNamespace(Namespace.of("test"))
            .withName(name)
            .withTitle("Sales")
            .withAuditInfo(audit)
            .withBodyUpdate(body)
            .withOwner(OwnerDTO.builder().withName("alice").withType(Owner.Type.USER).build())
            .withObjects(List.of(ref))
            .withObjectsHasMore(true)
            .build();
    var result = new ArticleManager.Result(article, "token");
    when(manager.createArticle(eq("test"), any())).thenReturn(result);
    when(manager.loadArticle("test", name, 2, 1)).thenReturn(result);
    when(manager.getArticleContent("test", name)).thenReturn(new ArticleContent(body, "token"));
    String json =
        """
        {"name":"销售-Guide","title":"Sales","body":{"format":"MARKDOWN","content":"# Sales"},
         "terms":["Revenue"],"objects":[{"type":"COLUMN","namespace":["warehouse","sales","orders"],"name":"total.amount"}]}
        """;
    try (Response r = target(BASE).request(V1).post(Entity.json(json))) {
      JsonNode metadata = success(r).get("article");
      assertEquals(name, metadata.get("name").asText());
      assertEquals("token", metadata.get("revisionToken").asText());
      assertEquals("alice", metadata.get("owner").get("name").asText());
      assertEquals("alice", metadata.get("audit_info").get("creator").asText());
      assertEquals("total.amount", metadata.get("objects").get(0).get("name").asText());
      assertTrue(metadata.get("objectsHasMore").asBoolean());
      assertFalse(metadata.has("body"));
      assertFalse(metadata.has("id"));
    }
    var captured = ArgumentCaptor.forClass(ArticleCreate.class);
    verify(manager).createArticle(eq("test"), captured.capture());
    assertEquals(body, captured.getValue().body());
    assertEquals(List.of(ref), captured.getValue().objects());
    WebTarget item = target(BASE + "/%E9%94%80%E5%94%AE-Guide");
    try (Response r =
        item.queryParam("limit", 2)
            .queryParam("offset", 1)
            .request(MediaType.APPLICATION_JSON)
            .get()) {
      assertEquals(name, success(r).get("article").get("name").asText());
    }
    try (Response r = item.path("content").request().get()) {
      var content = success(r);
      assertEquals("# Sales", content.get("body").get("content").asText());
      assertEquals("MARKDOWN", content.get("body").get("format").asText());
      assertEquals("token", content.get("revisionToken").asText());
      assertFalse(content.has("article"));
    }
  }

  @Test
  void listsTermBindingsWithCurrentStatusAndContinuation() throws IOException {
    var term = mock(ArticleTermBinding.class);
    when(term.name()).thenReturn("Revenue");
    when(term.status()).thenReturn(ArticleTermBinding.Status.DEPRECATED);
    when(manager.listTermBindings("test", "销售", 20, 0))
        .thenReturn(new TermBindingPage(List.of(term), true));
    when(manager.listTermBindings("test", "销售", 1, 42))
        .thenReturn(new TermBindingPage(List.of(), false));
    var terms = target(BASE + "/%E9%94%80%E5%94%AE/terms");
    try (Response response = terms.request(V1).get()) {
      assertEquals(
          JsonUtils.objectMapper()
              .readTree(
                  """
          {"code":0,"terms":[{"name":"Revenue","status":"DEPRECATED"}],"hasMore":true}
          """),
          success(response));
    }
    try (Response response =
        terms.queryParam("limit", 1).queryParam("offset", 42).request().get()) {
      assertEquals(
          JsonUtils.objectMapper()
              .readTree("""
          {"code":0,"terms":[],"hasMore":false}
          """),
          success(response));
    }
  }

  @Test
  void listsAssetBindingsWithStructuredIdentifiers() throws IOException {
    var column =
        ArticleObjectIdentifier.of(
            MetadataObject.Type.COLUMN, List.of("warehouse", "sales", "orders"), "total.amount");
    when(manager.listAssetBindings("test", "guide", 1, 1))
        .thenReturn(new AssetBindingPage(List.of(column), false));
    when(manager.listAssetBindings("test", "guide", 20, 0))
        .thenReturn(new AssetBindingPage(List.of(), false));
    var objects = target(BASE + "/guide/objects");
    try (Response response =
        objects.queryParam("limit", 1).queryParam("offset", 1).request().get()) {
      assertEquals(
          JsonUtils.objectMapper()
              .readTree(
                  """
          {"code":0,"objects":[{"type":"COLUMN","namespace":["warehouse","sales","orders"],
          "name":"total.amount"}],"hasMore":false}
          """),
          success(response));
    }
    try (Response response = objects.request(V1).get()) {
      assertEquals(
          JsonUtils.objectMapper()
              .readTree("""
          {"code":0,"objects":[],"hasMore":false}
          """),
          success(response));
    }
  }

  @Test
  void bindingPagesPreserveHiddenArticleAndInvalidPaginationErrors() {
    var hidden = new NoSuchKnowledgeBaseArticleException("hidden");
    when(manager.listTermBindings("test", "draft", 20, 0)).thenThrow(hidden);
    when(manager.listAssetBindings("test", "draft", 20, 0)).thenThrow(hidden);
    for (String path : List.of("terms", "objects")) {
      var page = target(BASE + "/draft/" + path);
      assertError(page.request().get(), 404, "NoSuchKnowledgeBaseArticleException");
      assertError(page.queryParam("offset", "bad").request().get(), 400, "NumberFormatException");
    }
  }

  @Test
  void conditionalUpdateAndRetryDeleteUseTheManagerResult() throws IOException {
    var item = target(BASE + "/guide");
    var replacement = ArticleBody.of(ArticleBody.Format.MARKDOWN, "First edit");
    var article =
        KnowledgeBaseArticleEntity.builder()
            .withId(1L)
            .withNamespace(Namespace.of("test"))
            .withName("guide")
            .withTitle("Guide")
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    when(manager.alterArticle(
            "test", "guide", "one", KnowledgeBaseArticleChange.updateBody(replacement)))
        .thenReturn(new ArticleManager.Result(article, "two"));
    when(manager.alterArticle(
            "test",
            "guide",
            "one",
            KnowledgeBaseArticleChange.updateBody(
                ArticleBody.of(ArticleBody.Format.MARKDOWN, "Second edit"))))
        .thenThrow(new ArticleRevisionConflictException("stale token"));
    String edit =
        """
        {"expectedRevision":"one","updates":[{"@type":"updateBody","newBody":{"format":"MARKDOWN","content":"%s"}}]}
        """;
    try (Response r = item.request().put(Entity.json(edit.formatted("First edit")))) {
      var metadata = success(r).get("article");
      assertEquals("two", metadata.get("revisionToken").asText());
      assertTrue(metadata.get("owner").isNull());
    }
    assertError(
        item.request().put(Entity.json(edit.formatted("Second edit"))),
        409,
        "ArticleRevisionConflictException");
    when(manager.deleteArticle("test", "guide")).thenReturn(true, false);
    for (boolean expected : List.of(true, false)) {
      try (Response r = item.request().delete()) {
        assertEquals(200, r.getStatus());
        assertEquals(expected, r.readEntity(DropResponse.class).dropped());
      }
    }
    when(manager.deleteArticle("test", "draft")).thenReturn(false);
    try (Response r = target(BASE + "/draft").request().delete()) {
      assertEquals(200, r.getStatus());
      assertFalse(r.readEntity(DropResponse.class).dropped());
    }
  }

  @Test
  void mapsVisibilityAndConflictErrors() {
    var item = target(BASE + "/guide");
    when(manager.loadArticle("test", "guide", 20, 0))
        .thenThrow(new NoSuchKnowledgeBaseArticleException("hidden"));
    assertError(item.request().get(), 404, "NoSuchKnowledgeBaseArticleException");
    when(manager.getArticleContent("test", "guide"))
        .thenThrow(new NoSuchKnowledgeBaseArticleException("hidden"));
    assertError(item.path("content").request().get(), 404, "NoSuchKnowledgeBaseArticleException");
    when(manager.deleteArticle("test", "guide"))
        .thenThrow(new ForbiddenException("insufficient privilege"));
    assertError(item.request().delete(), 403, "ForbiddenException");
    var create =
        Entity.json(
            "{\"title\":\"Guide\",\"body\":{\"format\":\"MARKDOWN\",\"content\":\"text\"}}");
    when(manager.createArticle(eq("test"), any()))
        .thenThrow(
            new KnowledgeBaseArticleAlreadyExistsException("duplicate"),
            new UnapprovedTermException("Revenue"),
            new ConnectionFailedException("offline"),
            new ServiceUnavailableException("unavailable"));
    assertError(
        target(BASE).request().post(create), 409, "KnowledgeBaseArticleAlreadyExistsException");
    assertError(target(BASE).request().post(create), 409, "UnapprovedTermException");
    assertError(target(BASE).request().post(create), 502, "ConnectionFailedException");
    assertError(target(BASE).request().post(create), 503, "ServiceUnavailableException");
  }

  @Test
  void rejectsMalformedRequestsBeforeDispatch() {
    var item = target(BASE + "/guide");
    try (Response r = item.queryParam("limit", "many").request().get();
        Response s = item.request().put(Entity.json("{\"updates\":[{\"@type\":\"rename\"}]}"));
        Response t = target(BASE).request().post(Entity.json("null"))) {
      assertEquals(400, r.getStatus());
      assertEquals(400, s.getStatus());
      assertEquals(400, t.getStatus());
    }
    for (String invalid :
        List.of(
            "{\"title\":42,\"body\":{\"format\":\"MARKDOWN\",\"content\":\"text\"}}",
            "{\"title\":\"Guide\",\"body\":{\"format\":\"MARKDOWN\",\"content\":true}}",
            "{\"title\":\"Guide\",\"body\":{\"format\":\"MARKDOWN\",\"content\":\"text\"},\"terms\":[42]}")) {
      try (Response response = target(BASE).request().post(Entity.json(invalid))) {
        assertEquals(400, response.getStatus());
      }
    }
    verifyNoInteractions(manager);
  }

  @Test
  void detachesDomainWithTheSameResponseForAnAbsentMembershipOrArticle() throws IOException {
    var draft = target(BASE + "/draft/domains/Finance");
    var absent = target(BASE + "/absent/domains/Finance");
    try (Response first = draft.request(V1).delete();
        Response repeat = draft.request().delete();
        Response missing = absent.request().delete()) {
      var ok = JsonUtils.objectMapper().readTree("{\"code\":0}");
      assertEquals(ok, success(first));
      assertEquals(ok, success(repeat));
      assertEquals(ok, success(missing));
    }
    verify(manager, times(2)).detachDomain("test", "draft", "Finance");
    verify(manager).detachDomain("test", "absent", "Finance");
  }

  @Test
  void detachRequiresAuthorityOnTheNamedDomain() {
    var path = target(BASE + "/missing/domains/Finance");
    doThrow(
            new ForbiddenException("Cannot manage Domain"),
            new NoSuchDomainException("Hidden Domain"))
        .when(manager)
        .detachDomain("test", "missing", "Finance");
    assertError(path.request().delete(), 403, "ForbiddenException");
    assertError(path.request().delete(), 404, "NoSuchDomainException");
  }

  private static JsonNode success(Response r) throws IOException {
    assertEquals(200, r.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, r.getMediaType());
    var json = JsonUtils.objectMapper().readTree(r.readEntity(String.class));
    assertEquals(0, json.get("code").asInt());
    return json;
  }

  private static void assertError(Response response, int status, String type) {
    try (response) {
      assertEquals(status, response.getStatus());
      assertEquals(type, response.readEntity(ErrorResponse.class).getType());
    }
  }
}
