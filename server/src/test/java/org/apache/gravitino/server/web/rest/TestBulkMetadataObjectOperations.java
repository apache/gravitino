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

import static org.apache.gravitino.Configs.BULK_MAX_ITEMS;
import static org.apache.gravitino.Configs.CACHE_ENABLED;
import static org.apache.gravitino.Configs.ENABLE_AUTHORIZATION;
import static org.apache.gravitino.Configs.GRAVITINO_AUTHORIZATION_THREAD_POOL_SIZE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.dto.requests.BulkMetadataObjectsRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.BulkMetadataObjectPolicyListResponse;
import org.apache.gravitino.dto.responses.BulkMetadataObjectTagListResponse;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.dto.tag.TagDTO;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagDispatcher;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

public class TestBulkMetadataObjectOperations extends BaseOperationsTest {
  private static final TagDispatcher TAGS = mock(TagDispatcher.class);
  private static final PolicyDispatcher POLICIES = mock(PolicyDispatcher.class);
  private static final String PATH = "/bulk/metalakes/demo/objects/";
  private static final String REQUEST =
      "{\"metadataObjects\":[{\"type\":\"SCHEMA\",\"fullName\":\"catalog.one\"},{\"type\":\"SCHEMA\",\"fullName\":\"catalog.two\"}]}";
  private static final MetadataObject ONE =
      MetadataObjects.parse("catalog.one", MetadataObject.Type.SCHEMA);
  private static final MetadataObject TWO =
      MetadataObjects.parse("catalog.two", MetadataObject.Type.SCHEMA);

  @BeforeAll
  public static void setupConfig() throws Exception {
    Config config = mock(Config.class);
    doReturn(false).when(config).get(CACHE_ENABLED);
    doReturn(false).when(config).get(ENABLE_AUTHORIZATION);
    doReturn(100).when(config).get(BULK_MAX_ITEMS);
    doReturn(2).when(config).get(GRAVITINO_AUTHORIZATION_THREAD_POOL_SIZE);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
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
            BulkMetadataObjectTagOperations.class, BulkMetadataObjectPolicyOperations.class)
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(TAGS).to(TagDispatcher.class);
                bind(POLICIES).to(PolicyDispatcher.class);
                bind(mock(HttpServletRequest.class)).to(HttpServletRequest.class);
              }
            });
  }

  @BeforeEach
  public void setupData() {
    reset(TAGS, POLICIES);
    AuditInfo audit =
        AuditInfo.builder().withCreator("tester").withCreateTime(Instant.EPOCH).build();
    Tag inherited =
        TagEntity.builder()
            .withId(1L)
            .withName("inherited")
            .withAuditInfo(audit)
            .withProperties(Collections.emptyMap())
            .build()
            .copyWithInherited(true);
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(2L)
            .withName("policy")
            .withAuditInfo(audit)
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withEnabled(true)
            .withContent(
                PolicyContents.custom(
                    null,
                    Collections.singleton(MetadataObject.Type.SCHEMA),
                    Collections.emptyMap()))
            .build();
    Map<MetadataObject, Tag[]> tags = new LinkedHashMap<>();
    tags.put(ONE, new Tag[] {inherited});
    tags.put(TWO, new Tag[0]);
    when(TAGS.listEffectiveTagsForDisplay(eq("demo"), any())).thenReturn(tags);
    Map<MetadataObject, PolicyEntity[]> policies = new LinkedHashMap<>();
    policies.put(ONE, new PolicyEntity[] {policy});
    policies.put(TWO, new PolicyEntity[0]);
    when(POLICIES.listPolicyInfosForDisplay(eq("demo"), any())).thenReturn(policies);
  }

  @ParameterizedTest
  @ValueSource(strings = {"tags", "policies"})
  public void testBatchContainsOnlyRequestedKindAndEmptyObjects(String kind) throws Exception {
    try (Response response = post(kind, REQUEST)) {
      Assertions.assertEquals(200, response.getStatus());
      JsonNode body = JsonUtils.objectMapper().readTree(response.readEntity(String.class));
      Assertions.assertEquals(0, body.path("code").asInt());
      Assertions.assertEquals(2, body.path("results").size());
      JsonNode first = body.path("results").get(0);
      Assertions.assertEquals(
          "catalog.one", first.path("metadataObject").path("fullName").asText());
      Assertions.assertEquals(1, first.path(kind).path(kind).size());
      Assertions.assertFalse(first.has(kind.equals("tags") ? "policies" : "tags"));
      Assertions.assertEquals(0, body.path("results").get(1).path(kind).path(kind).size());
      if (kind.equals("tags")) {
        Assertions.assertTrue(first.path(kind).path(kind).get(0).path("inherited").asBoolean());
        Assertions.assertEquals(
            "inherited", first.path(kind).path(kind).get(0).path("name").asText());
      } else {
        Assertions.assertEquals("policy", first.path(kind).path(kind).get(0).path("name").asText());
      }
    }
    if (kind.equals("tags")) {
      verify(TAGS).listEffectiveTagsForDisplay("demo", List.of(ONE, TWO));
      verifyNoInteractions(POLICIES);
    } else {
      verify(POLICIES).listPolicyInfosForDisplay("demo", List.of(ONE, TWO));
      verifyNoInteractions(TAGS);
    }
    verifyNoMoreInteractions(TAGS, POLICIES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"tags", "policies"})
  public void testRejectsInvalidBatchBeforeLoading(String kind) {
    for (String request :
        List.of(
            "null",
            "{}",
            "{\"metadataObjects\":[]}",
            "{\"metadataObjects\":[null]}",
            "{\"metadataObjects\":[{\"type\":\"ROLE\",\"fullName\":\"role\"}]}",
            "{\"metadataObjects\":[{\"type\":\"SCHEMA\",\"fullName\":\"catalog.one\"},{\"type\":\"SCHEMA\",\"fullName\":\"catalog.one\"}]}")) {
      try (Response response = post(kind, request)) {
        Assertions.assertEquals(400, response.getStatus(), request);
      }
    }
    verifyNoInteractions(TAGS, POLICIES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"tags", "policies"})
  public void testRejectsOversizedBatch(String kind) {
    String[] objects = new String[101];
    for (int i = 0; i < objects.length; i++) {
      objects[i] = "{\"type\":\"SCHEMA\",\"fullName\":\"catalog.s" + i + "\"}";
    }
    try (Response response =
        post(kind, "{\"metadataObjects\":[" + String.join(",", objects) + "]}")) {
      Assertions.assertEquals(400, response.getStatus());
    }
    verifyNoInteractions(TAGS, POLICIES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"tags", "policies"})
  public void testChecksEachObjectBeforeLoading(String kind) throws Exception {
    try (MockedStatic<MetadataAuthzHelper> auth =
        mockStatic(MetadataAuthzHelper.class, CALLS_REAL_METHODS)) {
      auth.when(
              () ->
                  MetadataAuthzHelper.filterMetadataObject(
                      eq("demo"), any(MetadataObjectDTO[].class)))
          .thenAnswer(
              invocation ->
                  new MetadataObjectDTO[] {((MetadataObjectDTO[]) invocation.getArgument(1))[0]});
      try (Response response = directPost(kind)) {
        Assertions.assertEquals(200, response.getStatus());
        BaseResponse result = (BaseResponse) response.getEntity();
        result.validate();
        JsonNode body = JsonUtils.objectMapper().valueToTree(result);
        Assertions.assertEquals(1, body.path("results").size());
        Assertions.assertEquals(1, body.path("errors").size());
        Assertions.assertEquals(1, body.path("errors").get(0).path("index").asInt());
        Assertions.assertEquals(1008, body.path("errors").get(0).path("code").asInt());
      }
      if (kind.equals("tags")) {
        verify(TAGS).listEffectiveTagsForDisplay("demo", List.of(ONE));
        verifyNoInteractions(POLICIES);
      } else {
        verify(POLICIES).listPolicyInfosForDisplay("demo", List.of(ONE));
        verifyNoInteractions(TAGS);
      }
      verifyNoMoreInteractions(TAGS, POLICIES);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"tags", "policies"})
  public void testFiltersHiddenResults(String kind) throws Exception {
    try (MockedStatic<MetadataAuthzHelper> auth =
        mockStatic(MetadataAuthzHelper.class, CALLS_REAL_METHODS)) {
      auth.when(
              () ->
                  MetadataAuthzHelper.filterByExpression(
                      eq("demo"),
                      eq(AuthorizationExpressionConstants.LOAD_TAG_AUTHORIZATION_EXPRESSION),
                      eq(EntityType.TAG),
                      any(TagDTO[].class),
                      any()))
          .thenReturn(new TagDTO[0]);
      auth.when(
              () ->
                  MetadataAuthzHelper.filterByExpression(
                      eq("demo"),
                      eq(AuthorizationExpressionConstants.LOAD_POLICY_AUTHORIZATION_EXPRESSION),
                      eq(EntityType.POLICY),
                      any(PolicyEntity[].class),
                      any()))
          .thenReturn(new PolicyEntity[0]);
      try (Response response = directPost(kind)) {
        Assertions.assertEquals(200, response.getStatus());
        BaseResponse result = (BaseResponse) response.getEntity();
        result.validate();
        JsonNode body = JsonUtils.objectMapper().valueToTree(result);
        String flag = kind.equals("tags") ? "hasInaccessibleTags" : "hasInaccessiblePolicies";
        Assertions.assertEquals(0, body.path("results").get(0).path(kind).path(kind).size());
        Assertions.assertTrue(body.path("results").get(0).path(kind).path(flag).asBoolean());
        Assertions.assertFalse(body.path("results").get(1).path(kind).path(flag).asBoolean());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"tags", "policies"})
  public void testFailureInOneKindDoesNotPreventOtherRead(String kind) throws Exception {
    if (kind.equals("tags")) {
      when(TAGS.listEffectiveTagsForDisplay(eq("demo"), any()))
          .thenThrow(new NoSuchMetadataObjectException("Object was removed"));
    } else {
      when(POLICIES.listPolicyInfosForDisplay(eq("demo"), any()))
          .thenThrow(new NoSuchMetadataObjectException("Object was removed"));
    }
    try (Response response = post(kind, REQUEST)) {
      Assertions.assertEquals(200, response.getStatus());
      JsonNode body = JsonUtils.objectMapper().readTree(response.readEntity(String.class));
      Assertions.assertEquals(0, body.path("results").size());
      Assertions.assertEquals(2, body.path("errors").size());
      Assertions.assertEquals(0, body.path("errors").get(0).path("index").asInt());
      Assertions.assertEquals(1, body.path("errors").get(1).path("index").asInt());
    }
    try (Response response = post(kind.equals("tags") ? "policies" : "tags", REQUEST)) {
      Assertions.assertEquals(200, response.getStatus());
    }
  }

  @Test
  public void testMissingMetalakeFailsRequestWithoutPerObjectRetries() throws Exception {
    when(TAGS.listEffectiveTagsForDisplay(eq("demo"), any()))
        .thenThrow(new NoSuchMetalakeException("Metalake demo does not exist"));
    try (Response response = post("tags", REQUEST)) {
      Assertions.assertEquals(404, response.getStatus());
    }
    verify(TAGS, times(1)).listEffectiveTagsForDisplay(eq("demo"), any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"tags", "policies"})
  public void testOneObjectFailurePreservesSuccessfulResults(String kind) throws Exception {
    if (kind.equals("tags")) {
      Map<MetadataObject, Tag[]> data = TAGS.listEffectiveTagsForDisplay("demo", List.of(ONE));
      reset(TAGS);
      when(TAGS.listEffectiveTagsForDisplay(eq("demo"), any()))
          .thenAnswer(
              invocation -> {
                List<MetadataObject> objects = invocation.getArgument(1);
                if (objects.contains(TWO)) {
                  throw new RuntimeException("Missing external metadata file");
                }
                return data;
              });
    } else {
      Map<MetadataObject, PolicyEntity[]> data =
          POLICIES.listPolicyInfosForDisplay("demo", List.of(ONE));
      reset(POLICIES);
      when(POLICIES.listPolicyInfosForDisplay(eq("demo"), any()))
          .thenAnswer(
              invocation -> {
                List<MetadataObject> objects = invocation.getArgument(1);
                if (objects.contains(TWO)) {
                  throw new RuntimeException("Missing external metadata file");
                }
                return data;
              });
    }
    try (Response response = directPost(kind)) {
      Assertions.assertEquals(200, response.getStatus());
      BaseResponse result = (BaseResponse) response.getEntity();
      result.validate();
      JsonNode body = JsonUtils.objectMapper().valueToTree(result);
      Assertions.assertEquals(1, body.path("results").size());
      Assertions.assertEquals(
          "catalog.one",
          body.path("results").get(0).path("metadataObject").path("fullName").asText());
      Assertions.assertEquals(1, body.path("results").get(0).path(kind).path(kind).size());
      Assertions.assertEquals(1, body.path("errors").size());
      JsonNode error = body.path("errors").get(0);
      Assertions.assertEquals(1, error.path("index").asInt());
      Assertions.assertEquals("catalog.two", error.path("name").asText());
      Assertions.assertEquals(1002, error.path("code").asInt());
      Assertions.assertEquals("Missing external metadata file", error.path("message").asText());
    }
    if (kind.equals("tags")) {
      verify(TAGS).listEffectiveTagsForDisplay("demo", List.of(ONE, TWO));
      verify(TAGS).listEffectiveTagsForDisplay("demo", List.of(ONE));
      verify(TAGS).listEffectiveTagsForDisplay("demo", List.of(TWO));
    } else {
      verify(POLICIES).listPolicyInfosForDisplay("demo", List.of(ONE, TWO));
      verify(POLICIES).listPolicyInfosForDisplay("demo", List.of(ONE));
      verify(POLICIES).listPolicyInfosForDisplay("demo", List.of(TWO));
    }
    verifyNoMoreInteractions(TAGS, POLICIES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"tags", "policies"})
  public void testSingleFailureReturnsValidErrorWithoutRetry(String kind) throws Exception {
    if (kind.equals("tags")) {
      when(TAGS.listEffectiveTagsForDisplay(eq("demo"), any())).thenThrow(new RuntimeException());
    } else {
      when(POLICIES.listPolicyInfosForDisplay(eq("demo"), any())).thenThrow(new RuntimeException());
    }
    String request = "{\"metadataObjects\":[{\"type\":\"SCHEMA\",\"fullName\":\"catalog.one\"}]}";
    try (Response response = post(kind, request)) {
      Assertions.assertEquals(200, response.getStatus());
      String body = response.readEntity(String.class);
      BaseResponse decoded =
          kind.equals("tags")
              ? JsonUtils.objectMapper().readValue(body, BulkMetadataObjectTagListResponse.class)
              : JsonUtils.objectMapper()
                  .readValue(body, BulkMetadataObjectPolicyListResponse.class);
      decoded.validate();
      JsonNode json = JsonUtils.objectMapper().readTree(body);
      Assertions.assertEquals(0, json.path("results").size());
      Assertions.assertEquals(1, json.path("errors").size());
      Assertions.assertEquals(
          "RuntimeException", json.path("errors").get(0).path("message").asText());
    }
    if (kind.equals("tags")) {
      verify(TAGS).listEffectiveTagsForDisplay("demo", List.of(ONE));
    } else {
      verify(POLICIES).listPolicyInfosForDisplay("demo", List.of(ONE));
    }
    verifyNoMoreInteractions(TAGS, POLICIES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"tags", "policies"})
  public void testAllDeniedObjectsAreNotLoaded(String kind) throws Exception {
    try (MockedStatic<MetadataAuthzHelper> auth =
        mockStatic(MetadataAuthzHelper.class, CALLS_REAL_METHODS)) {
      auth.when(
              () ->
                  MetadataAuthzHelper.filterMetadataObject(
                      eq("demo"), any(MetadataObjectDTO[].class)))
          .thenReturn(new MetadataObjectDTO[0]);
      try (Response response = directPost(kind)) {
        Assertions.assertEquals(200, response.getStatus());
        BaseResponse result = (BaseResponse) response.getEntity();
        result.validate();
        JsonNode body = JsonUtils.objectMapper().valueToTree(result);
        Assertions.assertEquals(0, body.path("results").size());
        Assertions.assertEquals(2, body.path("errors").size());
      }
      verifyNoInteractions(TAGS, POLICIES);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"tags", "policies"})
  public void testAuthorizedUserCanLoadBatchWithAuthorizationEnabled(String kind) throws Exception {
    Config config = GravitinoEnv.getInstance().config();
    doReturn(true).when(config).get(ENABLE_AUTHORIZATION);
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.isOwner(any(), eq("demo"), any(), any())).thenReturn(true);
    GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
    when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);
    try (MockedStatic<GravitinoAuthorizerProvider> providers =
        mockStatic(GravitinoAuthorizerProvider.class)) {
      providers.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      try (Response response = directPost(kind)) {
        Assertions.assertEquals(200, response.getStatus());
        BaseResponse result = (BaseResponse) response.getEntity();
        result.validate();
        JsonNode body = JsonUtils.objectMapper().valueToTree(result);
        Assertions.assertEquals(2, body.path("results").size());
      }
    } finally {
      doReturn(false).when(config).get(ENABLE_AUTHORIZATION);
    }
  }

  @Test
  public void testCombinedEndpointIsNotExposed() {
    try (Response response = post("associations", REQUEST)) {
      Assertions.assertEquals(404, response.getStatus());
    }
  }

  private Response directPost(String kind) throws Exception {
    BulkMetadataObjectsRequest request =
        JsonUtils.objectMapper().readValue(REQUEST, BulkMetadataObjectsRequest.class);
    if (kind.equals("tags")) {
      BulkMetadataObjectTagOperations operations = new BulkMetadataObjectTagOperations(TAGS);
      FieldUtils.writeField(operations, "httpRequest", mock(HttpServletRequest.class), true);
      return operations.listTags("demo", request);
    }
    BulkMetadataObjectPolicyOperations operations =
        new BulkMetadataObjectPolicyOperations(POLICIES);
    FieldUtils.writeField(operations, "httpRequest", mock(HttpServletRequest.class), true);
    return operations.listPolicies("demo", request);
  }

  private Response post(String kind, String request) {
    return target(PATH + kind)
        .request()
        .accept("application/vnd.gravitino.v1+json")
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));
  }
}
