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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
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
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.dto.glossary.TermDTO;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.ObjectTermsResponse;
import org.apache.gravitino.dto.responses.TermListResponse;
import org.apache.gravitino.dto.responses.TermResponse;
import org.apache.gravitino.dto.responses.TermTreeResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.LookupTooBroadException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.exceptions.TermVersionConflictException;
import org.apache.gravitino.glossary.GlossaryManager;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.glossary.GlossaryTermViews;
import org.apache.gravitino.glossary.TermBinding;
import org.apache.gravitino.glossary.TermSpec;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.metrics.MetricNames;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestTermOperations extends BaseOperationsTest {

  private static final String BASE = "/metalakes/test/terms";
  private static final String TABLE_TERMS = "/metalakes/test/objects/table/dw.sales.table/terms";
  private static final String METRIC_TERMS =
      "/metalakes/test/objects/semantic_model/dw.sales.model/metrics/revenue%20net/terms";
  private static final String ACCEPT_V1 = "application/vnd.gravitino.v1+json";
  // The production servlet filter passes this header to Jersey for an application/json request.
  private static final String NORMALIZED_JSON_ACCEPT = ACCEPT_V1 + ", application/json";
  private MetricRegistry metrics;
  private final GlossaryTermViews dispatcher = mock(GlossaryTermViews.class);
  private final EntityStore entityStore = mock(EntityStore.class);
  private Object previousStore;

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      return mock(HttpServletRequest.class);
    }
  }

  @BeforeEach
  void setUpMetalake() throws Exception {
    previousStore = FieldUtils.readField(GravitinoEnv.getInstance(), "entityStore", true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", entityStore, true);
    setMetalakeInUse(true);
  }

  @AfterEach
  void restoreEntityStore() throws Exception {
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
    metrics = new MetricRegistry();
    return new ResourceConfig(
            TermOperations.class,
            MetadataObjectTermOperations.class,
            JsonProcessingExceptionMapper.class,
            JsonMappingExceptionMapper.class)
        .register(new ObjectMapperProvider())
        .register(new InstrumentedResourceMethodApplicationListener(metrics))
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(dispatcher).to(GlossaryTermViews.class);
                bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
              }
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"resolve", "a/b"})
  void testLiteralNames(String name) throws Exception {
    TermDTO term =
        JsonUtils.objectMapper()
            .readValue(
                "{\"name\":\""
                    + name
                    + "\",\"status\":\"APPROVED\",\"version\":1,\"revisionToken\":\"token\"}",
                TermDTO.class);
    String item = BASE + "/" + name.replace("/", "%2F");
    when(dispatcher.getTerm("test", name, null)).thenReturn(term);
    when(dispatcher.alterTerm(eq("test"), eq(name), eq("token"), any())).thenReturn(term);
    when(dispatcher.dropTerm("test", name)).thenReturn(true);
    when(dispatcher.getTermHierarchy("test", name, GlossaryManager.Direction.NARROWER, null))
        .thenReturn(new TermDTO[] {term});
    try (Response r = target(item).request().get()) {
      assertEquals(200, r.getStatus());
      assertTrue(MediaType.APPLICATION_JSON_TYPE.isCompatible(r.getMediaType()));
      assertEquals(name, r.readEntity(TermResponse.class).getTerm().getName());
    }
    try (Response r =
        target(item)
            .request()
            .put(
                Entity.json(
                    "{\"expectedRevision\":\"token\",\"updates\":[{\"@type\":\"setProperty\",\"property\":\"x\",\"value\":\"y\"}]}"))) {
      assertEquals(200, r.getStatus());
      assertTrue(MediaType.APPLICATION_JSON_TYPE.isCompatible(r.getMediaType()));
      assertEquals("token", r.readEntity(TermResponse.class).getTerm().getRevisionToken());
    }
    try (Response r = target(item + "/hierarchy").request().get()) {
      assertEquals(200, r.getStatus());
      assertTrue(MediaType.APPLICATION_JSON_TYPE.isCompatible(r.getMediaType()));
      assertEquals(name, r.readEntity(TermTreeResponse.class).getTerms()[0].getName());
    }
    try (Response r = target(item).request().delete()) {
      assertEquals(200, r.getStatus());
      assertTrue(MediaType.APPLICATION_JSON_TYPE.isCompatible(r.getMediaType()));
      assertTrue(r.readEntity(DropResponse.class).dropped());
    }
  }

  @Test
  void testListDefaultsToNamesAcrossVisibleStatuses() {
    when(dispatcher.listTermNames("test", null, null, null, List.of()))
        .thenReturn(new String[] {"Draft term"});
    try (Response response = target(BASE).request(ACCEPT_V1).get()) {
      assertEquals(200, response.getStatus());
      assertTrue(MediaType.valueOf(ACCEPT_V1).isCompatible(response.getMediaType()));
      assertArrayEquals(
          new String[] {"Draft term"}, response.readEntity(NameListResponse.class).getNames());
    }
  }

  @Test
  void testListTermsQueryDefaultsAndDirectCall() throws Exception {
    when(dispatcher.listTermNames("test", null, null, null, List.of()))
        .thenReturn(new String[] {"Draft term"});
    when(dispatcher.listTerms(
            "test", "Sales", "rev", GlossaryTerm.Status.APPROVED, List.of(), false))
        .thenReturn(new TermDTO[0]);
    TermOperations ops = new TermOperations(dispatcher);
    FieldUtils.writeField(ops, "httpRequest", mock(HttpServletRequest.class), true);

    TermOperations.ListTermsQuery defaults = new TermOperations.ListTermsQuery();
    assertEquals("false", defaults.getDetails());
    try (Response r = ops.listTerms("test", defaults)) {
      assertEquals(200, r.getStatus());
    }
    try (Response r = ops.listTerms("test", null)) {
      assertEquals(200, r.getStatus());
    }
    TermOperations.ListTermsQuery filtered =
        new TermOperations.ListTermsQuery()
            .withDetails("true")
            .withDomain("Sales")
            .withStatus("approved")
            .withNameContains("rev");
    assertEquals("true", filtered.getDetails());
    assertEquals("Sales", filtered.getDomain());
    assertEquals("approved", filtered.getStatus());
    assertEquals("rev", filtered.getNameContains());
    try (Response r = ops.listTerms("test", filtered)) {
      assertEquals(200, r.getStatus());
    }
    when(dispatcher.getTermHierarchy("test", "T", GlossaryManager.Direction.NARROWER, null))
        .thenReturn(new TermDTO[0]);
    try (Response r = ops.getTermHierarchy("test", "T", null, null)) {
      assertEquals(200, r.getStatus());
    }
  }

  @Test
  void testCreateAndFilter() throws Exception {
    TermDTO term = JsonUtils.objectMapper().readValue("{\"name\":\"Term\"}", TermDTO.class);
    when(dispatcher.createTerm("test", TermSpec.builder("Term").build())).thenReturn(term);
    when(dispatcher.listTerms(
            "test", "Sales", "erm", GlossaryTerm.Status.APPROVED, List.of(), false))
        .thenReturn(new TermDTO[] {term});
    when(dispatcher.listTermNames("test", "Sales", "erm", GlossaryTerm.Status.APPROVED, List.of()))
        .thenReturn(new String[] {"Term"});
    try (Response r = target(BASE).request().post(Entity.json("{\"name\":\"Term\"}"))) {
      assertEquals(200, r.getStatus());
      assertTrue(MediaType.APPLICATION_JSON_TYPE.isCompatible(r.getMediaType()));
      assertEquals("Term", r.readEntity(TermResponse.class).getTerm().getName());
    }
    for (boolean details : new boolean[] {false, true}) {
      try (Response r =
          target(BASE)
              .queryParam("status", "approved")
              .queryParam("domain", "Sales")
              .queryParam("nameContains", "erm")
              .queryParam("details", details)
              .request()
              .get()) {
        assertEquals(200, r.getStatus());
        assertTrue(MediaType.APPLICATION_JSON_TYPE.isCompatible(r.getMediaType()));
        assertEquals(
            "Term",
            details
                ? r.readEntity(TermListResponse.class).getTerms()[0].getName()
                : r.readEntity(NameListResponse.class).getNames()[0]);
      }
    }
  }

  @Test
  void testErrors() {
    String item = BASE + "/T";
    for (String body :
        new String[] {"{}", "{\"updates\":[]}", "{\"updates\":[{\"@type\":\"bogus\"}]}"}) {
      try (Response r = target(item).request().put(Entity.json(body))) {
        assertEquals(400, r.getStatus());
      }
    }
    try (Response r = target(item).queryParam("status", "bogus").request().get()) {
      assertEquals(400, r.getStatus());
    }
    try (Response r =
        target(item + "/hierarchy").queryParam("direction", "RELATED").request().get()) {
      assertEquals(400, r.getStatus());
    }
    when(dispatcher.getTerm("test", "T", null)).thenThrow(new NoSuchTermException("hidden"));
    try (Response r = target(item).request().get()) {
      assertError(r, 404, "NoSuchTermException");
    }
    when(dispatcher.dropTerm("test", "T")).thenThrow(new ForbiddenException("denied"));
    try (Response r = target(item).request().delete()) {
      assertError(r, 403, "ForbiddenException");
    }
    when(dispatcher.createTerm(eq("test"), any()))
        .thenThrow(new TermAlreadyExistsException("duplicate"));
    try (Response r = target(BASE).request().post(Entity.json("{\"name\":\"T\"}"))) {
      assertError(r, 409, "TermAlreadyExistsException");
    }
    when(dispatcher.alterTerm(eq("test"), eq("T"), eq("old"), any()))
        .thenThrow(new TermVersionConflictException("stale"));
    try (Response r =
        target(item)
            .request()
            .put(
                Entity.json(
                    "{\"expectedRevision\":\"old\",\"updates\":[{\"@type\":\"rename\",\"newName\":\"X\"}]}"))) {
      assertError(r, 409, "TermVersionConflictException");
    }
  }

  @Test
  void testExplicitNullRevisionIsRejectedBeforeDispatch() {
    try (Response response =
        target(BASE + "/T")
            .request()
            .put(
                Entity.json(
                    "{\"expectedRevision\":null,\"updates\":[{\"@type\":\"rename\",\"newName\":\"New\"}]}"))) {
      assertEquals(400, response.getStatus());
      assertEquals(
          ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
          response.readEntity(ErrorResponse.class).getCode());
      verifyNoInteractions(dispatcher);
    }
  }

  @Test
  void testLookupWordValidation() {
    for (List<String> words : List.of(List.of("  "), Collections.nCopies(21, "one"))) {
      when(dispatcher.listTermNames("test", null, null, null, words)).thenCallRealMethod();
      try (Response r = target(BASE).queryParam("match", words.toArray()).request().get()) {
        assertEquals(400, r.getStatus());
        assertEquals("IllegalArgumentException", r.readEntity(ErrorResponse.class).getType());
      }
    }
  }

  @Test
  void testLookupWireFormatAndWorkLimit() throws Exception {
    List<String> words = List.of(" Active  user ", "Customer");
    String matched =
        "{\"name\":\"Customer\",\"matchedBy\":[{\"word\":\" Active  user \",\"field\":\"SYNONYM\"},{\"word\":\"Customer\",\"field\":\"NAME\"}]";
    TermDTO definition = JsonUtils.objectMapper().readValue(matched + "}", TermDTO.class);
    TermDTO expanded =
        JsonUtils.objectMapper()
            .readValue(matched + ",\"bindings\":[],\"bindingsTruncated\":false}", TermDTO.class);
    when(dispatcher.listTermNames("test", null, null, null, words))
        .thenReturn(new String[] {"Customer"});
    when(dispatcher.listTerms("test", null, null, null, words, false))
        .thenReturn(new TermDTO[] {definition});
    String message = "Glossary lookup is too broad. Omit embedded bindings or narrow the query.";
    when(dispatcher.listTerms("test", null, null, null, words, true))
        .thenReturn(new TermDTO[] {expanded})
        .thenThrow(new LookupTooBroadException(message));
    WebTarget lookup = target(BASE).queryParam("match", words.toArray());
    try (Response r = lookup.request().get()) {
      assertEquals(200, r.getStatus());
      assertArrayEquals(new String[] {"Customer"}, r.readEntity(NameListResponse.class).getNames());
    }
    WebTarget details = lookup.queryParam("details", true);
    for (boolean bindings : List.of(false, true)) {
      try (Response r = details.queryParam("bindings", bindings).request().get()) {
        assertEquals(200, r.getStatus());
        JsonNode root =
            JsonUtils.objectMapper().readTree(r.readEntity(String.class)).path("terms").get(0);
        assertEquals(words.get(0), root.path("matchedBy").get(0).path("word").asText());
        assertEquals("SYNONYM", root.path("matchedBy").get(0).path("field").asText());
        assertEquals("NAME", root.path("matchedBy").get(1).path("field").asText());
        assertEquals(bindings, root.has("bindings"));
        assertEquals(bindings, root.has("bindingsTruncated"));
        if (bindings) {
          assertTrue(root.path("bindings").isArray() && root.path("bindings").isEmpty());
          assertTrue(root.path("bindingsTruncated").isBoolean());
          assertFalse(root.path("bindingsTruncated").booleanValue());
        }
      }
    }
    try (Response r = details.queryParam("bindings", true).request().get()) {
      assertEquals(400, r.getStatus());
      ErrorResponse error = r.readEntity(ErrorResponse.class);
      assertEquals("LookupTooBroadException", error.getType());
      assertEquals(message, error.getMessage());
    }
  }

  @Test
  void testInvalidListFiltersAndMissingBodiesAreRejectedBeforeDispatch() {
    try (Response r = target(BASE).queryParam("details", "yes").request().get()) {
      assertEquals(400, r.getStatus());
      assertTrue(r.readEntity(ErrorResponse.class).getMessage().contains("true or false"));
    }
    try (Response r = target(BASE).queryParam("bindings", "yes").request().get()) {
      assertEquals(400, r.getStatus());
    }
    try (Response r =
        target(BASE).queryParam("bindings", "true").queryParam("details", true).request().get()) {
      assertEquals(400, r.getStatus());
    }
    try (Response r =
        target(BASE).queryParam("bindings", "true").queryParam("match", "T").request().get()) {
      assertEquals(400, r.getStatus());
    }
    try (Response r = target(BASE).request().post(Entity.json(""))) {
      assertEquals(400, r.getStatus());
    }
    try (Response r = target(BASE + "/T").request().put(Entity.json(""))) {
      assertEquals(400, r.getStatus());
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  void testStorageConflictPreservesErrorTypeAndCode() {
    when(dispatcher.dropTerm("test", "T")).thenThrow(new OptimisticLockException("Term changed"));
    try (Response response = target(BASE + "/T").request().delete()) {
      assertEquals(409, response.getStatus());
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals(ErrorConstants.OPTIMISTIC_LOCK_CONFLICT_CODE, error.getCode());
      assertEquals("OptimisticLockException", error.getType());
    }
  }

  @Test
  void testDeleteMetricsCountSuccessAndConflict() {
    when(dispatcher.dropTerm("test", "T"))
        .thenReturn(true)
        .thenThrow(new OptimisticLockException("changed"));
    for (int status : new int[] {200, 409}) {
      try (Response response = target(BASE + "/T").request().delete()) {
        assertEquals(status, response.getStatus());
      }
    }
    await()
        .untilAsserted(
            () -> {
              assertEquals(
                  2L,
                  metrics
                      .getTimers()
                      .get("delete-term." + MetricNames.HTTP_PROCESS_DURATION)
                      .getCount());
              assertEquals(1L, metrics.getMeters().get("delete-term.2xx-responses").getCount());
              assertEquals(1L, metrics.getMeters().get("delete-term.4xx-responses").getCount());
            });
  }

  @Test
  void testDisabledMetalakeRejectsEveryEndpointBeforeDispatch() throws Exception {
    setMetalakeInUse(false);
    String item = BASE + "/T";
    String update = "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"X\"}]}";
    for (Response r :
        Arrays.asList(
            target(BASE).request().get(),
            target(BASE).queryParam("details", true).request().get(),
            target(BASE).request().post(Entity.json("{\"name\":\"T\"}")),
            target(item).request().get(),
            target(item).request().put(Entity.json(update)),
            target(item).request().delete(),
            target(item + "/hierarchy").request().get(),
            target(item + "/objects").request().get(),
            target(TABLE_TERMS).queryParam("children", true).request().get(),
            target(TABLE_TERMS).request().post(Entity.json("{\"termsToRemove\":[]}")),
            target(METRIC_TERMS).request().get(),
            target(METRIC_TERMS).request().post(Entity.json("{\"termsToRemove\":[]}")))) {
      try (r) {
        assertEquals(409, r.getStatus());
        ErrorResponse error = r.readEntity(ErrorResponse.class);
        assertEquals(ErrorConstants.NOT_IN_USE_CODE, error.getCode());
        assertEquals("MetalakeNotInUseException", error.getType());
      }
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  void testMissingMetalakeReturnsNotFoundForEveryEndpoint() throws Exception {
    doThrow(new NoSuchEntityException("missing")).when(entityStore).get(any(), any(), any());
    String item = BASE + "/T";
    for (Response r :
        Arrays.asList(
            target(BASE).request().get(),
            target(BASE).request().post(Entity.json("{\"name\":\"T\"}")),
            target(item).request().get(),
            target(item)
                .request()
                .put(Entity.json("{\"updates\":[{\"@type\":\"rename\",\"newName\":\"X\"}]}")),
            target(item).request().delete(),
            target(item + "/hierarchy").request().get(),
            target(item + "/objects").request().get(),
            target(TABLE_TERMS).queryParam("children", true).request().get(),
            target(TABLE_TERMS).request().post(Entity.json("{\"termsToRemove\":[]}")),
            target(METRIC_TERMS).request().get(),
            target(METRIC_TERMS).request().post(Entity.json("{\"termsToRemove\":[]}")))) {
      try (r) {
        assertError(r, 404, NoSuchMetalakeException.class.getSimpleName());
      }
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  void testInvalidEnumParametersExplainAllowedValues() {
    try (Response r = target(BASE + "/T").queryParam("status", "unknown").request().get()) {
      assertEquals(400, r.getStatus());
      String message = r.readEntity(ErrorResponse.class).getMessage();
      assertTrue(message.contains("[DRAFT, APPROVED, DEPRECATED]"), message);
    }
    try (Response r =
        target(BASE + "/T/hierarchy").queryParam("direction", "sideways").request().get()) {
      assertEquals(400, r.getStatus());
      String message = r.readEntity(ErrorResponse.class).getMessage();
      assertTrue(message.contains("expected BROADER or NARROWER"), message);
    }
    try (Response r = target(BASE + "/T/hierarchy").queryParam("direction", "").request().get()) {
      assertEquals(400, r.getStatus());
      String message = r.readEntity(ErrorResponse.class).getMessage();
      assertTrue(message.contains("expected BROADER or NARROWER"), message);
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  void testCaseInsensitiveHierarchyDirection() throws Exception {
    TermDTO term =
        JsonUtils.objectMapper()
            .readValue(
                "{\"name\":\"Child\",\"status\":\"APPROVED\",\"version\":1,\"revisionToken\":\"token\"}",
                TermDTO.class);
    when(dispatcher.getTermHierarchy("test", "T", GlossaryManager.Direction.NARROWER, null))
        .thenReturn(new TermDTO[] {term});
    when(dispatcher.getTermHierarchy(
            "test", "T", GlossaryManager.Direction.BROADER, GlossaryTerm.Status.APPROVED))
        .thenReturn(new TermDTO[] {term});
    try (Response r =
        target(BASE + "/T/hierarchy")
            .queryParam("direction", "narrower")
            .request(ACCEPT_V1)
            .get()) {
      assertEquals(200, r.getStatus());
      assertEquals("Child", r.readEntity(TermTreeResponse.class).getTerms()[0].getName());
    }
    try (Response r =
        target(BASE + "/T/hierarchy")
            .queryParam("direction", "Broader")
            .queryParam("status", "approved")
            .request(ACCEPT_V1)
            .get()) {
      assertEquals(200, r.getStatus());
      assertEquals("Child", r.readEntity(TermTreeResponse.class).getTerms()[0].getName());
    }
  }

  @Test
  void testBlankQueryFiltersAndNullCreateFieldsAreRejectedBeforeDispatch() {
    for (Response r :
        Arrays.asList(
            target(BASE).queryParam("domain", "").request(ACCEPT_V1).get(),
            target(BASE).queryParam("domain", "   ").request(ACCEPT_V1).get(),
            target(BASE).queryParam("nameContains", "").request(ACCEPT_V1).get(),
            target(BASE).queryParam("nameContains", "   ").request(ACCEPT_V1).get(),
            target(BASE).request(ACCEPT_V1).post(Entity.json("{\"name\":\"T\",\"status\":null}")),
            target(BASE)
                .request(ACCEPT_V1)
                .post(Entity.json("{\"name\":\"T\",\"properties\":null}")),
            target(BASE).request(ACCEPT_V1).post(Entity.json("{\"name\":\"T\",\"synonyms\":null}")),
            target(BASE + "/T")
                .request(ACCEPT_V1)
                .put(
                    Entity.json(
                        "{\"expectedRevision\":\"\",\"updates\":[{\"@type\":\"rename\",\"newName\":\"New\"}]}")),
            target(BASE + "/T")
                .request(ACCEPT_V1)
                .put(
                    Entity.json(
                        "{\"expectedRevision\":\"   \",\"updates\":[{\"@type\":\"rename\",\"newName\":\"New\"}]}")))) {
      try (r) {
        assertEquals(400, r.getStatus());
      }
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  void testReverseReadsAndWrites() throws Exception {
    MetadataObject table = MetadataObjects.parse("dw.sales.table", MetadataObject.Type.TABLE);
    MetadataObject model =
        MetadataObjects.parse("dw.sales.model", MetadataObject.Type.SEMANTIC_MODEL);
    String[] names = {"Revenue"};
    TermDTO term =
        JsonUtils.objectMapper()
            .readValue(
                "{\"name\":\"Revenue\",\"status\":\"APPROVED\",\"version\":1,\"revisionToken\":\"v\"}",
                TermDTO.class);
    ObjectTermsResponse detail =
        new ObjectTermsResponse(
            new TermDTO[] {term}, List.of(new ObjectTermsResponse.Child("amount", List.of(term))));
    when(dispatcher.listTargetTermNames("test", table, null, null)).thenReturn(names);
    when(dispatcher.listTargetTermNames("test", model, "revenue net", null)).thenReturn(names);
    when(dispatcher.listTargetTermsWithChildren("test", table, GlossaryTerm.Status.APPROVED))
        .thenReturn(detail);
    when(dispatcher.listDirectTargetTerms("test", model, "revenue net", null))
        .thenReturn(new TermDTO[] {term});
    when(dispatcher.associateTerms("test", table, null, names, null)).thenReturn(names);
    when(dispatcher.associateTerms("test", model, "revenue net", names, null)).thenReturn(names);
    when(dispatcher.listDirectTargetTerms("test", table, null, null))
        .thenReturn(new TermDTO[] {term});
    try (Response r =
        target(TABLE_TERMS)
            .request()
            .post(Entity.json("{\"termsToAdd\":[\"Revenue\"],\"termsToRemove\":null}"))) {
      assertEquals(200, r.getStatus());
      assertArrayEquals(names, r.readEntity(NameListResponse.class).getNames());
    }
    try (Response r = target(TABLE_TERMS).queryParam("details", true).request().get()) {
      assertEquals(200, r.getStatus());
      ObjectTermsResponse direct = r.readEntity(ObjectTermsResponse.class);
      assertEquals("Revenue", direct.getTerms()[0].getName());
      assertTrue(direct.getChildren().isEmpty());
    }
    for (String path : List.of(TABLE_TERMS, METRIC_TERMS)) {
      for (String accept : List.of(MediaType.APPLICATION_JSON, ACCEPT_V1, NORMALIZED_JSON_ACCEPT)) {
        MediaType expected =
            accept.equals(ACCEPT_V1)
                ? MediaType.valueOf(ACCEPT_V1)
                : MediaType.APPLICATION_JSON_TYPE;
        try (Response r = target(path).request(accept).get()) {
          assertEquals(200, r.getStatus());
          assertTrue(expected.isCompatible(r.getMediaType()));
          assertArrayEquals(names, r.readEntity(NameListResponse.class).getNames());
        }
        try (Response r =
            target(path).request(accept).post(Entity.json("{\"termsToAdd\":[\"Revenue\"]}"))) {
          assertEquals(200, r.getStatus());
          assertTrue(expected.isCompatible(r.getMediaType()));
          assertArrayEquals(names, r.readEntity(NameListResponse.class).getNames());
        }
      }
    }
    try (Response r =
        target(TABLE_TERMS)
            .queryParam("children", true)
            .queryParam("status", "approved")
            .request()
            .get()) {
      assertEquals(200, r.getStatus());
      ObjectTermsResponse result = r.readEntity(ObjectTermsResponse.class);
      assertEquals("Revenue", result.getTerms()[0].getName());
      assertEquals("amount", result.getChildren().get(0).getName());
      assertEquals("Revenue", result.getChildren().get(0).getTerms().get(0).getName());
    }
    try (Response r = target(METRIC_TERMS).queryParam("details", true).request().get()) {
      assertEquals(200, r.getStatus());
      assertEquals("Revenue", r.readEntity(TermListResponse.class).getTerms()[0].getName());
    }
  }

  @Test
  void testListAllFilteredBindings() throws Exception {
    MetadataObject model =
        MetadataObjects.parse("dw.sales.model", MetadataObject.Type.SEMANTIC_MODEL);
    List<TermBinding> bindings =
        IntStream.range(0, 1001).mapToObj(i -> TermBinding.usedBy(model, "metric_" + i)).toList();
    when(dispatcher.listTermBindings(
            "test",
            "Revenue",
            TermBinding.Kind.USED_BY,
            Set.of(MetadataObject.Type.SEMANTIC_MODEL, MetadataObject.Type.TABLE),
            GlossaryTerm.Status.APPROVED))
        .thenReturn(bindings);
    try (Response r =
        target(BASE + "/Revenue/objects")
            .queryParam("kind", "used_by")
            .queryParam("types", "table,semantic_model")
            .queryParam("status", "approved")
            .request(NORMALIZED_JSON_ACCEPT)
            .get()) {
      assertEquals(200, r.getStatus());
      assertTrue(MediaType.APPLICATION_JSON_TYPE.isCompatible(r.getMediaType()));
      JsonNode response = JsonUtils.objectMapper().readTree(r.readEntity(String.class));
      assertEquals(0, response.get("code").intValue());
      assertEquals(1001, response.get("bindings").size());
      assertEquals("metric_1000", response.at("/bindings/1000/metric").textValue());
      assertFalse(response.has("nextPageToken"));
    }
  }

  @Test
  void testBindingJsonEnumValues() throws Exception {
    MetadataObject table = MetadataObjects.parse("dw.sales.table", MetadataObject.Type.TABLE);
    MetadataObject model =
        MetadataObjects.parse("dw.sales.model", MetadataObject.Type.SEMANTIC_MODEL);
    when(dispatcher.listTermBindings("test", "T", null, Set.of(), null))
        .thenReturn(
            List.of(TermBinding.implementedBy(table), TermBinding.usedBy(model, "revenue")));
    for (String accept : List.of(MediaType.APPLICATION_JSON, ACCEPT_V1)) {
      try (Response response = target(BASE + "/T/objects").request(accept).get()) {
        assertEquals(200, response.getStatus());
        JsonNode bindings =
            JsonUtils.objectMapper().readTree(response.readEntity(String.class)).get("bindings");
        assertEquals("implemented_by", bindings.at("/0/kind").textValue());
        assertEquals("table", bindings.at("/0/object/type").textValue());
        assertEquals("used_by", bindings.at("/1/kind").textValue());
        assertEquals("semantic_model", bindings.at("/1/object/type").textValue());
      }
    }
  }

  @Test
  void testAssociationRequiresJsonStrings() {
    for (String path : List.of(TABLE_TERMS, METRIC_TERMS)) {
      for (String body : List.of("{\"termsToAdd\":[5]}", "{\"termsToRemove\":[true]}")) {
        try (Response response = target(path).request().post(Entity.json(body))) {
          assertError(response, 400, IllegalArgumentException.class.getSimpleName());
        }
      }
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  void testHiddenRootAndDeniedOrMissingTarget() {
    MetadataObject table = MetadataObjects.parse("dw.sales.table", MetadataObject.Type.TABLE);
    MetadataObject model =
        MetadataObjects.parse("dw.sales.model", MetadataObject.Type.SEMANTIC_MODEL);
    when(dispatcher.listTermBindings(
            "test", "Private", null, Set.of(), GlossaryTerm.Status.APPROVED))
        .thenThrow(new NoSuchTermException("missing"));
    when(dispatcher.listTargetTermsWithChildren("test", table, null))
        .thenThrow(new ForbiddenException("denied"));
    when(dispatcher.listTargetTermNames("test", model, "revenue net", null))
        .thenThrow(new NoSuchMetadataObjectException("missing metric"));
    try (Response r =
        target(BASE + "/Private/objects").queryParam("status", "APPROVED").request().get()) {
      assertError(r, 404, "NoSuchTermException");
    }
    try (Response r = target(TABLE_TERMS).queryParam("children", true).request().get()) {
      assertError(r, 403, "ForbiddenException");
    }
    try (Response r = target(METRIC_TERMS).request().get()) {
      assertError(r, 404, "NoSuchMetadataObjectException");
    }
  }

  @Test
  void testBindingServerErrorsReturnJsonForVersionedClients() {
    MetadataObject table = MetadataObjects.parse("dw.sales.table", MetadataObject.Type.TABLE);
    when(dispatcher.listTermBindings("test", "T", null, Set.of(), null))
        .thenThrow(new IllegalStateException("binding store unavailable"));
    when(dispatcher.listTargetTermNames("test", table, null, null))
        .thenThrow(new IllegalStateException("binding store unavailable"));
    for (String path : List.of(BASE + "/T/objects", TABLE_TERMS)) {
      try (Response response = target(path).request(ACCEPT_V1).get()) {
        assertEquals(500, response.getStatus());
        assertTrue(MediaType.APPLICATION_JSON_TYPE.isCompatible(response.getMediaType()));
        ErrorResponse error = response.readEntity(ErrorResponse.class);
        assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, error.getCode());
        assertTrue(error.getMessage().contains("binding store unavailable"));
      }
    }
  }

  @Test
  void testInvalidBindingParametersAndBodies() {
    String table = "/metalakes/test/objects/table/dw.sales.table/terms";
    for (Response r :
        List.of(
            target(BASE + "/T/objects").queryParam("types", "unknown").request().get(),
            target("/metalakes/test/objects/view/dw.sales.v/terms")
                .queryParam("children", true)
                .request()
                .get(),
            target(table).queryParam("details", "sometimes").request().get(),
            target(table).request().post(Entity.json("{}")),
            target(table)
                .request()
                .post(Entity.json("{\"termsToAdd\":null,\"termsToRemove\":null}")),
            target(table).request().post(Entity.json("{\"termsToRemove\":[\"   \"]}")),
            target(table).request().post(Entity.json("{\"termsToAdd\":[null]}")))) {
      try (r) {
        assertEquals(400, r.getStatus());
      }
    }
    try (Response r = target(BASE + "/T/objects").queryParam("kind", "other").request().get()) {
      assertEquals(400, r.getStatus());
      assertTrue(
          r.readEntity(ErrorResponse.class).getMessage().contains("IMPLEMENTED_BY or USED_BY"));
    }
    verifyNoInteractions(dispatcher);
  }

  private void setMetalakeInUse(boolean inUse) throws Exception {
    BaseMetalake metalake = mock(BaseMetalake.class);
    PropertiesMetadata properties = mock(PropertiesMetadata.class);
    when(properties.getOrDefault(any(), any())).thenReturn(inUse);
    when(metalake.propertiesMetadata()).thenReturn(properties);
    when(entityStore.get(any(), any(), any())).thenReturn(metalake);
  }

  private static void assertError(Response response, int status, String type) {
    assertEquals(status, response.getStatus());
    assertEquals(type, response.readEntity(ErrorResponse.class).getType());
  }
}
