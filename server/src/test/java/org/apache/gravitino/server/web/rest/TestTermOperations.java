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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener;
import java.io.IOException;
import java.util.Arrays;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.dto.glossary.TermDTO;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.TermListResponse;
import org.apache.gravitino.dto.responses.TermResponse;
import org.apache.gravitino.dto.responses.TermTreeResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.exceptions.TermVersionConflictException;
import org.apache.gravitino.glossary.GlossaryManager;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.glossary.GlossaryTermViews;
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
    when(dispatcher.get("test", name, null)).thenReturn(term);
    when(dispatcher.alter(eq("test"), eq(name), eq("token"), any())).thenReturn(term);
    when(dispatcher.drop("test", name)).thenReturn(true);
    when(dispatcher.hierarchy("test", name, GlossaryManager.Direction.NARROWER, null))
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
  void testCreateAndFilter() throws Exception {
    TermDTO term = JsonUtils.objectMapper().readValue("{\"name\":\"Term\"}", TermDTO.class);
    when(dispatcher.create("test", TermSpec.builder("Term").build())).thenReturn(term);
    when(dispatcher.list("test", "Sales", "erm", GlossaryTerm.Status.APPROVED))
        .thenReturn(new TermDTO[] {term});
    when(dispatcher.names("test", "Sales", "erm", GlossaryTerm.Status.APPROVED))
        .thenReturn(new String[] {"Term"});
    try (Response r = target(BASE).request().post(Entity.json("{\"name\":\"Term\"}"))) {
      assertEquals(200, r.getStatus());
      assertTrue(MediaType.APPLICATION_JSON_TYPE.isCompatible(r.getMediaType()));
      assertEquals("Term", r.readEntity(TermResponse.class).getTerm().getName());
    }
    for (boolean details : new boolean[] {false, true}) {
      try (Response r =
          target(BASE)
              .queryParam("status", "APPROVED")
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
    when(dispatcher.get("test", "T", null)).thenThrow(new NoSuchTermException("hidden"));
    try (Response r = target(item).request().get()) {
      assertError(r, 404, "NoSuchTermException");
    }
    when(dispatcher.drop("test", "T")).thenThrow(new ForbiddenException("denied"));
    try (Response r = target(item).request().delete()) {
      assertError(r, 403, "ForbiddenException");
    }
    when(dispatcher.create(eq("test"), any()))
        .thenThrow(new TermAlreadyExistsException("duplicate"));
    try (Response r = target(BASE).request().post(Entity.json("{\"name\":\"T\"}"))) {
      assertError(r, 409, "TermAlreadyExistsException");
    }
    when(dispatcher.alter(eq("test"), eq("T"), eq("old"), any()))
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
  void testUnsupportedListFiltersAndMissingBodiesAreRejectedBeforeDispatch() {
    try (Response r = target(BASE).queryParam("match", "revenue").request().get()) {
      assertEquals(400, r.getStatus());
    }
    try (Response r = target(BASE).queryParam("bindings", "true").request().get()) {
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
    when(dispatcher.drop("test", "T")).thenThrow(new OptimisticLockException("Term changed"));
    try (Response response = target(BASE + "/T").request().delete()) {
      assertEquals(409, response.getStatus());
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals(ErrorConstants.OPTIMISTIC_LOCK_CONFLICT_CODE, error.getCode());
      assertEquals("OptimisticLockException", error.getType());
    }
  }

  @Test
  void testDeleteMetricsCountSuccessAndConflict() {
    when(dispatcher.drop("test", "T"))
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
            target(item + "/hierarchy").request().get())) {
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
            target(item + "/hierarchy").request().get())) {
      try (r) {
        assertError(r, 404, NoSuchMetalakeException.class.getSimpleName());
      }
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  void testInvalidEnumParametersExplainAllowedValues() {
    try (Response r = target(BASE + "/T").queryParam("status", "approved").request().get()) {
      assertEquals(400, r.getStatus());
      String message = r.readEntity(ErrorResponse.class).getMessage();
      assertTrue(message.contains("[DRAFT, APPROVED, DEPRECATED]"), message);
    }
    try (Response r =
        target(BASE + "/T/hierarchy").queryParam("direction", "narrower").request().get()) {
      assertEquals(400, r.getStatus());
      String message = r.readEntity(ErrorResponse.class).getMessage();
      assertTrue(message.contains("expected BROADER or NARROWER"), message);
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
