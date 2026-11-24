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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.glossary.TermDTO;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.TermListResponse;
import org.apache.gravitino.dto.responses.TermResponse;
import org.apache.gravitino.dto.responses.TermTreeResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.exceptions.TermVersionConflictException;
import org.apache.gravitino.glossary.GlossaryManager;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.glossary.GlossaryTermViews;
import org.apache.gravitino.glossary.TermSpec;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.server.web.mapper.JsonMappingExceptionMapper;
import org.apache.gravitino.server.web.mapper.JsonProcessingExceptionMapper;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestTermOperations extends BaseOperationsTest {

  private static final String BASE = "/metalakes/test/terms";
  private final GlossaryTermViews dispatcher = mock(GlossaryTermViews.class);

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      return mock(HttpServletRequest.class);
    }
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
            TermOperations.class,
            JsonProcessingExceptionMapper.class,
            JsonMappingExceptionMapper.class)
        .register(new ObjectMapperProvider())
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
    when(dispatcher.get("test", name)).thenReturn(term);
    when(dispatcher.alter(eq("test"), eq(name), eq("token"), any())).thenReturn(term);
    when(dispatcher.drop("test", name)).thenReturn(true);
    when(dispatcher.hierarchy("test", name, GlossaryManager.Direction.NARROWER))
        .thenReturn(new TermDTO[] {term});
    try (Response r = target(item).request().get()) {
      assertEquals(200, r.getStatus());
      assertEquals(name, r.readEntity(TermResponse.class).getTerm().getName());
    }
    try (Response r =
        target(item)
            .request()
            .put(
                Entity.json(
                    "{\"expectedRevision\":\"token\",\"updates\":[{\"@type\":\"setProperty\",\"property\":\"x\",\"value\":\"y\"}]}"))) {
      assertEquals(200, r.getStatus());
      assertEquals("token", r.readEntity(TermResponse.class).getTerm().getRevisionToken());
    }
    try (Response r = target(item + "/hierarchy").request().get()) {
      assertEquals(200, r.getStatus());
      assertEquals(name, r.readEntity(TermTreeResponse.class).getTerms()[0].getName());
    }
    try (Response r = target(item).request().delete()) {
      assertEquals(200, r.getStatus());
      assertTrue(r.readEntity(DropResponse.class).dropped());
    }
  }

  @Test
  void testCreateAndFilter() throws Exception {
    TermDTO term = JsonUtils.objectMapper().readValue("{\"name\":\"Term\"}", TermDTO.class);
    when(dispatcher.create("test", TermSpec.builder("Term").build())).thenReturn(term);
    when(dispatcher.withStatus(GlossaryTerm.Status.APPROVED)).thenReturn(dispatcher);
    when(dispatcher.list("test", "Sales", "erm")).thenReturn(new TermDTO[] {term});
    when(dispatcher.names("test", "Sales", "erm")).thenReturn(new String[] {"Term"});
    try (Response r = target(BASE).request().post(Entity.json("{\"name\":\"Term\"}"))) {
      assertEquals(200, r.getStatus());
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
    when(dispatcher.get("test", "T")).thenThrow(new NoSuchTermException("hidden"));
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

  private static void assertError(Response response, int status, String type) {
    assertEquals(status, response.getStatus());
    assertEquals(type, response.readEntity(ErrorResponse.class).getType());
  }
}
