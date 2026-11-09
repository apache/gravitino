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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainChange;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.dto.requests.DomainBackingTagSetRequest;
import org.apache.gravitino.dto.responses.DomainListResponse;
import org.apache.gravitino.dto.responses.DomainResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.exceptions.BackingTagAlreadyBoundException;
import org.apache.gravitino.exceptions.DomainAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NonEmptyDomainException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.server.web.mapper.JsonMappingExceptionMapper;
import org.apache.gravitino.server.web.mapper.JsonProcessingExceptionMapper;
import org.apache.gravitino.tag.TagValue;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestDomainOperations extends BaseOperationsTest {

  private static final String BASE = "/metalakes/test/domains";
  private static final String ITEM = BASE + "/Finance";
  private final DomainDispatcher dispatcher = mock(DomainDispatcher.class);

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
            DomainOperations.class,
            DomainMembershipOperations.class,
            JsonProcessingExceptionMapper.class,
            JsonMappingExceptionMapper.class)
        .register(new ObjectMapperProvider())
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(dispatcher).to(DomainDispatcher.class);
                bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
              }
            });
  }

  @Test
  public void testListsAndGet() {
    DomainEntity domain = domain(Domain.Status.DRAFT);
    when(dispatcher.listDomains("test")).thenReturn(new String[] {"Finance"});
    when(dispatcher.listDomainInfos("test")).thenReturn(new DomainEntity[] {domain});
    when(dispatcher.listSubdomains("test", "Finance")).thenReturn(new String[] {"Tax"});
    when(dispatcher.listSubdomainInfos("test", "Finance")).thenReturn(new DomainEntity[] {domain});
    when(dispatcher.getDomain("test", "Finance")).thenReturn(domain);
    for (String path : new String[] {BASE, ITEM + "/subdomains"}) {
      try (Response response = target(path).request().get()) {
        assertEquals(200, response.getStatus());
        assertArrayEquals(
            new String[] {path.equals(BASE) ? "Finance" : "Tax"},
            response.readEntity(NameListResponse.class).getNames());
      }
      try (Response response = target(path).queryParam("details", true).request().get()) {
        assertEquals(200, response.getStatus());
        DomainListResponse result = response.readEntity(DomainListResponse.class);
        result.validate();
        assertEquals("Finance", result.getDomains()[0].getName());
      }
    }
    try (Response response = target(ITEM).request().get()) {
      assertDomain(response, Domain.Status.DRAFT);
    }
  }

  @Test
  public void testEmptyLists() {
    when(dispatcher.listDomains("test")).thenReturn(new String[0]);
    when(dispatcher.listDomainInfos("test")).thenReturn(new DomainEntity[0]);
    when(dispatcher.listSubdomains("test", "Finance")).thenReturn(new String[0]);
    when(dispatcher.listSubdomainInfos("test", "Finance")).thenReturn(new DomainEntity[0]);
    for (String path : new String[] {BASE, ITEM + "/subdomains"}) {
      try (Response response = target(path).request().get()) {
        assertEquals(0, response.readEntity(NameListResponse.class).getNames().length);
      }
      try (Response response = target(path).queryParam("details", true).request().get()) {
        assertEquals(0, response.readEntity(DomainListResponse.class).getDomains().length);
      }
    }
  }

  @Test
  public void testCreateWithNewTag() {
    when(dispatcher.createDomain(
            "test",
            "Finance",
            "comment",
            Map.of("key", "value"),
            TagValue.noValue("department"),
            true))
        .thenReturn(domain(Domain.Status.DRAFT));
    try (Response response =
        send(
            "POST",
            BASE,
            "{\"name\":\"Finance\",\"comment\":\"comment\",\"properties\":{\"key\":\"value\"},\"backingTag\":{\"name\":\"department\",\"create\":true}}")) {
      assertDomain(response, Domain.Status.DRAFT);
    }
    verify(dispatcher)
        .createDomain(
            "test",
            "Finance",
            "comment",
            Map.of("key", "value"),
            TagValue.noValue("department"),
            true);
  }

  @Test
  public void testCreateSubdomainWithExistingValuedTag() {
    when(dispatcher.createSubdomain(
            "test", "Parent", "Finance", null, null, TagValue.of("department", "finance"), false))
        .thenReturn(domain(Domain.Status.DRAFT));
    try (Response response =
        send(
            "POST",
            BASE,
            "{\"name\":\"Finance\",\"parent\":\"Parent\",\"backingTag\":{\"name\":\"department\",\"value\":\"finance\"}}")) {
      assertDomain(response, Domain.Status.DRAFT);
    }
    verify(dispatcher)
        .createSubdomain(
            "test", "Parent", "Finance", null, null, TagValue.of("department", "finance"), false);
  }

  @Test
  public void testAlter() {
    DomainChange[] changes = {
      DomainChange.rename("New"),
      DomainChange.updateComment(null),
      DomainChange.setProperty("key", ""),
      DomainChange.removeProperty("old")
    };
    when(dispatcher.alterDomain("test", "Finance", changes))
        .thenReturn(domain(Domain.Status.DRAFT));
    try (Response response =
        send(
            "PUT",
            ITEM,
            "{\"updates\":[{\"@type\":\"rename\",\"newName\":\"New\"},"
                + "{\"@type\":\"updateComment\",\"newComment\":null},{\"@type\":\"setProperty\",\"property\":\"key\",\"value\":\"\"},"
                + "{\"@type\":\"removeProperty\",\"property\":\"old\"}]}")) {
      assertDomain(response, Domain.Status.DRAFT);
    }
    verify(dispatcher).alterDomain("test", "Finance", changes);
  }

  @ParameterizedTest
  @ValueSource(strings = {"DRAFT", "PUBLISHED"})
  public void testStatus(String status) {
    when(dispatcher.publishDomain("test", "Finance")).thenReturn(domain(Domain.Status.PUBLISHED));
    when(dispatcher.unpublishDomain("test", "Finance")).thenReturn(domain(Domain.Status.DRAFT));
    try (Response response = send("PATCH", ITEM, "{\"status\":\"" + status + "\"}")) {
      assertDomain(response, Domain.Status.valueOf(status));
    }
    if (status.equals("PUBLISHED")) {
      verify(dispatcher).publishDomain("test", "Finance");
    } else {
      verify(dispatcher).unpublishDomain("test", "Finance");
    }
  }

  @Test
  public void testParentAndBackingTag() {
    when(dispatcher.attachDomain("test", "Finance", "Parent"))
        .thenReturn(domain(Domain.Status.DRAFT));
    when(dispatcher.detachDomain("test", "Finance")).thenReturn(domain(Domain.Status.DRAFT));
    when(dispatcher.updateBackingTag(eq("test"), eq("Finance"), any()))
        .thenReturn(domain(Domain.Status.DRAFT));
    when(dispatcher.updateBackingTag("test", "Finance", null))
        .thenReturn(domain(Domain.Status.DRAFT));
    try (Response response = send("PUT", ITEM + "/parent", "{\"parent\":\"Parent\"}")) {
      assertDomain(response, Domain.Status.DRAFT);
    }
    try (Response response = target(ITEM + "/parent").request().delete()) {
      assertDomain(response, Domain.Status.DRAFT);
    }
    try (Response response =
        send(
            "PUT",
            ITEM + "/backingTag",
            "{\"backingTag\":{\"name\":\"department\",\"value\":\"finance\"}}")) {
      assertDomain(response, Domain.Status.DRAFT);
    }
    try (Response response = send("PUT", ITEM + "/backingTag", "{\"backingTag\":null}")) {
      assertDomain(response, Domain.Status.DRAFT);
    }
    verify(dispatcher).attachDomain("test", "Finance", "Parent");
    verify(dispatcher).detachDomain("test", "Finance");
    verify(dispatcher).updateBackingTag("test", "Finance", TagValue.of("department", "finance"));
    verify(dispatcher).updateBackingTag("test", "Finance", null);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDelete(boolean deleted) {
    when(dispatcher.deleteDomain("test", "Finance")).thenReturn(deleted);
    try (Response response = target(ITEM).request().delete()) {
      assertEquals(200, response.getStatus());
      assertEquals(deleted, response.readEntity(DropResponse.class).dropped());
    }
  }

  @Test
  public void testNullBodies() {
    for (String[] route :
        new String[][] {
          {"POST", BASE},
          {"PUT", ITEM},
          {"PATCH", ITEM},
          {"PUT", ITEM + "/parent"},
          {"PUT", ITEM + "/backingTag"}
        }) {
      try (Response response = send(route[0], route[1], "null")) {
        assertNullRequestBodyRejected(response);
      }
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  public void testInvalidBodies() {
    for (String json :
        new String[] {
          "{}",
          "{\"name\":\"Finance\"}",
          "{\"name\":\"Finance\",\"parent\":\" \",\"backingTag\":{\"name\":\"t\"}}",
          "{\"name\":\"Finance\",\"backingTag\":{\"name\":\"t\",\"value\":\"v\",\"create\":true}}"
        }) {
      try (Response response = send("POST", BASE, json)) {
        assertError(response, 400);
      }
    }
    for (String json :
        new String[] {
          "{}",
          "{\"updates\":[null]}",
          "{\"updates\":[{\"@type\":\"rename\",\"newName\":\" \"}]}",
          "{\"updates\":[{\"@type\":\"unknown\"}]}"
        }) {
      try (Response response = send("PUT", ITEM, json)) {
        assertError(response, 400);
      }
    }
    for (String json : new String[] {"{}", "{\"status\":\"UNKNOWN\"}"}) {
      try (Response response = send("PATCH", ITEM, json)) {
        assertError(response, 400);
      }
    }
    try (Response response = send("PUT", ITEM + "/parent", "{\"parent\":\" \"}")) {
      assertError(response, 400);
    }
    try (Response response =
        send("PUT", ITEM + "/backingTag", "{\"backingTag\":{\"name\":\" \"}}")) {
      assertError(response, 400);
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  public void testErrors() {
    for (RuntimeException error :
        new RuntimeException[] {
          new DomainAlreadyExistsException("duplicate"),
          new TagAlreadyExistsException("duplicate"),
          new BackingTagAlreadyBoundException("bound")
        }) {
      when(dispatcher.createDomain(eq("test"), eq("Finance"), isNull(), isNull(), any(), eq(false)))
          .thenThrow(error);
      try (Response response =
          send("POST", BASE, "{\"name\":\"Finance\",\"backingTag\":{\"name\":\"t\"}}")) {
        assertError(response, 409);
      }
    }
    when(dispatcher.deleteDomain("test", "Finance"))
        .thenThrow(new NonEmptyDomainException("has children"));
    try (Response response = target(ITEM).request().delete()) {
      assertEquals(409, response.getStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals(ErrorConstants.NON_EMPTY_CODE, error.getCode());
      assertEquals(NonEmptyDomainException.class.getSimpleName(), error.getType());
    }
    when(dispatcher.getDomain("test", "Finance")).thenThrow(new NoSuchDomainException("missing"));
    try (Response response = target(ITEM).request().get()) {
      assertError(response, 404);
    }
    when(dispatcher.updateBackingTag(eq("test"), eq("Finance"), any()))
        .thenThrow(new NoSuchTagException("missing tag"));
    try (Response response =
        send("PUT", ITEM + "/backingTag", "{\"backingTag\":{\"name\":\"t\"}}")) {
      assertError(response, 404);
    }
    when(dispatcher.publishDomain("test", "Finance"))
        .thenThrow(new IllegalArgumentException("draft parent"));
    try (Response response = send("PATCH", ITEM, "{\"status\":\"PUBLISHED\"}")) {
      assertError(response, 400);
    }
  }

  @Test
  public void testInactiveMetalake() {
    when(dispatcher.listDomains("test")).thenThrow(new MetalakeNotInUseException("disabled"));
    try (Response response = target(BASE).request().get()) {
      assertEquals(409, response.getStatus());
      assertEquals("MetalakeNotInUseException", response.readEntity(ErrorResponse.class).getType());
    }
  }

  @Test
  public void testEncodedDomainName() {
    when(dispatcher.getDomain("test", "Finance/US")).thenReturn(domain(Domain.Status.DRAFT));
    try (Response response =
        target(BASE + "/{domain}").resolveTemplate("domain", "Finance/US").request().get()) {
      assertDomain(response, Domain.Status.DRAFT);
    }
    verify(dispatcher).getDomain("test", "Finance/US");
  }

  @Test
  public void testProductionJsonRepresentation() throws IOException {
    when(dispatcher.getDomain("test", "Finance")).thenReturn(domain(Domain.Status.DRAFT));
    try (Response response = target(ITEM).request().get()) {
      assertEquals(200, response.getStatus());
      JsonNode snapshot =
          JsonUtils.objectMapper().readTree(response.readEntity(String.class)).get("domain");
      assertEquals("draft", snapshot.get("status").asText());
      assertFalse(snapshot.has("relations"));
      assertFalse(snapshot.has("comment"));
      assertFalse(snapshot.get("audit").has("lastModifier"));
    }
  }

  @Test
  public void testMissingBackingTagFieldDoesNotRemoveBinding() {
    for (String json : new String[] {"{}", "{\"backing_tag\":null}"}) {
      try (Response response = send("PUT", ITEM + "/backingTag", json)) {
        assertError(response, 400);
      }
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  public void testProductionSerializationPreservesExplicitRemoval() throws IOException {
    DomainBackingTagSetRequest request = new DomainBackingTagSetRequest(null);
    when(dispatcher.updateBackingTag("test", "Finance", null))
        .thenReturn(domain(Domain.Status.DRAFT));
    try (Response response =
        send(
            "PUT",
            ITEM + "/backingTag",
            ObjectMapperProvider.objectMapper().writeValueAsString(request))) {
      assertDomain(response, Domain.Status.DRAFT);
    }
    verify(dispatcher).updateBackingTag("test", "Finance", null);
  }

  private Response send(String method, String path, String json) {
    return target(path)
        .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
        .request()
        .method(method, Entity.json(json));
  }

  private static DomainEntity domain(Domain.Status status) {
    return DomainEntity.builder()
        .withId(1L)
        .withName("Finance")
        .withNamespace(Namespace.of("test"))
        .withProperties(Map.of())
        .withStatus(status)
        .withAuditInfo(
            AuditInfo.builder().withCreator("user").withCreateTime(Instant.now()).build())
        .build();
  }

  private static void assertDomain(Response response, Domain.Status status) {
    assertEquals(200, response.getStatus());
    DomainResponse result = response.readEntity(DomainResponse.class);
    result.validate();
    assertEquals("Finance", result.getDomain().getName());
    assertEquals(status, result.getDomain().getStatus());
    assertEquals(0, result.getDomain().getRelations().length);
  }

  private static void assertError(Response response, int status) {
    assertEquals(status, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
    assertTrue(response.readEntity(ErrorResponse.class).getCode() > 0);
  }
}
