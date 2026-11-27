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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.requests.OntologyCreateRequest;
import org.apache.gravitino.dto.requests.OntologyUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.OntologyResponse;
import org.apache.gravitino.exceptions.IllegalOntologyException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.NoSuchOntologyException;
import org.apache.gravitino.exceptions.OntologyAlreadyExistsException;
import org.apache.gravitino.exceptions.OntologyInUseException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.ontology.OntologyChange;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.ontology.OntologyDispatcher;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.server.web.mapper.JsonMappingExceptionMapper;
import org.apache.gravitino.server.web.mapper.JsonProcessingExceptionMapper;
import org.apache.gravitino.utils.NamespaceUtil;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Exercises the ontology REST resource with authorization disabled. */
public class TestOntologyOperations extends BaseOperationsTest {
  private static final String METALAKE = "lake";
  private static final String BASE = "/metalakes/lake/ontologies";
  private static final String ITEM = BASE + "/Sales";
  private static final MediaType GRAVITINO_JSON =
      MediaType.valueOf("application/vnd.gravitino.v1+json");
  private static final Map<String, Object> DEFINITION =
      Map.of("ontology", List.of(Map.of("concept", "Customer", "type", "EntityType")));

  private final OntologyDispatcher dispatcher = mock(OntologyDispatcher.class);

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
            OntologyOperations.class,
            JsonProcessingExceptionMapper.class,
            JsonMappingExceptionMapper.class)
        .register(new ObjectMapperProvider())
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(dispatcher).to(OntologyDispatcher.class);
                bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
              }
            });
  }

  @Test
  public void testListAndLoad() {
    when(dispatcher.listOntologies(METALAKE)).thenReturn(List.of("Sales", "Hr"));
    when(dispatcher.loadOntology(METALAKE, "Sales")).thenReturn(ontology());

    try (Response response = target(BASE).request(GRAVITINO_JSON).get()) {
      assertEquals(200, response.getStatus());
      assertArrayEquals(
          new String[] {"Sales", "Hr"}, response.readEntity(NameListResponse.class).getNames());
    }
    try (Response response = target(ITEM).request(GRAVITINO_JSON).get()) {
      assertEquals(200, response.getStatus());
      OntologyResponse result = response.readEntity(OntologyResponse.class);
      result.validate();
      assertEquals("Sales", result.ontology().name());
      assertEquals(DEFINITION, result.ontology().definition().fields());
    }
  }

  @Test
  public void testCreateAlterAndDrop() {
    when(dispatcher.createOntology(
            eq(METALAKE), eq("Sales"), eq("orders"), any(OntologyDefinition.class), anyMap()))
        .thenReturn(ontology());
    when(dispatcher.alterOntology(eq(METALAKE), eq("Sales"), any(OntologyChange[].class)))
        .thenReturn(ontology());
    when(dispatcher.dropOntology(eq(METALAKE), eq("Sales"))).thenReturn(true);

    OntologyCreateRequest create =
        new OntologyCreateRequest("Sales", "orders", DEFINITION, Map.of("k", "v"));
    try (Response response =
        target(BASE).request(GRAVITINO_JSON).post(Entity.entity(create, GRAVITINO_JSON))) {
      assertEquals(200, response.getStatus());
      assertEquals("Sales", response.readEntity(OntologyResponse.class).ontology().name());
    }

    OntologyUpdatesRequest updates =
        OntologyUpdatesRequest.fromOntologyChanges(
            OntologyChange.updateComment("new"), OntologyChange.setProperty("k", "v2"));
    try (Response response =
        target(ITEM).request(GRAVITINO_JSON).put(Entity.entity(updates, GRAVITINO_JSON))) {
      assertEquals(200, response.getStatus());
    }
    ArgumentCaptor<OntologyChange[]> changes = ArgumentCaptor.forClass(OntologyChange[].class);
    verify(dispatcher).alterOntology(eq(METALAKE), eq("Sales"), changes.capture());
    assertEquals(
        List.of(OntologyChange.updateComment("new"), OntologyChange.setProperty("k", "v2")),
        List.of(changes.getValue()));

    try (Response response = target(ITEM).request(GRAVITINO_JSON).delete()) {
      assertEquals(200, response.getStatus());
      assertTrue(response.readEntity(DropResponse.class).dropped());
    }
  }

  @Test
  public void testImportAndExportDocuments() {
    when(dispatcher.importOntology(eq(METALAKE), any(OssieDocument.class))).thenReturn(ontology());
    when(dispatcher.exportOntology(METALAKE, "Sales", OssieFormat.JSON))
        .thenReturn(OssieDocument.json("{\"name\":\"Sales\"}\n"));
    when(dispatcher.exportOntology(METALAKE, "Sales", OssieFormat.YAML))
        .thenReturn(OssieDocument.yaml("name: Sales\n"));

    try (Response response =
        target(BASE + "/ossie")
            .request(GRAVITINO_JSON)
            .post(Entity.entity("name: Sales\n", "application/yaml"))) {
      assertEquals(200, response.getStatus());
    }
    ArgumentCaptor<OssieDocument> imported = ArgumentCaptor.forClass(OssieDocument.class);
    verify(dispatcher).importOntology(eq(METALAKE), imported.capture());
    assertEquals(OssieFormat.YAML, imported.getValue().format());

    try (Response response =
        target(ITEM + "/ossie")
            .queryParam("format", "JSON")
            .request(MediaType.APPLICATION_JSON)
            .get()) {
      assertEquals(200, response.getStatus());
      assertTrue(response.getMediaType().isCompatible(MediaType.APPLICATION_JSON_TYPE));
      assertEquals("{\"name\":\"Sales\"}\n", response.readEntity(String.class));
    }
    try (Response response = target(ITEM + "/ossie").request("application/yaml").get()) {
      assertEquals(200, response.getStatus());
      assertEquals("name: Sales\n", response.readEntity(String.class));
    }
  }

  @Test
  public void testErrorsMapToRestCodes() {
    assertError(
        target(ITEM + "/ossie").queryParam("format", "xml").request(MediaType.APPLICATION_JSON),
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalArgumentException.class);

    when(dispatcher.loadOntology(METALAKE, "Sales"))
        .thenThrow(new NoSuchOntologyException("Ontology Sales does not exist"));
    assertError(
        target(ITEM).request(GRAVITINO_JSON),
        ErrorConstants.NOT_FOUND_CODE,
        NoSuchOntologyException.class);

    when(dispatcher.dropOntology(eq(METALAKE), eq("Sales")))
        .thenThrow(new OntologyInUseException("Ontology Sales is used by cat.sch.model.customers"));
    try (Response response = target(ITEM).request(GRAVITINO_JSON).delete()) {
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals(ErrorConstants.IN_USE_CODE, error.getCode());
      assertEquals(OntologyInUseException.class.getSimpleName(), error.getType());
    }

    when(dispatcher.createOntology(eq(METALAKE), eq("Sales"), isNull(), any(), anyMap()))
        .thenThrow(new OntologyAlreadyExistsException("Ontology Sales already exists"));
    OntologyCreateRequest create = new OntologyCreateRequest("Sales", null, DEFINITION, null);
    try (Response response =
        target(BASE).request(GRAVITINO_JSON).post(Entity.entity(create, GRAVITINO_JSON))) {
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, error.getCode());
      assertEquals(OntologyAlreadyExistsException.class.getSimpleName(), error.getType());
    }

    when(dispatcher.importOntology(eq(METALAKE), any()))
        .thenThrow(new IllegalOntologyException("$.ontology: required"));
    try (Response response =
        target(BASE + "/ossie")
            .request(GRAVITINO_JSON)
            .post(Entity.entity("{}", MediaType.APPLICATION_JSON))) {
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, error.getCode());
      assertEquals(IllegalOntologyException.class.getSimpleName(), error.getType());
    }

    when(dispatcher.listOntologies(METALAKE))
        .thenThrow(new MetalakeNotInUseException("Metalake lake is not in use"));
    assertError(
        target(BASE).request(GRAVITINO_JSON),
        ErrorConstants.NOT_IN_USE_CODE,
        MetalakeNotInUseException.class);
  }

  @Test
  public void testLockConflictsAreRetryable() {
    when(dispatcher.alterOntology(eq(METALAKE), eq("Sales"), any(OntologyChange[].class)))
        .thenThrow(new OptimisticLockException("The ontology Sales could not be changed"));
    OntologyUpdatesRequest updates =
        OntologyUpdatesRequest.fromOntologyChanges(OntologyChange.updateComment("new"));
    try (Response response =
        target(ITEM).request(GRAVITINO_JSON).put(Entity.entity(updates, GRAVITINO_JSON))) {
      assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals(ErrorConstants.OPTIMISTIC_LOCK_CONFLICT_CODE, error.getCode());
    }
  }

  @Test
  public void testOntologyNamedOssieIsRoutable() {
    when(dispatcher.loadOntology(METALAKE, "ossie")).thenReturn(ontology());
    when(dispatcher.alterOntology(eq(METALAKE), eq("ossie"), any(OntologyChange[].class)))
        .thenReturn(ontology());
    when(dispatcher.dropOntology(eq(METALAKE), eq("ossie"))).thenReturn(true);
    OntologyUpdatesRequest updates =
        OntologyUpdatesRequest.fromOntologyChanges(OntologyChange.updateComment("new"));
    try (Response response = target(BASE + "/ossie").request(GRAVITINO_JSON).get()) {
      assertEquals(200, response.getStatus());
    }
    try (Response response =
        target(BASE + "/ossie")
            .request(GRAVITINO_JSON)
            .put(Entity.entity(updates, GRAVITINO_JSON))) {
      assertEquals(200, response.getStatus());
    }
    try (Response response = target(BASE + "/ossie").request(GRAVITINO_JSON).delete()) {
      assertEquals(200, response.getStatus());
    }
  }

  @Test
  public void testRejectsEmptyUpdates() {
    try (Response response =
        target(ITEM)
            .request(GRAVITINO_JSON)
            .put(Entity.entity("{\"updates\":[]}", GRAVITINO_JSON))) {
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, error.getCode());
    }
    verifyNoInteractions(dispatcher);
  }

  private static void assertError(Invocation.Builder request, int code, Class<?> type) {
    try (Response response = request.get()) {
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals(code, error.getCode());
      assertEquals(type.getSimpleName(), error.getType());
    }
  }

  private static OntologyEntity ontology() {
    return new OntologyEntity(
        1L,
        "Sales",
        NamespaceUtil.ofOntology(METALAKE),
        "orders",
        OntologyDefinition.of(DEFINITION),
        Map.of("ossie-version", "0.3.0.dev0"),
        AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build());
  }
}
