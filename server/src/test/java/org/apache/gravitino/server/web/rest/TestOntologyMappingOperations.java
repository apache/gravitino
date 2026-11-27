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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.ontology.OntologyMappingDTO;
import org.apache.gravitino.dto.requests.OntologyMappingCreateRequest;
import org.apache.gravitino.dto.requests.OntologyUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.OntologyMappingResponse;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.IllegalOntologyMappingException;
import org.apache.gravitino.exceptions.NoSuchOntologyMappingException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.OntologyMappingAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.ontology.OntologyDispatcher;
import org.apache.gravitino.ontology.OntologyMapping;
import org.apache.gravitino.ontology.OntologyMappingChange;
import org.apache.gravitino.ontology.OntologyMappingDefinition;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.server.web.mapper.JsonMappingExceptionMapper;
import org.apache.gravitino.server.web.mapper.JsonProcessingExceptionMapper;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Exercises the model-scoped ontology mapping REST resource with authorization disabled. */
public class TestOntologyMappingOperations extends BaseOperationsTest {
  private static final NameIdentifier MODEL = NameIdentifier.of("lake", "cat", "sch", "model");
  private static final String BASE =
      "/metalakes/lake/catalogs/cat/schemas/sch/semantic-models/model/ontology-mappings";
  private static final String ITEM = BASE + "/customers";
  private static final MediaType GRAVITINO_JSON =
      MediaType.valueOf("application/vnd.gravitino.v1+json");
  private static final Map<String, Object> DEFINITION =
      Map.of(
          "conceptMappings",
          List.of(
              Map.of(
                  "concept",
                  "Customer",
                  "objectMappings",
                  List.of(Map.of("expression", "customer.id")))));

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
            OntologyMappingOperations.class,
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
  public void testListLoadAndExport() {
    NameIdentifier customers = NameIdentifier.of("lake", "cat", "sch", "model", "customers");
    OntologyMapping mapping = mapping();
    when(dispatcher.listModelMappings(eq(MODEL), any())).thenReturn(List.of(customers));
    when(dispatcher.loadMapping(eq(MODEL), eq("customers"), any())).thenReturn(mapping);
    when(dispatcher.exportMapping(eq(MODEL), eq("customers"), eq(OssieFormat.YAML), any()))
        .thenReturn(OssieDocument.yaml("name: customers\n"));

    try (Response response = target(BASE).request(GRAVITINO_JSON).get()) {
      assertEquals(200, response.getStatus());
      assertArrayEquals(
          new NameIdentifier[] {customers},
          response.readEntity(EntityListResponse.class).identifiers());
    }
    try (Response response = target(ITEM).request(GRAVITINO_JSON).get()) {
      assertEquals(200, response.getStatus());
      OntologyMappingDTO dto = response.readEntity(OntologyMappingResponse.class).ontologyMapping();
      assertEquals("customers", dto.name());
      assertEquals("Sales", dto.ontology());
      assertEquals(DEFINITION, dto.definition().fields());
    }
    try (Response response = target(ITEM + "/ossie").request("application/yaml").get()) {
      assertEquals(200, response.getStatus());
      assertEquals("name: customers\n", response.readEntity(String.class));
    }
  }

  @Test
  public void testCreateImportAlterAndDrop() {
    OntologyMapping mapping = mapping();
    when(dispatcher.createMapping(
            eq(MODEL),
            eq("customers"),
            isNull(),
            eq("Sales"),
            any(OntologyMappingDefinition.class),
            anyMap(),
            any()))
        .thenReturn(mapping);
    when(dispatcher.importMapping(eq(MODEL), any(OssieDocument.class), any())).thenReturn(mapping);
    when(dispatcher.alterMapping(
            eq(MODEL), eq("customers"), any(), any(OntologyMappingChange[].class)))
        .thenReturn(mapping);
    when(dispatcher.dropMapping(MODEL, "customers")).thenReturn(false);

    OntologyMappingCreateRequest create =
        new OntologyMappingCreateRequest("customers", null, "Sales", DEFINITION, null);
    try (Response response =
        target(BASE).request(GRAVITINO_JSON).post(Entity.entity(create, GRAVITINO_JSON))) {
      assertEquals(200, response.getStatus());
    }
    try (Response response =
        target(BASE + "/ossie")
            .request(GRAVITINO_JSON)
            .post(Entity.entity("{\"name\":\"customers\"}", MediaType.APPLICATION_JSON))) {
      assertEquals(200, response.getStatus());
    }
    ArgumentCaptor<OssieDocument> imported = ArgumentCaptor.forClass(OssieDocument.class);
    verify(dispatcher).importMapping(eq(MODEL), imported.capture(), any());
    assertEquals(OssieFormat.JSON, imported.getValue().format());

    OntologyUpdatesRequest updates =
        OntologyUpdatesRequest.fromMappingChanges(OntologyMappingChange.rename("clients"));
    try (Response response =
        target(ITEM).request(GRAVITINO_JSON).put(Entity.entity(updates, GRAVITINO_JSON))) {
      assertEquals(200, response.getStatus());
    }
    ArgumentCaptor<OntologyMappingChange[]> changes =
        ArgumentCaptor.forClass(OntologyMappingChange[].class);
    verify(dispatcher).alterMapping(eq(MODEL), eq("customers"), any(), changes.capture());
    assertEquals(List.of(OntologyMappingChange.rename("clients")), List.of(changes.getValue()));

    try (Response response = target(ITEM).request(GRAVITINO_JSON).delete()) {
      assertEquals(200, response.getStatus());
      assertFalse(response.readEntity(DropResponse.class).dropped());
    }
  }

  @Test
  public void testErrorsMapToRestCodes() {
    when(dispatcher.loadMapping(eq(MODEL), eq("customers"), any()))
        .thenThrow(new NoSuchOntologyMappingException("Mapping customers does not exist"));
    try (Response response = target(ITEM).request(GRAVITINO_JSON).get()) {
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals(ErrorConstants.NOT_FOUND_CODE, error.getCode());
      assertEquals(NoSuchOntologyMappingException.class.getSimpleName(), error.getType());
    }

    when(dispatcher.listModelMappings(eq(MODEL), any()))
        .thenThrow(new CatalogNotInUseException("Catalog cat is not in use"));
    try (Response response = target(BASE).request(GRAVITINO_JSON).get()) {
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals(ErrorConstants.NOT_IN_USE_CODE, error.getCode());
      assertEquals(CatalogNotInUseException.class.getSimpleName(), error.getType());
    }

    when(dispatcher.createMapping(any(), any(), any(), any(), any(), anyMap(), any()))
        .thenThrow(new OntologyMappingAlreadyExistsException("Mapping customers already exists"));
    OntologyMappingCreateRequest create =
        new OntologyMappingCreateRequest("customers", null, "Sales", DEFINITION, null);
    try (Response response =
        target(BASE).request(GRAVITINO_JSON).post(Entity.entity(create, GRAVITINO_JSON))) {
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, error.getCode());
    }

    when(dispatcher.importMapping(eq(MODEL), any(), any()))
        .thenThrow(new IllegalOntologyMappingException("$.semantic_model_ref.name mismatch"));
    try (Response response =
        target(BASE + "/ossie")
            .request(GRAVITINO_JSON)
            .post(Entity.entity("name: customers\n", "text/yaml"))) {
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, error.getCode());
      assertEquals(IllegalOntologyMappingException.class.getSimpleName(), error.getType());
    }
  }

  /** Checks that mapping read endpoints hide a model that the caller cannot read. */
  @Test
  public void testModelNotFoundOnMappingReads() {
    NoSuchSemanticModelException missing =
        new NoSuchSemanticModelException("Semantic model '%s' does not exist", MODEL);
    when(dispatcher.listModelMappings(eq(MODEL), any())).thenThrow(missing);
    when(dispatcher.loadMapping(eq(MODEL), eq("customers"), any())).thenThrow(missing);
    when(dispatcher.exportMapping(eq(MODEL), eq("customers"), eq(OssieFormat.YAML), any()))
        .thenThrow(missing);

    for (String path : List.of(BASE, ITEM, ITEM + "/ossie")) {
      try (Response response = target(path).request().get()) {
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
        assertEquals(
            NoSuchSemanticModelException.class.getSimpleName(),
            response.readEntity(ErrorResponse.class).getType());
      }
    }
  }

  private static OntologyMapping mapping() {
    OntologyMapping mapping = mock(OntologyMapping.class);
    when(mapping.name()).thenReturn("customers");
    when(mapping.ontology()).thenReturn("Sales");
    when(mapping.definition()).thenReturn(OntologyMappingDefinition.of(DEFINITION));
    when(mapping.properties()).thenReturn(Map.of());
    when(mapping.auditInfo())
        .thenReturn(
            AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build());
    return mapping;
  }
}
