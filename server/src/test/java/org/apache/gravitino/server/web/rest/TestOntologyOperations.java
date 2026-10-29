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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.ontology.OntologyConceptDTO;
import org.apache.gravitino.dto.ontology.OntologyDTO;
import org.apache.gravitino.dto.ontology.OntologyDefinitionDTO;
import org.apache.gravitino.dto.requests.OntologyCreateRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.OntologyListResponse;
import org.apache.gravitino.dto.responses.OntologyResponse;
import org.apache.gravitino.exceptions.NoSuchOntologyException;
import org.apache.gravitino.exceptions.NonEmptyOntologyException;
import org.apache.gravitino.exceptions.OntologyAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.ontology.ConceptType;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyChange;
import org.apache.gravitino.ontology.OntologyConcept;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.ontology.OntologyDispatcher;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestOntologyOperations extends BaseOperationsTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private final OntologyDispatcher dispatcher = mock(OntologyDispatcher.class);

  private final String metalake = "test_metalake";

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    Config config = mock(Config.class);
    Mockito.doReturn(false).when(config).get(Configs.CACHE_ENABLED);
    Mockito.doReturn(false).when(config).get(Configs.ENABLE_AUTHORIZATION);
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

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(OntologyOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(dispatcher).to(OntologyDispatcher.class).ranked(2);
            bindFactory(TestOntologyOperations.MockServletRequestFactory.class)
                .to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  private String ontologyPath(String metalake) {
    return "/metalakes/" + metalake + "/ontologies";
  }

  private String ontologyPath(String metalake, String ontology) {
    return "/metalakes/" + metalake + "/ontologies/" + ontology;
  }

  private static OntologyEntity ontologyEntity(String name) {
    return OntologyEntity.builder()
        .withId(1L)
        .withName(name)
        .withNamespace(Namespace.of("test_metalake"))
        .withStatus(Ontology.Status.DRAFT)
        .withDefinition(simpleDefinition())
        .withProperties(Collections.singletonMap(Ontology.PROPERTY_OSSIE_VERSION, "0.2.0.dev0"))
        .withAuditInfo(
            AuditInfo.builder().withCreator("user1").withCreateTime(Instant.now()).build())
        .build();
  }

  private static OntologyDefinition simpleDefinition() {
    return OntologyDefinition.builder()
        .withConcepts(
            OntologyConcept.builder()
                .withConcept("Account")
                .withType(ConceptType.ENTITY_TYPE)
                .build())
        .build();
  }

  @Test
  public void testListOntologies() {
    when(dispatcher.listOntologies(metalake)).thenReturn(new String[] {"Finance", "Sales"});

    Response response =
        target(ontologyPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    NameListResponse nameListResponse = response.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse.getCode());
    Assertions.assertArrayEquals(new String[] {"Finance", "Sales"}, nameListResponse.getNames());

    // A null list is normalized to an empty list.
    when(dispatcher.listOntologies(metalake)).thenReturn(null);
    Response emptyResponse =
        target(ontologyPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), emptyResponse.getStatus());
    Assertions.assertEquals(0, emptyResponse.readEntity(NameListResponse.class).getNames().length);
  }

  @Test
  public void testListOntologiesInfo() {
    when(dispatcher.listOntologiesInfo(metalake))
        .thenReturn(new Ontology[] {ontologyEntity("Finance")});

    Response response =
        target(ontologyPath(metalake))
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    OntologyListResponse listResponse = response.readEntity(OntologyListResponse.class);
    Assertions.assertEquals(0, listResponse.getCode());
    Assertions.assertEquals(1, listResponse.getOntologies().length);
    Assertions.assertEquals("Finance", listResponse.getOntologies()[0].name());
    Assertions.assertEquals(Ontology.Status.DRAFT, listResponse.getOntologies()[0].status());
  }

  @Test
  public void testGetOntology() {
    when(dispatcher.getOntology(metalake, "Finance")).thenReturn(ontologyEntity("Finance"));

    Response response =
        target(ontologyPath(metalake, "Finance"))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    OntologyResponse ontologyResponse = response.readEntity(OntologyResponse.class);
    Assertions.assertEquals(0, ontologyResponse.getCode());
    OntologyDTO dto = ontologyResponse.getOntology();
    Assertions.assertEquals("Finance", dto.name());
    Assertions.assertEquals("user1", dto.auditInfo().creator());
    Assertions.assertEquals(1, dto.definition().concepts().length);

    // A missing ontology is a 404.
    when(dispatcher.getOntology(metalake, "missing"))
        .thenThrow(new NoSuchOntologyException("mock error"));
    Response missing =
        target(ontologyPath(metalake, "missing"))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), missing.getStatus());
    ErrorResponse errorResponse = missing.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchOntologyException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testCreateOntology() {
    when(dispatcher.createOntology(eq(metalake), eq("Finance"), any(), any(), any()))
        .thenReturn(ontologyEntity("Finance"));

    OntologyCreateRequest request =
        new OntologyCreateRequest(
            "Finance",
            "Finance vocabulary",
            Collections.singletonMap(Ontology.PROPERTY_OSSIE_VERSION, "0.2.0.dev0"),
            new OntologyDefinitionDTO(
                null,
                null,
                new OntologyConceptDTO[] {
                  new OntologyConceptDTO(
                      "Account", ConceptType.ENTITY_TYPE, null, null, null, null, null, null, null)
                },
                null));

    Response response =
        target(ontologyPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    OntologyResponse ontologyResponse = response.readEntity(OntologyResponse.class);
    Assertions.assertEquals(0, ontologyResponse.getCode());
    Assertions.assertEquals("Finance", ontologyResponse.getOntology().name());
    Assertions.assertEquals(Ontology.Status.DRAFT, ontologyResponse.getOntology().status());

    // A duplicate name is a 409.
    doThrow(new OntologyAlreadyExistsException("mock error"))
        .when(dispatcher)
        .createOntology(anyString(), anyString(), any(), any(), any());
    Response conflict =
        target(ontologyPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), conflict.getStatus());
    Assertions.assertEquals(
        ErrorConstants.ALREADY_EXISTS_CODE, conflict.readEntity(ErrorResponse.class).getCode());
  }

  @Test
  public void testAlterOntologyDeserializesEveryUpdateType() {
    when(dispatcher.alterOntology(eq(metalake), eq("Finance"), any()))
        .thenReturn(ontologyEntity("FinanceV2"));

    String body =
        "{"
            + "\"updates\": ["
            + "{\"@type\": \"rename\", \"newName\": \"FinanceV2\"},"
            + "{\"@type\": \"updateComment\", \"newComment\": \"new comment\"},"
            + "{\"@type\": \"setProperty\", \"property\": \"ossie-version\", \"value\": \"0.2.0.dev0\"},"
            + "{\"@type\": \"removeProperty\", \"property\": \"legacy\"},"
            + "{\"@type\": \"replaceDefinition\", \"definition\": {\"concepts\": [{\"concept\": \"Account\", \"type\": \"ENTITY_TYPE\"}]}},"
            + "{\"@type\": \"updateStatus\", \"status\": \"PUBLISHED\"}"
            + "]}";

    Response response =
        target(ontologyPath(metalake, "Finance"))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(body, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(0, response.readEntity(OntologyResponse.class).getCode());

    ArgumentCaptor<OntologyChange[]> captor = ArgumentCaptor.forClass(OntologyChange[].class);
    Mockito.verify(dispatcher).alterOntology(eq(metalake), eq("Finance"), captor.capture());
    OntologyChange[] changes = captor.getValue();
    Assertions.assertEquals(6, changes.length);
    Assertions.assertTrue(changes[0] instanceof OntologyChange.RenameOntology);
    Assertions.assertEquals("FinanceV2", ((OntologyChange.RenameOntology) changes[0]).getNewName());
    Assertions.assertTrue(changes[1] instanceof OntologyChange.UpdateComment);
    Assertions.assertTrue(changes[2] instanceof OntologyChange.SetProperty);
    Assertions.assertTrue(changes[3] instanceof OntologyChange.RemoveProperty);
    Assertions.assertTrue(changes[4] instanceof OntologyChange.ReplaceDefinition);
    Assertions.assertTrue(changes[5] instanceof OntologyChange.UpdateStatus);
    Assertions.assertEquals(
        Ontology.Status.PUBLISHED, ((OntologyChange.UpdateStatus) changes[5]).getStatus());
  }

  @Test
  public void testAlterOntologyRejectsInvalidRequest() {
    Response empty =
        target(ontologyPath(metalake, "Finance"))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity("{\"updates\": []}", MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), empty.getStatus());
  }

  @Test
  public void testDeleteOntology() {
    when(dispatcher.deleteOntology(metalake, "Finance")).thenReturn(true);

    Response response =
        target(ontologyPath(metalake, "Finance"))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertTrue(response.readEntity(DropResponse.class).dropped());

    // Deleting an absent ontology reports that nothing was dropped.
    when(dispatcher.deleteOntology(metalake, "absent")).thenReturn(false);
    Response absent =
        target(ontologyPath(metalake, "absent"))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), absent.getStatus());
    Assertions.assertFalse(absent.readEntity(DropResponse.class).dropped());

    // An ontology that is still referenced cannot be dropped.
    doThrow(new NonEmptyOntologyException("mock error"))
        .when(dispatcher)
        .deleteOntology(metalake, "referenced");
    Response referenced =
        target(ontologyPath(metalake, "referenced"))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), referenced.getStatus());
  }
}
