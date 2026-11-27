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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.ontology.OntologyDTO;
import org.apache.gravitino.dto.ontology.OntologyMappingDTO;
import org.apache.gravitino.dto.requests.OntologyCreateRequest;
import org.apache.gravitino.dto.requests.OntologyMappingCreateRequest;
import org.apache.gravitino.dto.requests.OntologyUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.OntologyMappingResponse;
import org.apache.gravitino.dto.responses.OntologyResponse;
import org.apache.gravitino.exceptions.IllegalOntologyMappingException;
import org.apache.gravitino.exceptions.NoSuchOntologyException;
import org.apache.gravitino.exceptions.NoSuchOntologyMappingException;
import org.apache.gravitino.exceptions.OntologyAlreadyExistsException;
import org.apache.gravitino.exceptions.OntologyInUseException;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyChange;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.ontology.OntologyMapping;
import org.apache.gravitino.ontology.OntologyMappingChange;
import org.apache.gravitino.ontology.OntologyMappingDefinition;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

/** Verifies the REST calls and error translation of the ontology client operations. */
public class TestOntologyClientOperations extends TestBase {
  private static final String METALAKE = "lake";
  private static final String ONTOLOGIES = "/api/metalakes/lake/ontologies";
  private static final String MAPPINGS =
      "/api/metalakes/lake/catalogs/cat/schemas/sch/semantic-models/model/ontology-mappings";
  private static final Map<String, Object> DEFINITION =
      Map.of("ontology", List.of(Map.of("concept", "Customer", "type", "EntityType")));
  private static final Map<String, Object> MAPPING_DEFINITION =
      Map.of(
          "conceptMappings",
          List.of(
              Map.of(
                  "concept",
                  "Customer",
                  "objectMappings",
                  List.of(Map.of("expression", "customer.id")))));
  private static final AuditDTO AUDIT =
      AuditDTO.builder().withCreator("tester").withCreateTime(Instant.now()).build();

  /**
   * Verifies the ontology lifecycle requests.
   *
   * @throws JsonProcessingException if the mock bodies cannot be serialized
   */
  @Test
  public void testOntologyLifecycle() throws JsonProcessingException {
    OntologyClientOperations ontologies =
        new OntologyClientOperations(client.restClient(), METALAKE);
    OntologyResponse response = new OntologyResponse(ontology());

    buildMockResource(
        Method.GET, ONTOLOGIES, null, new NameListResponse(new String[] {"Sales"}), 200);
    Assertions.assertArrayEquals(new String[] {"Sales"}, ontologies.listOntologies());

    buildMockResource(
        Method.POST,
        ONTOLOGIES,
        new OntologyCreateRequest("Sales", "orders", DEFINITION, Map.of()),
        response,
        200);
    Ontology created =
        ontologies.createOntology(
            "Sales", "orders", OntologyDefinition.of(DEFINITION), Collections.emptyMap());
    Assertions.assertEquals("Sales", created.name());
    Assertions.assertEquals(DEFINITION, created.definition().fields());

    buildMockResource(Method.GET, ONTOLOGIES + "/Sales", null, response, 200);
    Assertions.assertEquals("orders", ontologies.loadOntology("Sales").comment());

    buildMockResource(
        Method.PUT,
        ONTOLOGIES + "/Sales",
        OntologyUpdatesRequest.fromOntologyChanges(OntologyChange.rename("Sales")),
        response,
        200);
    Assertions.assertEquals(
        "Sales", ontologies.alterOntology("Sales", OntologyChange.rename("Sales")).name());

    buildMockResource(Method.DELETE, ONTOLOGIES + "/Sales", null, new DropResponse(true), 200);
    Assertions.assertTrue(ontologies.dropOntology("Sales"));

    buildMockResource(
        Method.GET,
        ONTOLOGIES + "/Sales/mappings",
        null,
        new EntityListResponse(
            new NameIdentifier[] {NameIdentifier.of(METALAKE, "cat", "sch", "model", "m")}),
        200);
    Assertions.assertArrayEquals(
        new NameIdentifier[] {NameIdentifier.of("cat", "sch", "model", "m")},
        ontologies.listOntologyMappings("Sales"));

    Assertions.assertThrows(IllegalArgumentException.class, () -> ontologies.loadOntology(" "));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ontologies.alterOntology("Sales"));
  }

  /**
   * Verifies Ossie import and export requests.
   *
   * @throws JsonProcessingException if the mock bodies cannot be serialized
   */
  @Test
  public void testOntologyDocuments() throws JsonProcessingException {
    OntologyClientOperations ontologies =
        new OntologyClientOperations(client.restClient(), METALAKE);
    mockServer
        .when(
            HttpRequest.request(ONTOLOGIES + "/ossie")
                .withMethod("POST")
                .withBody("name: Sales\n".getBytes(StandardCharsets.UTF_8)),
            Times.exactly(1))
        .respond(
            HttpResponse.response()
                .withStatusCode(200)
                .withBody(MAPPER.writeValueAsString(new OntologyResponse(ontology()))));
    Assertions.assertEquals(
        "Sales", ontologies.importOntology(OssieDocument.yaml("name: Sales\n")).name());

    mockServer
        .when(
            HttpRequest.request(ONTOLOGIES + "/Sales/ossie")
                .withMethod("GET")
                .withQueryStringParameter("format", "json"),
            Times.exactly(1))
        .respond(
            HttpResponse.response()
                .withStatusCode(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"name\":\"Sales\"}"));
    OssieDocument exported = ontologies.exportOntology("Sales", OssieFormat.JSON);
    Assertions.assertEquals(OssieFormat.JSON, exported.format());
    Assertions.assertEquals("{\"name\":\"Sales\"}", exported.content());
  }

  /**
   * Verifies mapping lifecycle requests under a semantic model.
   *
   * @throws JsonProcessingException if the mock bodies cannot be serialized
   */
  @Test
  public void testMappingLifecycle() throws JsonProcessingException {
    OntologyMappingClientOperations mappings =
        new OntologyMappingClientOperations(
            client.restClient(), Namespace.of(METALAKE, "cat", "sch"), "model");
    OntologyMappingResponse response = new OntologyMappingResponse(mapping());

    buildMockResource(
        Method.GET,
        MAPPINGS,
        null,
        new EntityListResponse(
            new NameIdentifier[] {NameIdentifier.of(METALAKE, "cat", "sch", "model", "m")}),
        200);
    Assertions.assertArrayEquals(new String[] {"m"}, mappings.listOntologyMappings());

    buildMockResource(
        Method.POST,
        MAPPINGS,
        new OntologyMappingCreateRequest("m", null, "Sales", MAPPING_DEFINITION, Map.of()),
        response,
        200);
    OntologyMapping created =
        mappings.createOntologyMapping(
            "m",
            null,
            "Sales",
            OntologyMappingDefinition.of(MAPPING_DEFINITION),
            Collections.emptyMap());
    Assertions.assertEquals("Sales", created.ontology());

    buildMockResource(Method.GET, MAPPINGS + "/m", null, response, 200);
    Assertions.assertEquals("m", mappings.loadOntologyMapping("m").name());

    buildMockResource(
        Method.PUT,
        MAPPINGS + "/m",
        OntologyUpdatesRequest.fromMappingChanges(OntologyMappingChange.updateComment("c")),
        response,
        200);
    Assertions.assertEquals(
        "m", mappings.alterOntologyMapping("m", OntologyMappingChange.updateComment("c")).name());

    buildMockResource(Method.DELETE, MAPPINGS + "/m", null, new DropResponse(false), 200);
    Assertions.assertFalse(mappings.dropOntologyMapping("m"));
  }

  /**
   * Verifies that server error types become the matching client exceptions.
   *
   * @throws JsonProcessingException if the mock bodies cannot be serialized
   */
  @Test
  public void testErrorTranslation() throws JsonProcessingException {
    OntologyClientOperations ontologies =
        new OntologyClientOperations(client.restClient(), METALAKE);
    OntologyMappingClientOperations mappings =
        new OntologyMappingClientOperations(
            client.restClient(), Namespace.of(METALAKE, "cat", "sch"), "model");

    buildMockResource(
        Method.GET,
        ONTOLOGIES + "/Missing",
        null,
        ErrorResponse.notFound(NoSuchOntologyException.class.getSimpleName(), "missing"),
        HttpStatus.SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchOntologyException.class, () -> ontologies.loadOntology("Missing"));

    buildMockResource(
        Method.DELETE,
        ONTOLOGIES + "/Sales",
        null,
        ErrorResponse.inUse(OntologyInUseException.class.getSimpleName(), "in use", null),
        HttpStatus.SC_CONFLICT);
    Assertions.assertThrows(OntologyInUseException.class, () -> ontologies.dropOntology("Sales"));

    buildMockResource(
        Method.POST,
        ONTOLOGIES,
        new OntologyCreateRequest("Sales", null, DEFINITION, Map.of()),
        ErrorResponse.alreadyExists(OntologyAlreadyExistsException.class.getSimpleName(), "exists"),
        HttpStatus.SC_CONFLICT);
    Assertions.assertThrows(
        OntologyAlreadyExistsException.class,
        () ->
            ontologies.createOntology(
                "Sales", null, OntologyDefinition.of(DEFINITION), Collections.emptyMap()));

    buildMockResource(
        Method.GET,
        MAPPINGS + "/missing",
        null,
        ErrorResponse.notFound(NoSuchOntologyMappingException.class.getSimpleName(), "missing"),
        HttpStatus.SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchOntologyMappingException.class, () -> mappings.loadOntologyMapping("missing"));

    buildMockResource(
        Method.PUT,
        MAPPINGS + "/m",
        OntologyUpdatesRequest.fromMappingChanges(OntologyMappingChange.rename("n")),
        ErrorResponse.illegalArguments(
            IllegalOntologyMappingException.class.getSimpleName(),
            "$.conceptMappings[0].concept: unknown concept",
            null),
        HttpStatus.SC_BAD_REQUEST);
    Assertions.assertThrows(
        IllegalOntologyMappingException.class,
        () -> mappings.alterOntologyMapping("m", OntologyMappingChange.rename("n")));
  }

  private static OntologyDTO ontology() {
    return new OntologyDTO("Sales", "orders", DEFINITION, Map.of(), AUDIT);
  }

  private static OntologyMappingDTO mapping() {
    return new OntologyMappingDTO("m", null, "Sales", MAPPING_DEFINITION, Map.of(), AUDIT);
  }
}
