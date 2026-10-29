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

import static org.apache.hc.core5.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.hc.core5.http.HttpStatus.SC_CONFLICT;
import static org.apache.hc.core5.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.ontology.OntologyConceptDTO;
import org.apache.gravitino.dto.ontology.OntologyDTO;
import org.apache.gravitino.dto.ontology.OntologyDefinitionDTO;
import org.apache.gravitino.dto.ontology.OntologyRelationshipDTO;
import org.apache.gravitino.dto.ontology.OntologyRoleDTO;
import org.apache.gravitino.dto.requests.OntologyCreateRequest;
import org.apache.gravitino.dto.requests.OntologyUpdateRequest;
import org.apache.gravitino.dto.requests.OntologyUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.OntologyListResponse;
import org.apache.gravitino.dto.responses.OntologyResponse;
import org.apache.gravitino.exceptions.IllegalOntologyException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchOntologyException;
import org.apache.gravitino.exceptions.NonEmptyOntologyException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.OntologyAlreadyExistsException;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.ontology.ConceptType;
import org.apache.gravitino.ontology.Multiplicity;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyChange;
import org.apache.gravitino.ontology.OntologyConcept;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests the ontology operations of {@link GravitinoMetalake}. */
public class TestOntologyClient extends TestBase {

  private static final String METALAKE_NAME = "metalake";

  private static final String ONTOLOGY_NAME = "Finance";

  private static final String ONTOLOGY_PATH = "/api/metalakes/" + METALAKE_NAME + "/ontologies";

  private static GravitinoMetalake metalake;

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
    metalake = TestGravitinoMetalake.createMetalake(client, METALAKE_NAME);
  }

  @Test
  public void testListOntologies() throws JsonProcessingException {
    String[] names = new String[] {"Finance", "Sales"};
    buildMockResource(Method.GET, ONTOLOGY_PATH, null, new NameListResponse(names), SC_OK);

    Assertions.assertArrayEquals(names, metalake.listOntologies());

    // Test the empty ontology list
    buildMockResource(Method.GET, ONTOLOGY_PATH, null, new NameListResponse(new String[0]), SC_OK);
    Assertions.assertEquals(0, metalake.listOntologies().length);

    // Test throw NoSuchMetalakeException
    ErrorResponse notFound =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, ONTOLOGY_PATH, null, notFound, SC_NOT_FOUND);
    Throwable ex = Assertions.assertThrows(NoSuchMetalakeException.class, metalake::listOntologies);
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test an unknown not-found type falls back to the generic NotFoundException
    ErrorResponse unknownNotFound = ErrorResponse.notFound("SomeOtherException", "mock error");
    buildMockResource(Method.GET, ONTOLOGY_PATH, null, unknownNotFound, SC_NOT_FOUND);
    Throwable ex1 = Assertions.assertThrows(NotFoundException.class, metalake::listOntologies);
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test an unknown error code falls back to the generic REST handler
    ErrorResponse unknownError = ErrorResponse.unknownError("mock error");
    buildMockResource(Method.GET, ONTOLOGY_PATH, null, unknownError, SC_INTERNAL_SERVER_ERROR);
    Throwable ex2 = Assertions.assertThrows(RESTException.class, metalake::listOntologies);
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse internalError = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, ONTOLOGY_PATH, null, internalError, SC_INTERNAL_SERVER_ERROR);
    Throwable ex3 = Assertions.assertThrows(RuntimeException.class, metalake::listOntologies);
    Assertions.assertTrue(ex3.getMessage().contains("mock error"));
  }

  @Test
  public void testListOntologiesInfo() throws JsonProcessingException {
    Map<String, String> params = ImmutableMap.of("details", "true");

    OntologyDTO finance =
        newOntologyDTO(ONTOLOGY_NAME, "Finance vocabulary", Ontology.Status.DRAFT);
    OntologyDTO sales = newOntologyDTO("Sales", "Sales vocabulary", Ontology.Status.PUBLISHED);
    buildMockResource(
        Method.GET,
        ONTOLOGY_PATH,
        params,
        null,
        new OntologyListResponse(new OntologyDTO[] {finance, sales}),
        SC_OK);

    Ontology[] ontologies = metalake.listOntologiesInfo();
    Assertions.assertEquals(2, ontologies.length);
    Assertions.assertEquals(ONTOLOGY_NAME, ontologies[0].name());
    Assertions.assertEquals("Finance vocabulary", ontologies[0].comment());
    Assertions.assertEquals(Ontology.Status.DRAFT, ontologies[0].status());
    Assertions.assertEquals("Sales", ontologies[1].name());
    Assertions.assertEquals(Ontology.Status.PUBLISHED, ontologies[1].status());
    Assertions.assertEquals(
        "http://example.com/finance/", ontologies[0].definition().prefixes().get("ex"));
    Assertions.assertEquals(2, ontologies[0].definition().concepts().length);

    // Test the empty ontology list
    buildMockResource(
        Method.GET,
        ONTOLOGY_PATH,
        params,
        null,
        new OntologyListResponse(new OntologyDTO[0]),
        SC_OK);
    Assertions.assertEquals(0, metalake.listOntologiesInfo().length);

    // Test throw NoSuchMetalakeException
    ErrorResponse notFound =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, ONTOLOGY_PATH, params, null, notFound, SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(NoSuchMetalakeException.class, metalake::listOntologiesInfo);
    Assertions.assertTrue(ex.getMessage().contains("mock error"));
  }

  @Test
  public void testGetOntology() throws JsonProcessingException {
    String path = ONTOLOGY_PATH + "/" + ONTOLOGY_NAME;

    buildMockResource(
        Method.GET,
        path,
        null,
        new OntologyResponse(
            newOntologyDTO(ONTOLOGY_NAME, "Finance vocabulary", Ontology.Status.DRAFT)),
        SC_OK);

    Ontology ontology = metalake.getOntology(ONTOLOGY_NAME);
    Assertions.assertEquals(ONTOLOGY_NAME, ontology.name());
    Assertions.assertEquals("Finance vocabulary", ontology.comment());
    Assertions.assertEquals(Ontology.Status.DRAFT, ontology.status());
    Assertions.assertEquals(
        "0.2.0.dev0", ontology.properties().get(Ontology.PROPERTY_OSSIE_VERSION));
    Assertions.assertNotNull(ontology.auditInfo());
    Assertions.assertEquals("creator", ontology.auditInfo().creator());

    // Test a blank name
    Throwable ex =
        Assertions.assertThrows(IllegalArgumentException.class, () -> metalake.getOntology(" "));
    Assertions.assertTrue(ex.getMessage().contains("ontology name must not be null or empty"));

    // Test throw NoSuchMetalakeException
    ErrorResponse notFoundMetalake =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, notFoundMetalake, SC_NOT_FOUND);
    Throwable ex1 =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> metalake.getOntology(ONTOLOGY_NAME));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test throw NoSuchOntologyException
    ErrorResponse notFoundOntology =
        ErrorResponse.notFound(NoSuchOntologyException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, notFoundOntology, SC_NOT_FOUND);
    Throwable ex2 =
        Assertions.assertThrows(
            NoSuchOntologyException.class, () -> metalake.getOntology(ONTOLOGY_NAME));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));
  }

  @Test
  public void testCreateOntology() throws JsonProcessingException {
    Map<String, String> properties = ImmutableMap.of(Ontology.PROPERTY_OSSIE_VERSION, "0.2.0.dev0");
    OntologyCreateRequest req =
        new OntologyCreateRequest(
            ONTOLOGY_NAME,
            "Finance vocabulary",
            properties,
            OntologyDefinitionDTO.fromOntologyDefinition(newDefinition()));
    buildMockResource(
        Method.POST,
        ONTOLOGY_PATH,
        req,
        new OntologyResponse(
            newOntologyDTO(ONTOLOGY_NAME, "Finance vocabulary", Ontology.Status.DRAFT)),
        SC_OK);

    Ontology ontology =
        metalake.createOntology(ONTOLOGY_NAME, "Finance vocabulary", properties, newDefinition());
    Assertions.assertEquals(ONTOLOGY_NAME, ontology.name());
    Assertions.assertEquals("Finance vocabulary", ontology.comment());
    Assertions.assertEquals(Ontology.Status.DRAFT, ontology.status());
    Assertions.assertEquals(2, ontology.definition().concepts().length);

    // Test a blank name
    Throwable ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalake.createOntology(" ", null, null, newDefinition()));
    Assertions.assertTrue(ex.getMessage().contains("ontology name must not be null or empty"));

    // Test a null definition
    Throwable ex1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalake.createOntology(ONTOLOGY_NAME, null, null, null));
    Assertions.assertTrue(ex1.getMessage().contains("ontology definition must not be null"));

    // Test throw NoSuchMetalakeException
    ErrorResponse notFoundMetalake =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.POST, ONTOLOGY_PATH, null, notFoundMetalake, SC_NOT_FOUND);
    Throwable ex2 =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> metalake.createOntology(ONTOLOGY_NAME, null, null, newDefinition()));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));

    // Test throw OntologyAlreadyExistsException
    ErrorResponse alreadyExists =
        ErrorResponse.alreadyExists(
            OntologyAlreadyExistsException.class.getSimpleName(), "mock error");
    buildMockResource(Method.POST, ONTOLOGY_PATH, null, alreadyExists, SC_CONFLICT);
    Throwable ex3 =
        Assertions.assertThrows(
            OntologyAlreadyExistsException.class,
            () -> metalake.createOntology(ONTOLOGY_NAME, null, null, newDefinition()));
    Assertions.assertTrue(ex3.getMessage().contains("mock error"));

    // Test throw IllegalOntologyException
    ErrorResponse illegal =
        ErrorResponse.illegalArguments(
            IllegalOntologyException.class.getSimpleName(), "mock error", null);
    buildMockResource(Method.POST, ONTOLOGY_PATH, null, illegal, SC_BAD_REQUEST);
    Throwable ex4 =
        Assertions.assertThrows(
            IllegalOntologyException.class,
            () -> metalake.createOntology(ONTOLOGY_NAME, null, null, newDefinition()));
    Assertions.assertTrue(ex4.getMessage().contains("mock error"));
  }

  @Test
  public void testAlterOntologyRename() throws JsonProcessingException {
    String renamed = "FinanceArchive";
    OntologyUpdatesRequest req =
        updatesRequest(new OntologyUpdateRequest.RenameOntologyRequest(renamed));
    buildMockResource(
        Method.PUT,
        ONTOLOGY_PATH + "/" + ONTOLOGY_NAME,
        req,
        new OntologyResponse(newOntologyDTO(renamed, "Finance vocabulary", Ontology.Status.DRAFT)),
        SC_OK);

    Ontology ontology = metalake.alterOntology(ONTOLOGY_NAME, OntologyChange.rename(renamed));
    Assertions.assertEquals(renamed, ontology.name());
  }

  @Test
  public void testAlterOntologyUpdateComment() throws JsonProcessingException {
    OntologyUpdatesRequest req =
        updatesRequest(new OntologyUpdateRequest.UpdateOntologyCommentRequest("new comment"));
    buildMockResource(
        Method.PUT,
        ONTOLOGY_PATH + "/" + ONTOLOGY_NAME,
        req,
        new OntologyResponse(newOntologyDTO(ONTOLOGY_NAME, "new comment", Ontology.Status.DRAFT)),
        SC_OK);

    Ontology ontology =
        metalake.alterOntology(ONTOLOGY_NAME, OntologyChange.updateComment("new comment"));
    Assertions.assertEquals("new comment", ontology.comment());

    // Test clearing the comment
    OntologyUpdatesRequest clearReq =
        updatesRequest(new OntologyUpdateRequest.UpdateOntologyCommentRequest(null));
    buildMockResource(
        Method.PUT,
        ONTOLOGY_PATH + "/" + ONTOLOGY_NAME,
        clearReq,
        new OntologyResponse(newOntologyDTO(ONTOLOGY_NAME, null, Ontology.Status.DRAFT)),
        SC_OK);

    Ontology cleared = metalake.alterOntology(ONTOLOGY_NAME, OntologyChange.updateComment(null));
    Assertions.assertNull(cleared.comment());
  }

  @Test
  public void testAlterOntologySetProperty() throws JsonProcessingException {
    OntologyUpdatesRequest req =
        updatesRequest(new OntologyUpdateRequest.SetOntologyPropertyRequest("owner", "finance"));
    buildMockResource(
        Method.PUT,
        ONTOLOGY_PATH + "/" + ONTOLOGY_NAME,
        req,
        new OntologyResponse(
            newOntologyDTO(ONTOLOGY_NAME, "Finance vocabulary", Ontology.Status.DRAFT)),
        SC_OK);

    Ontology ontology =
        metalake.alterOntology(ONTOLOGY_NAME, OntologyChange.setProperty("owner", "finance"));
    Assertions.assertEquals(ONTOLOGY_NAME, ontology.name());
  }

  @Test
  public void testAlterOntologyRemoveProperty() throws JsonProcessingException {
    OntologyUpdatesRequest req =
        updatesRequest(new OntologyUpdateRequest.RemoveOntologyPropertyRequest("owner"));
    buildMockResource(
        Method.PUT,
        ONTOLOGY_PATH + "/" + ONTOLOGY_NAME,
        req,
        new OntologyResponse(
            newOntologyDTO(ONTOLOGY_NAME, "Finance vocabulary", Ontology.Status.DRAFT)),
        SC_OK);

    Ontology ontology =
        metalake.alterOntology(ONTOLOGY_NAME, OntologyChange.removeProperty("owner"));
    Assertions.assertEquals(ONTOLOGY_NAME, ontology.name());
  }

  @Test
  public void testAlterOntologyReplaceDefinition() throws JsonProcessingException {
    OntologyDefinition replacement =
        OntologyDefinition.builder()
            .withConcepts(
                new OntologyConcept[] {newConcept("Customer", ConceptType.ENTITY_TYPE, null)})
            .build();
    OntologyUpdatesRequest req =
        updatesRequest(
            new OntologyUpdateRequest.ReplaceOntologyDefinitionRequest(
                OntologyDefinitionDTO.fromOntologyDefinition(replacement)));
    OntologyDTO altered =
        newOntologyDTO(ONTOLOGY_NAME, "Finance vocabulary", Ontology.Status.DRAFT);
    buildMockResource(
        Method.PUT, ONTOLOGY_PATH + "/" + ONTOLOGY_NAME, req, new OntologyResponse(altered), SC_OK);

    Ontology ontology =
        metalake.alterOntology(ONTOLOGY_NAME, OntologyChange.replaceDefinition(replacement));
    Assertions.assertEquals(ONTOLOGY_NAME, ontology.name());
  }

  @Test
  public void testAlterOntologyUpdateStatus() throws JsonProcessingException {
    OntologyUpdatesRequest req =
        updatesRequest(
            new OntologyUpdateRequest.UpdateOntologyStatusRequest(Ontology.Status.PUBLISHED));
    buildMockResource(
        Method.PUT,
        ONTOLOGY_PATH + "/" + ONTOLOGY_NAME,
        req,
        new OntologyResponse(
            newOntologyDTO(ONTOLOGY_NAME, "Finance vocabulary", Ontology.Status.PUBLISHED)),
        SC_OK);

    Ontology ontology =
        metalake.alterOntology(
            ONTOLOGY_NAME, OntologyChange.updateStatus(Ontology.Status.PUBLISHED));
    Assertions.assertEquals(Ontology.Status.PUBLISHED, ontology.status());
  }

  @Test
  public void testAlterOntologyWithInvalidChanges() {
    // Test an unknown change type
    Throwable ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalake.alterOntology(ONTOLOGY_NAME, new UnsupportedChange()));
    Assertions.assertTrue(ex.getMessage().contains(UnsupportedChange.class.getSimpleName()));

    // Test a null change
    Throwable ex1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalake.alterOntology(ONTOLOGY_NAME, (OntologyChange) null));
    Assertions.assertTrue(ex1.getMessage().contains("ontology change must not be null"));

    // Test no changes
    Throwable ex2 =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> metalake.alterOntology(ONTOLOGY_NAME));
    Assertions.assertTrue(ex2.getMessage().contains("updates must not be empty"));

    // Test a null change array
    Throwable ex3 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalake.alterOntology(ONTOLOGY_NAME, (OntologyChange[]) null));
    Assertions.assertTrue(ex3.getMessage().contains("ontology changes must not be null"));

    // Test a blank name
    Throwable ex4 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalake.alterOntology(" ", OntologyChange.rename("Sales")));
    Assertions.assertTrue(ex4.getMessage().contains("ontology name must not be null or empty"));
  }

  @Test
  public void testAlterOntologyErrors() throws JsonProcessingException {
    String path = ONTOLOGY_PATH + "/" + ONTOLOGY_NAME;

    // Test throw NoSuchOntologyException
    ErrorResponse notFound =
        ErrorResponse.notFound(NoSuchOntologyException.class.getSimpleName(), "mock error");
    buildMockResource(Method.PUT, path, null, notFound, SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchOntologyException.class,
            () -> metalake.alterOntology(ONTOLOGY_NAME, OntologyChange.rename("Sales")));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw OntologyAlreadyExistsException
    ErrorResponse alreadyExists =
        ErrorResponse.alreadyExists(
            OntologyAlreadyExistsException.class.getSimpleName(), "mock error");
    buildMockResource(Method.PUT, path, null, alreadyExists, SC_CONFLICT);
    Throwable ex1 =
        Assertions.assertThrows(
            OntologyAlreadyExistsException.class,
            () -> metalake.alterOntology(ONTOLOGY_NAME, OntologyChange.rename("Sales")));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test throw IllegalOntologyException
    ErrorResponse illegal =
        ErrorResponse.illegalArguments(
            IllegalOntologyException.class.getSimpleName(), "mock error", null);
    buildMockResource(Method.PUT, path, null, illegal, SC_BAD_REQUEST);
    Throwable ex2 =
        Assertions.assertThrows(
            IllegalOntologyException.class,
            () ->
                metalake.alterOntology(
                    ONTOLOGY_NAME, OntologyChange.replaceDefinition(newDefinition())));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));

    // Test a plain illegal argument error
    ErrorResponse plainIllegal = ErrorResponse.illegalArguments("mock error");
    buildMockResource(Method.PUT, path, null, plainIllegal, SC_BAD_REQUEST);
    Throwable ex3 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalake.alterOntology(ONTOLOGY_NAME, OntologyChange.rename("Sales")));
    Assertions.assertTrue(ex3.getMessage().contains("mock error"));
  }

  @Test
  public void testDeleteOntology() throws JsonProcessingException {
    String path = ONTOLOGY_PATH + "/" + ONTOLOGY_NAME;

    buildMockResource(Method.DELETE, path, null, new DropResponse(true), SC_OK);
    Assertions.assertTrue(metalake.deleteOntology(ONTOLOGY_NAME));

    // Test the ontology does not exist
    buildMockResource(Method.DELETE, path, null, new DropResponse(false), SC_OK);
    Assertions.assertFalse(metalake.deleteOntology(ONTOLOGY_NAME));

    // Test a blank name
    Throwable ex =
        Assertions.assertThrows(IllegalArgumentException.class, () -> metalake.deleteOntology(" "));
    Assertions.assertTrue(ex.getMessage().contains("ontology name must not be null or empty"));

    // Test throw NonEmptyOntologyException
    ErrorResponse nonEmpty =
        ErrorResponse.nonEmpty(NonEmptyOntologyException.class.getSimpleName(), "mock error");
    buildMockResource(Method.DELETE, path, null, nonEmpty, SC_CONFLICT);
    Throwable ex1 =
        Assertions.assertThrows(
            NonEmptyOntologyException.class, () -> metalake.deleteOntology(ONTOLOGY_NAME));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test throw NoSuchMetalakeException
    ErrorResponse notFound =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.DELETE, path, null, notFound, SC_NOT_FOUND);
    Throwable ex2 =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> metalake.deleteOntology(ONTOLOGY_NAME));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));
  }

  private static OntologyUpdatesRequest updatesRequest(OntologyUpdateRequest update) {
    return new OntologyUpdatesRequest(Collections.singletonList(update));
  }

  private static OntologyConcept newConcept(String concept, ConceptType type, String iri) {
    return OntologyConcept.builder().withConcept(concept).withType(type).withIri(iri).build();
  }

  private static OntologyDefinition newDefinition() {
    OntologyRelationshipDTO relationship =
        new OntologyRelationshipDTO(
            "holds",
            null,
            new String[] {"an account holds a balance"},
            new OntologyRoleDTO[] {new OntologyRoleDTO("Balance", null)},
            Multiplicity.MANY_TO_ONE,
            null,
            null,
            "ex:holds",
            null);
    OntologyConceptDTO account =
        new OntologyConceptDTO(
            "Account",
            ConceptType.ENTITY_TYPE,
            null,
            null,
            null,
            null,
            new String[] {"Account IS NOT NULL"},
            new OntologyRelationshipDTO[] {relationship},
            "ex:Account",
            null);
    OntologyConceptDTO balance =
        new OntologyConceptDTO(
            "Balance",
            ConceptType.VALUE_TYPE,
            null,
            null,
            null,
            null,
            null,
            null,
            "ex:Balance",
            null);
    return new OntologyDefinitionDTO(
            null,
            new String[] {"Account IS NOT NULL"},
            new OntologyConceptDTO[] {account, balance},
            ImmutableMap.of("ex", "http://example.com/finance/"))
        .toOntologyDefinition();
  }

  private static AuditDTO newAudit() {
    return AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
  }

  private static OntologyDTO newOntologyDTO(String name, String comment, Ontology.Status status) {
    return new OntologyDTO(
        name,
        comment,
        ImmutableMap.of(Ontology.PROPERTY_OSSIE_VERSION, "0.2.0.dev0"),
        status,
        OntologyDefinitionDTO.fromOntologyDefinition(newDefinition()),
        newAudit());
  }

  /** An ontology change the client cannot translate into a REST request. */
  private static final class UnsupportedChange implements OntologyChange {}
}
