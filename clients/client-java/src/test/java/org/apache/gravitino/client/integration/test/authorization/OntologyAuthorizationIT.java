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
package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.ErrorHandlers;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.client.RESTClient;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.OntologyInUseException;
import org.apache.gravitino.exceptions.OntologyMappingAlreadyExistsException;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyChange;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.ontology.OntologyMapping;
import org.apache.gravitino.ontology.OntologyMappingChange;
import org.apache.gravitino.ontology.SupportsOntologyMappings;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.semantic.SemanticModelCatalog;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.Test;

/** Exercises the Ossie Flights ontology and mapping through the Java client and a real server. */
public class OntologyAuthorizationIT extends BaseRestApiAuthorizationIT {
  private static final String CATALOG = "ontology_catalog";
  private static final String SCHEMA = "flights";
  private static final String ROLE = "ontology_writer";
  private static final String MODEL = "flights_model";
  private static final String HIDDEN_MODEL = "private_model";
  private static final String MAPPING = "flights_mapping";
  private static final String ONTOLOGY = "Flights";

  /** Checks metadata ownership, mapping authorization, reference conflicts, and YAML/JSON I/O. */
  @Test
  public void testFlightsLifecycleAndVisibility() throws Exception {
    GravitinoMetalake admin = client.loadMetalake(METALAKE);
    GravitinoMetalake writer = normalUserClient.loadMetalake(METALAKE);
    Catalog catalog =
        admin.createCatalog(
            CATALOG,
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            null,
            Map.of(
                "catalog-backend",
                "jdbc",
                "warehouse",
                System.getProperty("java.io.tmpdir") + "/ontology_authorization",
                "uri",
                "jdbc:h2:mem:ontology_authorization;DB_CLOSE_DELAY=-1;MODE=MYSQL;DATABASE_TO_LOWER=TRUE",
                "jdbc-driver",
                "org.h2.Driver",
                "jdbc-initialize",
                "true"));
    catalog.asSchemas().createSchema(SCHEMA, null, Map.of());
    NameIdentifier table = NameIdentifier.of(SCHEMA, "airports");
    catalog
        .asTableCatalog()
        .createTable(table, new Column[] {Column.of("id", Types.LongType.get())}, null, Map.of());
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(
                new Dataset[] {
                  Dataset.builder()
                      .withName("AIRPORT")
                      .withSource(NameIdentifier.of(CATALOG, SCHEMA, table.name()))
                      .build()
                })
            .build();
    SemanticModelCatalog models = catalog.asSemanticModelCatalog();
    for (String model : List.of(MODEL, HIDDEN_MODEL)) {
      models.createSemanticModel(NameIdentifier.of(SCHEMA, model), null, definition, Map.of());
    }
    admin.createRole(
        ROLE,
        Map.of(),
        List.of(
            SecurableObjects.ofMetalake(METALAKE, List.of(Privileges.CreateOntology.allow())),
            SecurableObjects.ofCatalog(CATALOG, List.of(Privileges.UseCatalog.allow()))));
    admin.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(CATALOG, SCHEMA, MetadataObject.Type.SCHEMA),
        List.of(Privileges.UseSchema.allow()));
    admin.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(List.of(CATALOG, SCHEMA, MODEL), MetadataObject.Type.SEMANTIC_MODEL),
        List.of(Privileges.ModifySemanticModel.allow()));
    admin.grantRolesToUser(List.of(ROLE), NORMAL_USER);

    Ontology flights = admin.importOntology(OssieDocument.yaml(fixture("flights.ontology.yaml")));
    assertEquals(ONTOLOGY, flights.name());
    assertArrayEquals(new String[0], writer.listOntologies());
    Ontology owned =
        writer.createOntology("OwnedTerms", null, definition("BusinessTerm"), Map.of());
    assertEquals(NORMAL_USER, admin.getOwner(ontology(owned.name())).orElseThrow().name());

    SupportsOntologyMappings mappings =
        writer
            .loadCatalog(CATALOG)
            .asSemanticModelCatalog()
            .loadSemanticModel(NameIdentifier.of(SCHEMA, MODEL))
            .supportsOntologyMappings();
    // Ossie's display name contains spaces. Change only the document reference to match the
    // Gravitino model's identifier so the examples exercise the same concept mappings.
    String originalMapping = fixture("flights.mapping.yaml");
    String boundMapping = originalMapping.replace("name: Flights semantic model", "name: " + MODEL);
    assertNotEquals(originalMapping, boundMapping);
    OssieDocument yaml = OssieDocument.yaml(boundMapping);
    assertThrows(ForbiddenException.class, () -> mappings.importOntologyMapping(yaml));

    admin.grantPrivilegesToRole(ROLE, ontology(ONTOLOGY), List.of(Privileges.UseOntology.allow()));
    OntologyMapping saved = mappings.importOntologyMapping(yaml);
    assertEquals(ONTOLOGY, saved.ontology());
    assertArrayEquals(new String[] {MAPPING}, mappings.listOntologyMappings());
    OssieDocument duplicateMapping =
        OssieDocument.yaml(boundMapping.replace("name: " + MAPPING, "name: alternate_flights"));
    assertThrows(
        OntologyMappingAlreadyExistsException.class,
        () -> mappings.importOntologyMapping(duplicateMapping));

    admin.revokePrivilegesFromRole(
        ROLE, ontology(ONTOLOGY), Set.of(Privileges.UseOntology.allow()));
    assertThrows(
        ForbiddenException.class,
        () ->
            mappings.alterOntologyMapping(
                MAPPING, OntologyMappingChange.replaceDefinition(saved.definition())));
    assertEquals(
        "Mapping metadata",
        mappings
            .alterOntologyMapping(MAPPING, OntologyMappingChange.updateComment("Mapping metadata"))
            .comment());
    admin.grantPrivilegesToRole(ROLE, ontology(ONTOLOGY), Set.of(Privileges.UseOntology.allow()));

    SupportsOntologyMappings preloaded =
        models.loadSemanticModel(NameIdentifier.of(SCHEMA, MODEL)).supportsOntologyMappings();
    admin.disableCatalog(CATALOG);
    try {
      assertThrows(CatalogNotInUseException.class, () -> preloaded.loadOntologyMapping(MAPPING));
      assertThrows(CatalogNotInUseException.class, () -> preloaded.dropOntologyMapping(MAPPING));
    } finally {
      admin.enableCatalog(CATALOG);
    }
    assertEquals(saved.definition(), preloaded.loadOntologyMapping(MAPPING).definition());

    assertThrows(
        ForbiddenException.class,
        () -> writer.alterOntology(ONTOLOGY, OntologyChange.updateComment("change")));

    SupportsOntologyMappings hidden =
        models
            .loadSemanticModel(NameIdentifier.of(SCHEMA, HIDDEN_MODEL))
            .supportsOntologyMappings();
    hidden.createOntologyMapping("hidden_flights", null, ONTOLOGY, saved.definition(), Map.of());
    assertArrayEquals(
        new NameIdentifier[] {NameIdentifier.of(CATALOG, SCHEMA, MODEL, MAPPING)},
        writer.listOntologyMappings(ONTOLOGY));
    RESTClient writerRest = restClient(normalUserClient);
    NameIdentifier[] fullIdentifiers =
        new NameIdentifier[] {NameIdentifier.of(METALAKE, CATALOG, SCHEMA, MODEL, MAPPING)};
    String metalakePath = "api/metalakes/" + METALAKE;
    for (String path :
        List.of(
            metalakePath + "/ontologies/" + ONTOLOGY + "/mappings",
            metalakePath
                + "/catalogs/"
                + CATALOG
                + "/schemas/"
                + SCHEMA
                + "/semantic-models/"
                + MODEL
                + "/ontology-mappings")) {
      assertArrayEquals(
          fullIdentifiers,
          writerRest
              .get(path, EntityListResponse.class, Map.of(), ErrorHandlers.ontologyErrorHandler())
              .identifiers());
    }
    assertEquals(2, admin.listOntologyMappings(ONTOLOGY).length);
    assertArrayEquals(new String[] {"Flights", "OwnedTerms"}, writer.listOntologies());

    admin.createTag("ontology_label", null, Map.of());
    flights.supportsTags().associateTags(new String[] {"ontology_label"}, new String[0]);
    assertArrayEquals(new String[] {"ontology_label"}, flights.supportsTags().listTags());
    assertArrayEquals(new String[0], flights.supportsPolicies().listPolicies());

    admin.grantPrivilegesToRole(
        ROLE, ontology(ONTOLOGY), List.of(Privileges.ModifyOntology.allow()));
    OntologyInUseException conflict =
        assertThrows(
            OntologyInUseException.class,
            () ->
                writer.alterOntology(
                    ONTOLOGY, OntologyChange.replaceDefinition(definition("Unrelated"))));
    assertTrue(conflict.getMessage().contains(MAPPING));
    assertTrue(conflict.getMessage().contains("1 mapping the caller cannot load"));
    assertFalse(conflict.getMessage().contains("hidden_flights"));
    assertEquals(saved.definition(), mappings.loadOntologyMapping(MAPPING).definition());

    assertEquals(
        "FlightsV2", writer.alterOntology(ONTOLOGY, OntologyChange.rename("FlightsV2")).name());
    assertEquals("FlightsV2", writer.loadOntology("FlightsV2").name());
    assertThrows(ForbiddenException.class, () -> writer.dropOntology("FlightsV2"));
    assertThrows(OntologyInUseException.class, () -> admin.dropOntology("FlightsV2"));
    assertEquals(
        "FlightsV2",
        mappings
            .alterOntologyMapping(MAPPING, OntologyMappingChange.updateComment("New comment"))
            .ontology());

    models.alterSemanticModel(
        NameIdentifier.of(SCHEMA, MODEL), SemanticModelChange.rename("updated_model"));
    SupportsOntologyMappings moved =
        models
            .loadSemanticModel(NameIdentifier.of(SCHEMA, "updated_model"))
            .supportsOntologyMappings();
    JsonNode exported =
        new ObjectMapper()
            .readTree(moved.exportOntologyMapping(MAPPING, OssieFormat.JSON).content());
    assertEquals("FlightsV2", exported.path("ontology_ref").path("name").asText());
    assertEquals("updated_model", exported.path("semantic_model_ref").path("name").asText());
    assertFalse(exported.path("semantic_model_ref").has("iri"));
    assertTrue(moved.dropOntologyMapping(MAPPING));
    assertTrue(models.dropSemanticModel(NameIdentifier.of(SCHEMA, HIDDEN_MODEL)));
    assertTrue(admin.dropOntology("FlightsV2"));
  }

  /** Checks list admission before per-ontology visibility filtering. */
  @Test
  public void testOntologyListRejectsNonMembersAndUnheldRoles() throws Exception {
    String path = "api/metalakes/" + METALAKE + "/ontologies";
    try (GravitinoAdminClient nonMember =
            GravitinoAdminClient.builder(serverUri).withSimpleAuth("ontology_visitor").build();
        GravitinoAdminClient unheld =
            GravitinoAdminClient.builder(serverUri)
                .withSimpleAuth(NORMAL_USER)
                .withHeaders(
                    Map.of(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER, "unheld_ontology_role"))
                .build()) {
      RESTClient nonMemberRest = restClient(nonMember);
      RESTClient unheldRest = restClient(unheld);
      assertThrows(
          ForbiddenException.class,
          () ->
              nonMemberRest.get(
                  path, NameListResponse.class, Map.of(), ErrorHandlers.ontologyErrorHandler()));
      ForbiddenException failure =
          assertThrows(
              ForbiddenException.class,
              () ->
                  unheldRest.get(
                      path,
                      NameListResponse.class,
                      Map.of(),
                      ErrorHandlers.ontologyErrorHandler()));
      assertTrue(failure.getMessage().contains("cannot assume active role"));
    }
  }

  /** Exercises ontology operations through the metalake-scoped client. */
  @Test
  public void testScopedClientAndDisabledMetalake() throws IOException {
    String name = "ClientTerms";
    try (GravitinoClient scoped =
        GravitinoClient.builder(serverUri).withMetalake(METALAKE).withSimpleAuth(USER).build()) {
      assertEquals(
          name, scoped.createOntology(name, null, definition("Customer"), Map.of()).name());
      assertTrue(List.of(scoped.listOntologies()).contains(name));
      assertEquals(name, scoped.loadOntology(name).name());
      assertArrayEquals(new NameIdentifier[0], scoped.listOntologyMappings(name));
      assertEquals(
          "Facade ontology",
          scoped.alterOntology(name, OntologyChange.updateComment("Facade ontology")).comment());
      JsonNode exported =
          new ObjectMapper().readTree(scoped.exportOntology(name, OssieFormat.JSON).content());
      assertEquals(name, exported.path("name").asText());
      assertEquals(
          "ImportedClientTerms",
          scoped
              .importOntology(
                  OssieDocument.yaml(
                      """
              version: 0.3.0.dev0
              name: ImportedClientTerms
              ontology:
                - concept: Customer
                  type: EntityType
              """))
              .name());

      client.disableMetalake(METALAKE);
      try {
        assertThrows(MetalakeNotInUseException.class, scoped::listOntologies);
        assertThrows(MetalakeNotInUseException.class, () -> scoped.loadOntology(name));
        assertThrows(MetalakeNotInUseException.class, () -> scoped.dropOntology(name));
      } finally {
        client.enableMetalake(METALAKE);
      }
      assertTrue(scoped.dropOntology(name));
      assertTrue(scoped.dropOntology("ImportedClientTerms"));
    }
  }

  private static MetadataObject ontology(String name) {
    return MetadataObjects.of(null, name, MetadataObject.Type.ONTOLOGY);
  }

  private static OntologyDefinition definition(String concept) {
    return OntologyDefinition.of(
        Map.of("ontology", List.of(Map.of("concept", concept, "type", "EntityType"))));
  }

  private static String fixture(String name) throws IOException {
    try (InputStream stream =
        OntologyAuthorizationIT.class.getResourceAsStream("/ossie/ontology/" + name)) {
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
