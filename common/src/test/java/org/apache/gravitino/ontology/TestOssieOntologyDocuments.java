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
package org.apache.gravitino.ontology;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.exceptions.IllegalOntologyException;
import org.apache.gravitino.exceptions.IllegalOntologyMappingException;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestOssieOntologyDocuments {

  @Test
  void testPinnedFlightsOntologyAndMapping() throws IOException {
    OssieOntologyDocuments.ParsedOntology ontology =
        OssieOntologyDocuments.importOntology(fixture("flights.ontology.yaml"));
    OssieOntologyDocuments.ParsedMapping mapping =
        OssieOntologyDocuments.importMapping(
            fixture("flights.mapping.yaml"), "Flights semantic model");

    Assertions.assertEquals("Flights", ontology.name());
    Assertions.assertEquals(ontology.name(), mapping.ontology());
    Assertions.assertEquals(
        OssieOntologyDocuments.DEFAULT_VERSION,
        ontology.properties().get(OssieOntologyDocuments.VERSION_PROPERTY));
    OntologyReferences.of(ontology.definition()).validateMapping(mapping.definition());

    OntologyMapping current =
        new CurrentMapping(
            mapping.name(),
            mapping.comment(),
            "Renamed",
            mapping.definition(),
            Map.of("owner", "team"));
    for (OssieFormat format : OssieFormat.values()) {
      OssieDocument out = OssieOntologyDocuments.exportMapping(current, "Renamed Model", format);
      Assertions.assertFalse(out.content().contains("flights.semantic_model.yaml"));
      OssieOntologyDocuments.ParsedMapping roundTrip =
          OssieOntologyDocuments.importMapping(out, "Renamed Model");
      Assertions.assertEquals("Renamed", roundTrip.ontology());
      Assertions.assertEquals("team", roundTrip.properties().get("owner"));
      Assertions.assertEquals(mapping.definition(), roundTrip.definition());
    }
  }

  @Test
  void testVersionAndOpaqueAnnotationsRoundTrip() {
    OssieOntologyDocuments.ParsedOntology parsed =
        OssieOntologyDocuments.importOntology(
            OssieDocument.yaml(
                "version: 0.3.0.dev9\nname: Example\n"
                    + "ai_context: {instructions: explanation}\n"
                    + "annotations: {identify_by: vendor, gravitino_properties: {team: analytics}}\n"
                    + "ontology: [{concept: Party, type: EntityType}]\n"));
    Ontology ontology =
        new CurrentOntology(
            parsed.name(), parsed.comment(), parsed.definition(), parsed.properties());
    OssieDocument json = OssieOntologyDocuments.exportOntology(ontology, OssieFormat.JSON);
    OssieOntologyDocuments.ParsedOntology roundTrip = OssieOntologyDocuments.importOntology(json);
    Assertions.assertEquals(parsed.definition(), roundTrip.definition());
    Assertions.assertEquals(
        Map.of("ossie-version", "0.3.0.dev9", "team", "analytics"), roundTrip.properties());
    Assertions.assertEquals(
        Map.of("identify_by", "vendor"), roundTrip.definition().fields().get("annotations"));
  }

  @Test
  void testAnyNonBlankVersionIsAccepted() throws IOException {
    // The vendored schemas relax the upstream root version constraint to minLength 1. Re-vendoring
    // the upstream files without re-applying that change makes this test fail.
    OssieOntologyDocuments.ParsedOntology ontology =
        OssieOntologyDocuments.importOntology(withVersion(fixture("flights.ontology.yaml"), "1"));
    Assertions.assertEquals(
        "1", ontology.properties().get(OssieOntologyDocuments.VERSION_PROPERTY));

    OssieOntologyDocuments.ParsedMapping mapping =
        OssieOntologyDocuments.importMapping(
            withVersion(fixture("flights.mapping.yaml"), "1"), "Flights semantic model");
    Assertions.assertEquals("1", mapping.properties().get(OssieOntologyDocuments.VERSION_PROPERTY));
  }

  @Test
  void testInvalidDocumentShape() {
    IllegalOntologyException badRole =
        Assertions.assertThrows(
            IllegalOntologyException.class,
            () ->
                OssieOntologyDocuments.importOntology(
                    OssieDocument.yaml(
                        "version: dev\nname: Example\n"
                            + "ontology: [{concept: X, type: EntityType, relationships: [{name: r, verbalizes: []"
                            + ", roles: [{name: roleWithoutConcept}]}]}]\n")));
    Assertions.assertTrue(badRole.getMessage().contains("$.ontology[0].relationships[0].roles[0]"));
    Assertions.assertThrows(
        IllegalOntologyException.class,
        () ->
            OssieOntologyDocuments.importOntology(
                OssieDocument.yaml(
                    "version: dev\nname: Example\nontology_mappings: []\nontology: []\n")));
    Assertions.assertThrows(
        IllegalOntologyException.class,
        () ->
            OssieOntologyDocuments.importOntology(
                OssieDocument.yaml(
                    "version: dev\nname: Example\nname: Duplicate\nontology: []\n")));
    Assertions.assertThrows(
        IllegalOntologyMappingException.class,
        () -> OssieOntologyDocuments.importMapping(fixture("flights.mapping.yaml"), "Other model"));
    Assertions.assertThrows(
        IllegalOntologyMappingException.class,
        () ->
            OssieOntologyDocuments.importMapping(
                OssieDocument.yaml(
                    "version: dev\nname: Example\nontology_ref: {name: Sales}\n"
                        + "semantic_model_ref: {name: model}\nconcept_mappings: [{concept: Party}]\n"),
                "model"));
  }

  @Test
  void testDefinitionCopiesCallerData() {
    List<Map<String, String>> concepts = new ArrayList<>();
    concepts.add(Map.of("concept", "Party", "type", "EntityType"));
    OntologyDefinition definition = OntologyDefinition.of(Map.of("ontology", concepts));
    concepts.clear();
    Assertions.assertEquals(
        List.of(Map.of("concept", "Party", "type", "EntityType")),
        definition.fields().get("ontology"));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> definition.fields().clear());
  }

  @Test
  void testInvalidReferencesAndArity() {
    OntologyDefinition definition =
        OntologyDefinition.of(
            Map.of(
                "ontology",
                List.of(
                    Map.of(
                        "concept",
                        "Party",
                        "type",
                        "EntityType",
                        "relationships",
                        List.of(
                            Map.of(
                                "name",
                                "number",
                                "roles",
                                List.of(Map.of("concept", "Integer")),
                                "verbalizes",
                                List.of("number")))),
                    Map.of(
                        "concept",
                        "Customer",
                        "type",
                        "EntityType",
                        "extends",
                        List.of("Party"),
                        "identifyBy",
                        List.of("number")))));
    OntologyReferences references = OntologyReferences.of(definition);
    references.validateMapping(
        OntologyMappingDefinition.of(
            Map.of(
                "conceptMappings",
                List.of(
                    Map.of(
                        "concept",
                        "Customer",
                        "objectMappings",
                        List.of(
                            Map.of(
                                "referentMappings",
                                List.of(
                                    Map.of(
                                        "relationship",
                                        "Party.number",
                                        "expression",
                                        "raw sql")))))))));
    IllegalOntologyMappingException error =
        Assertions.assertThrows(
            IllegalOntologyMappingException.class,
            () ->
                references.validateMapping(
                    OntologyMappingDefinition.of(
                        Map.of(
                            "conceptMappings",
                            List.of(
                                Map.of(
                                    "concept",
                                    "Customer",
                                    "linkMappings",
                                    List.of(
                                        Map.of(
                                            "relationship",
                                            "number",
                                            "objectMapping",
                                            Map.of("expression", "customers.id")))))))));
    Assertions.assertTrue(
        error.getMessage().contains("$.conceptMappings[0].linkMappings[0].relationship"));
    Assertions.assertThrows(
        IllegalOntologyException.class,
        () ->
            OntologyReferences.of(
                OntologyDefinition.of(
                    Map.of(
                        "ontology",
                        List.of(
                            Map.of(
                                "concept",
                                "Customer",
                                "type",
                                "EntityType",
                                "extends",
                                List.of("Party")),
                            Map.of(
                                "concept",
                                "Party",
                                "type",
                                "EntityType",
                                "extends",
                                List.of("Customer")))))));
  }

  @Test
  void testIdentifyByUsesTheDeclaredRelationshipName() {
    String ontology =
        """
        version: 0.3.0.dev0
        name: Places
        ontology:
          - concept: Place
            type: EntityType
            relationships:
              - name: geo.distance
                roles: [{concept: Integer}]
                verbalizes: [distance]
          - concept: Airport
            type: EntityType
            extends: [Place]
            identify_by: [%s]
        """;
    OntologyDefinition exactName =
        OssieOntologyDocuments.importOntology(
                OssieDocument.yaml(ontology.formatted("geo.distance")))
            .definition();
    OntologyReferences.of(exactName);

    OntologyDefinition qualifiedName =
        OssieOntologyDocuments.importOntology(
                OssieDocument.yaml(ontology.formatted("Place.geo.distance")))
            .definition();
    IllegalOntologyException error =
        Assertions.assertThrows(
            IllegalOntologyException.class, () -> OntologyReferences.of(qualifiedName));
    Assertions.assertTrue(error.getMessage().contains("$.ontology[1].identifyBy[0]"));
  }

  @Test
  void testOntologyRoleNamesAndIdentifierArity() {
    String ontology =
        """
        version: 0.3.0.dev0
        name: Sales
        ontology:
          - concept: Customer
            type: EntityType
            identify_by: [%s]
            relationships:
              - name: parent
                roles: [{concept: Customer%s}]
                verbalizes: [parent]
              - name: active
                verbalizes: [active]
        """;
    OntologyReferences.of(
        OssieOntologyDocuments.importOntology(
                OssieDocument.yaml(ontology.formatted("parent", ", name: parent")))
            .definition());
    IllegalOntologyException duplicate =
        Assertions.assertThrows(
            IllegalOntologyException.class,
            () ->
                OntologyReferences.of(
                    OssieOntologyDocuments.importOntology(
                            OssieDocument.yaml(ontology.formatted("parent", "")))
                        .definition()));
    Assertions.assertTrue(
        duplicate.getMessage().contains("$.ontology[0].relationships[0].roles[0]"));
    IllegalOntologyException nonBinary =
        Assertions.assertThrows(
            IllegalOntologyException.class,
            () ->
                OntologyReferences.of(
                    OssieOntologyDocuments.importOntology(
                            OssieDocument.yaml(ontology.formatted("active", ", name: parent")))
                        .definition()));
    Assertions.assertTrue(nonBinary.getMessage().contains("$.ontology[0].identifyBy[0]"));
  }

  @Test
  void testMappingReferencesFollowOntologyIdentifiers() {
    String ontology =
        """
        version: 0.3.0.dev0
        name: Sales
        ontology:
          - {concept: EmailAddress, type: ValueType, extends: [String]}
          - concept: Customer
            type: EntityType
            identify_by: [%s]
            relationships:
              - name: id
                roles: [{concept: Integer}]
                verbalizes: [id]
              - name: email
                roles: [{concept: EmailAddress}]
                verbalizes: [email]
        """;
    OntologyMappingDefinition referent =
        mappingFor(
            "Customer",
            Map.of(
                "referentMappings",
                List.of(Map.of("relationship", "Customer.id", "expression", "customers.id"))));
    OntologyMappingDefinition expression =
        mappingFor("Customer", Map.of("expression", "customers.id"));
    OntologyReferences simple =
        OntologyReferences.of(
            OssieOntologyDocuments.importOntology(OssieDocument.yaml(ontology.formatted("id")))
                .definition());
    simple.validateMapping(referent);
    simple.validateMapping(expression);

    OntologyReferences changed =
        OntologyReferences.of(
            OssieOntologyDocuments.importOntology(OssieDocument.yaml(ontology.formatted("email")))
                .definition());
    changed.validateMapping(mappingFor("Customer", Map.of("expression", "customers.email")));
    IllegalOntologyMappingException wrongReferent =
        Assertions.assertThrows(
            IllegalOntologyMappingException.class, () -> changed.validateMapping(referent));
    Assertions.assertTrue(
        wrongReferent
            .getMessage()
            .contains("$.conceptMappings[0].objectMappings[0].referentMappings[0].relationship"));

    OntologyReferences compound =
        OntologyReferences.of(
            OssieOntologyDocuments.importOntology(
                    OssieDocument.yaml(ontology.formatted("id, email")))
                .definition());
    IllegalOntologyMappingException missingReferents =
        Assertions.assertThrows(
            IllegalOntologyMappingException.class, () -> compound.validateMapping(expression));
    Assertions.assertTrue(
        missingReferents
            .getMessage()
            .contains("$.conceptMappings[0].objectMappings[0].expression"));
    compound.validateMapping(
        mappingFor(
            "Customer",
            Map.of(
                "referentMappings",
                List.of(
                    Map.of("relationship", "id", "expression", "customers.id"),
                    Map.of("relationship", "email", "expression", "customers.email")))));
  }

  @Test
  void testExpressionOnlyEntityMappingsRequireASimpleIdentifier() {
    String ontology =
        """
        version: 0.3.0.dev0
        name: Billing
        ontology:
          - concept: Order
            type: EntityType
            identify_by: [%s]
            relationships:
              - {name: store_code, roles: [{concept: String}], verbalizes: [store]}
              - {name: order_no, roles: [{concept: Integer}], verbalizes: [number]}
          - concept: Invoice
            type: EntityType
            identify_by: [order]
            relationships:
              - {name: order, roles: [{concept: Order}], verbalizes: [order]}
          - {concept: Unidentified, type: EntityType}
        """;
    OntologyReferences compoundOrder =
        OntologyReferences.of(
            OssieOntologyDocuments.importOntology(
                    OssieDocument.yaml(ontology.formatted("store_code, order_no")))
                .definition());
    Map<String, String> orderNumber = Map.of("expression", "sales.order_no");
    for (String entity : List.of("Invoice", "Unidentified", "Any")) {
      IllegalOntologyMappingException error =
          Assertions.assertThrows(
              IllegalOntologyMappingException.class,
              () -> compoundOrder.validateMapping(mappingFor(entity, orderNumber)),
              entity);
      Assertions.assertTrue(
          error.getMessage().contains("$.conceptMappings[0].objectMappings[0].expression"));
      Assertions.assertTrue(error.getMessage().contains("'" + entity + "'"));
    }

    List<Map<String, String>> orderIdentifiers =
        List.of(
            Map.of("relationship", "store_code", "expression", "sales.store_code"),
            Map.of("relationship", "order_no", "expression", "sales.order_no"));
    Map<String, Object> orderReferent =
        Map.of("relationship", "order", "referentMappings", orderIdentifiers);
    compoundOrder.validateMapping(
        mappingFor("Invoice", Map.of("referentMappings", List.of(orderReferent))));

    OntologyReferences simpleOrder =
        OntologyReferences.of(
            OssieOntologyDocuments.importOntology(
                    OssieDocument.yaml(ontology.formatted("order_no")))
                .definition());
    simpleOrder.validateMapping(mappingFor("Order", orderNumber));
    Assertions.assertThrows(
        IllegalOntologyMappingException.class,
        () -> simpleOrder.validateMapping(mappingFor("Invoice", orderNumber)));
  }

  private static OntologyMappingDefinition mappingFor(
      String concept, Map<String, ?> objectMapping) {
    return OntologyMappingDefinition.of(
        Map.of(
            "conceptMappings",
            List.of(Map.of("concept", concept, "objectMappings", List.of(objectMapping)))));
  }

  private static OssieDocument withVersion(OssieDocument document, String version) {
    String content =
        document.content().replaceFirst("(?m)^version: .*$", "version: \"" + version + "\"");
    Assertions.assertNotEquals(document.content(), content);
    return OssieDocument.yaml(content);
  }

  private static OssieDocument fixture(String name) throws IOException {
    try (InputStream stream =
        TestOssieOntologyDocuments.class.getResourceAsStream("/ossie/ontology/" + name)) {
      return OssieDocument.yaml(new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @SuppressWarnings("UnusedVariable")
  private record CurrentOntology(
      String name, String comment, OntologyDefinition definition, Map<String, String> properties)
      implements Ontology {
    @Override
    public Audit auditInfo() {
      throw new UnsupportedOperationException();
    }
  }

  @SuppressWarnings("UnusedVariable")
  private record CurrentMapping(
      String name,
      String comment,
      String ontology,
      OntologyMappingDefinition definition,
      Map<String, String> properties)
      implements OntologyMapping {
    @Override
    public Audit auditInfo() {
      throw new UnsupportedOperationException();
    }
  }
}
