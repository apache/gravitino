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
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestOssieOntologyDocuments {

  @Test
  void testPinnedFlightsOntology() throws IOException {
    OssieOntologyDocuments.ParsedOntology ontology =
        OssieOntologyDocuments.importOntology(fixture("flights.ontology.yaml"));
    Assertions.assertEquals("Flights", ontology.name());
    Assertions.assertEquals(
        OssieOntologyDocuments.DEFAULT_VERSION,
        ontology.properties().get(OssieOntologyDocuments.VERSION_PROPERTY));
    OntologyReferences.of(ontology.definition());
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
  void testInvalidInheritance() {
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
}
