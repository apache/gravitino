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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.exceptions.IllegalOntologyException;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.junit.jupiter.api.Test;

public class TestOssieOntologyConverter {

  private static final String VALID_DOCUMENT =
      "{\n"
          + "  \"version\": \"0.2.0.dev0\",\n"
          + "  \"name\": \"Finance\",\n"
          + "  \"description\": \"Finance vocabulary\",\n"
          + "  \"ai_context\": \"Certified\",\n"
          + "  \"requires\": [\"Account IS NOT NULL\"],\n"
          + "  \"prefixes\": {\"ex\": \"http://example.com/finance/\"},\n"
          + "  \"ontology\": [\n"
          + "    {\"concept\": \"Party\", \"type\": \"EntityType\"},\n"
          + "    {\n"
          + "      \"concept\": \"Account\",\n"
          + "      \"type\": \"EntityType\",\n"
          + "      \"extends\": [\"Party\"],\n"
          + "      \"identify_by\": [\"account_id\"],\n"
          + "      \"iri\": \"ex:Account\",\n"
          + "      \"relationships\": [\n"
          + "        {\n"
          + "          \"name\": \"holds\",\n"
          + "          \"verbalizes\": [\"an account holds a balance\"],\n"
          + "          \"roles\": [{\"concept\": \"Balance\"}],\n"
          + "          \"multiplicity\": \"ManyToOne\",\n"
          + "          \"iri\": \"ex:holds\"\n"
          + "        }\n"
          + "      ]\n"
          + "    },\n"
          + "    {\"concept\": \"Balance\", \"type\": \"ValueType\"}\n"
          + "  ]\n"
          + "}";

  @Test
  public void testImportJsonDocument() {
    OssieOntologyConverter.ParsedOntology parsed =
        OssieOntologyConverter.importDocument(OssieDocument.json(VALID_DOCUMENT));

    assertEquals("Finance", parsed.name());
    assertEquals("Finance vocabulary", parsed.comment());
    assertEquals(
        "0.2.0.dev0", parsed.properties().get(OssieOntologyConverter.PROPERTY_OSSIE_VERSION));

    OntologyDefinition definition = parsed.definition();
    assertEquals(3, definition.concepts().length);
    assertEquals(1, definition.requires().length);
    assertEquals("http://example.com/finance/", definition.prefixes().get("ex"));
    assertEquals("Certified", definition.aiContext().text());

    OntologyConcept account = definition.concepts()[1];
    assertEquals("Account", account.concept());
    assertEquals(ConceptType.ENTITY_TYPE, account.type());
    assertArrayEquals(new String[] {"Party"}, account.extendsConcepts());
    assertArrayEquals(new String[] {"account_id"}, account.identifyBy());
    assertEquals("ex:Account", account.iri());

    OntologyRelationship holds = account.relationships()[0];
    assertEquals("holds", holds.name());
    assertArrayEquals(new String[] {"an account holds a balance"}, holds.verbalizes());
    assertEquals(Multiplicity.MANY_TO_ONE, holds.multiplicity());
    assertEquals("Balance", holds.roles()[0].concept());
    assertEquals("ex:holds", holds.iri());
  }

  @Test
  public void testImportYamlDocument() {
    String yaml =
        "version: 0.2.0.dev0\n"
            + "name: Finance\n"
            + "ontology:\n"
            + "  - concept: Account\n"
            + "    type: EntityType\n";
    OssieOntologyConverter.ParsedOntology parsed =
        OssieOntologyConverter.importDocument(OssieDocument.yaml(yaml));

    assertEquals("Finance", parsed.name());
    assertNull(parsed.comment());
    assertEquals(1, parsed.definition().concepts().length);
    assertEquals(0, parsed.definition().requires().length);
    assertTrue(parsed.definition().prefixes().isEmpty());
  }

  @Test
  public void testImportRejectsUnknownRootProperty() {
    String document =
        VALID_DOCUMENT.replace("\"name\": \"Finance\",", "\"name\": \"Finance\", \"extra\": 1,");
    IllegalOntologyException exception =
        assertThrows(
            IllegalOntologyException.class,
            () -> OssieOntologyConverter.importDocument(OssieDocument.json(document)));
    assertTrue(exception.getMessage().contains("$.extra"));
  }

  @Test
  public void testImportRejectsMissingName() {
    String document = VALID_DOCUMENT.replace("\"name\": \"Finance\",", "");
    assertThrows(
        IllegalOntologyException.class,
        () -> OssieOntologyConverter.importDocument(OssieDocument.json(document)));
  }

  @Test
  public void testImportRejectsEmbeddedOntologyMappings() {
    String document =
        VALID_DOCUMENT.replace("\"ontology\": [", "\"ontology_mappings\": [], \"ontology\": [");
    IllegalOntologyException exception =
        assertThrows(
            IllegalOntologyException.class,
            () -> OssieOntologyConverter.importDocument(OssieDocument.json(document)));
    assertTrue(exception.getMessage().contains("mapping.json"));
  }

  @Test
  public void testImportRejectsRelationshipWithoutVerbalizes() {
    String document =
        VALID_DOCUMENT.replace("\"verbalizes\": [\"an account holds a balance\"],", "");
    IllegalOntologyException exception =
        assertThrows(
            IllegalOntologyException.class,
            () -> OssieOntologyConverter.importDocument(OssieDocument.json(document)));
    assertTrue(exception.getMessage().contains("verbalization"));
  }

  @Test
  public void testImportRejectsBlankVerbalization() {
    String document =
        VALID_DOCUMENT.replace(
            "[\"an account holds a balance\"]", "[\"an account holds a balance\", \" \"]");
    assertThrows(
        IllegalOntologyException.class,
        () -> OssieOntologyConverter.importDocument(OssieDocument.json(document)));
  }

  @Test
  public void testImportRejectsClosedConceptType() {
    String document = VALID_DOCUMENT.replace("\"type\": \"ValueType\"", "\"type\": \"Term\"");
    IllegalOntologyException exception =
        assertThrows(
            IllegalOntologyException.class,
            () -> OssieOntologyConverter.importDocument(OssieDocument.json(document)));
    assertTrue(exception.getMessage().contains("EntityType"));
  }

  @Test
  public void testImportRejectsMultiplicityOutsideClosedSet() {
    String document =
        VALID_DOCUMENT.replace(
            "\"multiplicity\": \"ManyToOne\"", "\"multiplicity\": \"ManyToMany\"");
    assertThrows(
        IllegalOntologyException.class,
        () -> OssieOntologyConverter.importDocument(OssieDocument.json(document)));
  }

  @Test
  public void testImportRejectsExtendsOutsideDocument() {
    String document =
        VALID_DOCUMENT.replace("\"extends\": [\"Party\"]", "\"extends\": [\"Person\"]");
    IllegalOntologyException exception =
        assertThrows(
            IllegalOntologyException.class,
            () -> OssieOntologyConverter.importDocument(OssieDocument.json(document)));
    assertTrue(exception.getMessage().contains("Person"));
  }

  @Test
  public void testImportRejectsRoleConceptOutsideDocument() {
    String document =
        VALID_DOCUMENT.replace(
            "\"roles\": [{\"concept\": \"Balance\"}]", "\"roles\": [{\"concept\": \"Ledger\"}]");
    IllegalOntologyException exception =
        assertThrows(
            IllegalOntologyException.class,
            () -> OssieOntologyConverter.importDocument(OssieDocument.json(document)));
    assertTrue(exception.getMessage().contains("Ledger"));
  }

  @Test
  public void testImportRejectsUndeclaredQNamePrefix() {
    String document = VALID_DOCUMENT.replace("\"ex:Account\"", "\"fin:Account\"");
    IllegalOntologyException exception =
        assertThrows(
            IllegalOntologyException.class,
            () -> OssieOntologyConverter.importDocument(OssieDocument.json(document)));
    assertTrue(exception.getMessage().contains("fin"));
  }

  @Test
  public void testImportAcceptsAbsoluteIriWithoutPrefix() {
    String document =
        VALID_DOCUMENT.replace("\"ex:Account\"", "\"http://example.com/finance/Account\"");
    OssieOntologyConverter.ParsedOntology parsed =
        OssieOntologyConverter.importDocument(OssieDocument.json(document));
    assertEquals("http://example.com/finance/Account", parsed.definition().concepts()[1].iri());
  }

  @Test
  public void testImportRejectsBareIri() {
    String document = VALID_DOCUMENT.replace("\"ex:Account\"", "\"Account\"");
    assertThrows(
        IllegalOntologyException.class,
        () -> OssieOntologyConverter.importDocument(OssieDocument.json(document)));
  }

  @Test
  public void testExportJsonAndRoundTrip() {
    OssieOntologyConverter.ParsedOntology ontology = ontology(definition(), ImmutableMap.of());
    OssieDocument exported = OssieOntologyConverter.exportDocument(ontology, OssieFormat.JSON);

    assertTrue(exported.content().contains("\"version\" : \"0.2.0.dev0\""));
    assertTrue(exported.content().contains("\"name\" : \"Finance\""));

    OssieOntologyConverter.ParsedOntology reparsed =
        OssieOntologyConverter.importDocument(exported);
    assertEquals("Finance", reparsed.name());
    assertEquals("Finance vocabulary", reparsed.comment());
    assertEquals(definition(), reparsed.definition());
  }

  @Test
  public void testExportYamlRoundTrip() {
    OssieOntologyConverter.ParsedOntology ontology = ontology(definition(), ImmutableMap.of());
    OssieDocument exported = OssieOntologyConverter.exportDocument(ontology, OssieFormat.YAML);

    assertEquals(OssieFormat.YAML, exported.format());
    assertTrue(exported.content().contains("Finance"));

    OssieOntologyConverter.ParsedOntology reparsed =
        OssieOntologyConverter.importDocument(exported);
    assertEquals(definition(), reparsed.definition());
  }

  @Test
  public void testExportUsesOssieVersionProperty() {
    OssieOntologyConverter.ParsedOntology ontology =
        ontology(
            definition(),
            ImmutableMap.of(OssieOntologyConverter.PROPERTY_OSSIE_VERSION, "0.2.0.dev0"));
    OssieDocument exported = OssieOntologyConverter.exportDocument(ontology, OssieFormat.JSON);
    assertEquals(
        "0.2.0.dev0",
        OssieOntologyConverter.importDocument(exported)
            .properties()
            .get(OssieOntologyConverter.PROPERTY_OSSIE_VERSION));
  }

  @Test
  public void testExportOmitsEmptyOptionalSections() {
    OntologyDefinition minimal =
        OntologyDefinition.builder()
            .withConcepts(
                OntologyConcept.builder()
                    .withConcept("Account")
                    .withType(ConceptType.ENTITY_TYPE)
                    .build())
            .build();
    OssieDocument exported =
        OssieOntologyConverter.exportDocument(
            new OssieOntologyConverter.ParsedOntology("Finance", null, minimal, ImmutableMap.of()),
            OssieFormat.JSON);

    assertFalse(exported.content().contains("requires"));
    assertFalse(exported.content().contains("prefixes"));
    assertFalse(exported.content().contains("description"));
  }

  @Test
  public void testExportRejectsInvalidOntology() {
    OssieOntologyConverter.ParsedOntology broken =
        new OssieOntologyConverter.ParsedOntology(
            "Finance",
            null,
            OntologyDefinition.builder()
                .withConcepts(
                    OntologyConcept.builder()
                        .withConcept("Account")
                        .withType(ConceptType.ENTITY_TYPE)
                        .build())
                .build(),
            ImmutableMap.of(OssieOntologyConverter.PROPERTY_OSSIE_VERSION, " "));

    assertThrows(
        IllegalOntologyException.class,
        () -> OssieOntologyConverter.exportDocument(broken, OssieFormat.JSON));
  }

  private static OntologyDefinition definition() {
    OntologyRelationship holds =
        OntologyRelationship.builder()
            .withName("holds")
            .withVerbalizes("an account holds a balance")
            .withRoles(OntologyRole.builder().withConcept("Balance").build())
            .withMultiplicity(Multiplicity.MANY_TO_ONE)
            .build();
    return OntologyDefinition.builder()
        .withAiContext(AIContext.of("Certified"))
        .withRequires("Account IS NOT NULL")
        .withConcepts(
            OntologyConcept.builder()
                .withConcept("Account")
                .withType(ConceptType.ENTITY_TYPE)
                .withRelationships(holds)
                .build(),
            OntologyConcept.builder()
                .withConcept("Balance")
                .withType(ConceptType.VALUE_TYPE)
                .build())
        .withPrefixes(ImmutableMap.of("ex", "http://example.com/finance/"))
        .build();
  }

  private static OssieOntologyConverter.ParsedOntology ontology(
      OntologyDefinition definition, Map<String, String> properties) {
    return new OssieOntologyConverter.ParsedOntology(
        "Finance", "Finance vocabulary", definition, properties);
  }
}
