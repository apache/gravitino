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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.semantic.AIContext;
import org.junit.jupiter.api.Test;

public class TestOntologyDefinition {

  @Test
  public void testBuildDefinitionAndDefensivelyCopyCollections() {
    OntologyConcept account = entityConcept("Account");
    OntologyConcept[] concepts = {account};
    String[] requires = {"Account IS NOT NULL"};

    OntologyDefinition definition =
        OntologyDefinition.builder()
            .withAiContext(AIContext.of("Finance vocabulary"))
            .withRequires(requires)
            .withConcepts(concepts)
            .withPrefixes(ImmutableMap.of("foaf", "http://xmlns.com/foaf/0.1/"))
            .build();

    // The builder arrays are copied, so later mutation is not visible.
    concepts[0] =
        OntologyConcept.builder().withConcept("Mutated").withType(ConceptType.VALUE_TYPE).build();
    requires[0] = "mutated";

    assertArrayEquals(new String[] {"Account IS NOT NULL"}, definition.requires());
    assertEquals(1, definition.concepts().length);
    assertEquals("Account", definition.concepts()[0].concept());
    assertEquals("Finance vocabulary", definition.aiContext().text());

    Map<String, String> copiedPrefixes = definition.prefixes();
    assertEquals("http://xmlns.com/foaf/0.1/", copiedPrefixes.get("foaf"));
    assertThrows(
        UnsupportedOperationException.class, () -> copiedPrefixes.put("ex", "http://example.com/"));
  }

  @Test
  public void testDefinitionEquality() {
    OntologyDefinition first =
        OntologyDefinition.builder().withConcepts(entityConcept("Account")).build();
    OntologyDefinition second =
        OntologyDefinition.builder().withConcepts(entityConcept("Account")).build();
    OntologyDefinition third =
        OntologyDefinition.builder().withConcepts(entityConcept("Balance")).build();

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
    assertNotEquals(first, third);
    assertNotEquals(first, null);
  }

  @Test
  public void testBuildFailsWithoutConcepts() {
    assertThrows(IllegalArgumentException.class, () -> OntologyDefinition.builder().build());
    assertThrows(
        IllegalArgumentException.class,
        () -> OntologyDefinition.builder().withConcepts(new OntologyConcept[0]).build());
  }

  @Test
  public void testBuildFailsOnDuplicateConcept() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                OntologyDefinition.builder()
                    .withConcepts(entityConcept("Account"), entityConcept("Account"))
                    .build());
    assertTrue(exception.getMessage().contains("Account"));
  }

  @Test
  public void testBuildFailsWhenExtendedConceptIsNotDeclared() {
    OntologyConcept child =
        OntologyConcept.builder()
            .withConcept("SavingsAccount")
            .withType(ConceptType.ENTITY_TYPE)
            .withExtendsConcepts("Account")
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> OntologyDefinition.builder().withConcepts(child).build());
    assertTrue(exception.getMessage().contains("SavingsAccount"));
    assertTrue(exception.getMessage().contains("Account"));
  }

  @Test
  public void testBuildFailsWhenRelationshipRoleIsNotDeclared() {
    OntologyRelationship relationship =
        OntologyRelationship.builder()
            .withName("holds")
            .withVerbalizes("an account holds a balance")
            .withRoles(OntologyRole.builder().withConcept("Balance").build())
            .build();
    OntologyConcept account =
        OntologyConcept.builder()
            .withConcept("Account")
            .withType(ConceptType.ENTITY_TYPE)
            .withRelationships(relationship)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> OntologyDefinition.builder().withConcepts(account).build());
    assertTrue(exception.getMessage().contains("Balance"));
  }

  @Test
  public void testBuildFailsOnDuplicateRelationshipName() {
    OntologyRelationship first =
        OntologyRelationship.builder()
            .withName("holds")
            .withVerbalizes("an account holds a balance")
            .build();
    OntologyRelationship second =
        OntologyRelationship.builder()
            .withName("holds")
            .withVerbalizes("a customer holds an account")
            .build();
    OntologyConcept account =
        OntologyConcept.builder()
            .withConcept("Account")
            .withType(ConceptType.ENTITY_TYPE)
            .withRelationships(first)
            .build();
    OntologyConcept customer =
        OntologyConcept.builder()
            .withConcept("Customer")
            .withType(ConceptType.ENTITY_TYPE)
            .withRelationships(second)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> OntologyDefinition.builder().withConcepts(account, customer).build());
    assertTrue(exception.getMessage().contains("holds"));
  }

  @Test
  public void testBuildFailsOnBlankRequiresExpressionOrPrefix() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyDefinition.builder()
                .withConcepts(entityConcept("Account"))
                .withRequires(" ")
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyDefinition.builder()
                .withConcepts(entityConcept("Account"))
                .withPrefixes(ImmutableMap.of(" ", "http://example.com/"))
                .build());
  }

  @Test
  public void testEmptyCollectionsAreNormalized() {
    OntologyDefinition definition =
        OntologyDefinition.builder()
            .withConcepts(entityConcept("Account"))
            .withRequires((String[]) null)
            .withPrefixes(null)
            .build();

    assertEquals(0, definition.requires().length);
    assertTrue(definition.prefixes().isEmpty());
  }

  private static OntologyConcept entityConcept(String name) {
    return OntologyConcept.builder().withConcept(name).withType(ConceptType.ENTITY_TYPE).build();
  }
}
