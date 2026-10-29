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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestOntologyMembers {

  @Test
  public void testRoleValueSemantics() {
    OntologyRole role = OntologyRole.builder().withConcept("Account").withName("source").build();
    OntologyRole same = OntologyRole.builder().withConcept("Account").withName("source").build();
    OntologyRole unnamed = OntologyRole.builder().withConcept("Account").build();

    assertEquals("Account", role.concept());
    assertEquals("source", role.name());
    assertNull(unnamed.name());
    assertEquals(role, same);
    assertEquals(role.hashCode(), same.hashCode());
    assertNotEquals(role, unnamed);
    assertTrue(role.toString().contains("Account"));
  }

  @Test
  public void testRoleRejectsBlankValues() {
    assertThrows(IllegalArgumentException.class, () -> OntologyRole.builder().build());
    assertThrows(
        IllegalArgumentException.class, () -> OntologyRole.builder().withConcept(" ").build());
    assertThrows(
        IllegalArgumentException.class,
        () -> OntologyRole.builder().withConcept("Account").withName("").build());
  }

  @Test
  public void testRelationshipValueSemanticsAndCopies() {
    OntologyRole[] roles = {OntologyRole.builder().withConcept("Account").build()};
    String[] verbalizes = {"an account holds a balance"};
    String[] derivedBy = {"amount"};

    OntologyRelationship relationship =
        OntologyRelationship.builder()
            .withName("holds")
            .withVerbalizes(verbalizes)
            .withDescription("An account holds a balance")
            .withRoles(roles)
            .withMultiplicity(Multiplicity.MANY_TO_ONE)
            .withDerivedBy(derivedBy)
            .withIri("ex:holds")
            .build();

    roles[0] = null;
    verbalizes[0] = "mutated";
    derivedBy[0] = "mutated";

    assertArrayEquals(new String[] {"an account holds a balance"}, relationship.verbalizes());
    assertArrayEquals(new String[] {"amount"}, relationship.derivedBy());
    assertEquals("Account", relationship.roles()[0].concept());
    assertEquals(Multiplicity.MANY_TO_ONE, relationship.multiplicity());
    assertEquals("ex:holds", relationship.iri());
    assertEquals("An account holds a balance", relationship.description());
    assertNull(relationship.requires());
  }

  @Test
  public void testRelationshipEquality() {
    OntologyRelationship first = relationship("holds", "an account holds a balance");
    OntologyRelationship second = relationship("holds", "an account holds a balance");
    OntologyRelationship renamed = relationship("owns", "an account holds a balance");

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
    assertNotEquals(first, renamed);
    assertTrue(first.toString().contains("holds"));
  }

  @Test
  public void testRelationshipRequiresAtLeastOneVerbalization() {
    IllegalArgumentException missing =
        assertThrows(
            IllegalArgumentException.class,
            () -> OntologyRelationship.builder().withName("holds").build());
    assertTrue(missing.getMessage().contains("verbalization"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyRelationship.builder().withName("holds").withVerbalizes(new String[0]).build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyRelationship.builder()
                .withName("holds")
                .withVerbalizes("an account holds a balance", " ")
                .build());
  }

  @Test
  public void testRelationshipRejectsNullElements() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyRelationship.builder()
                .withName("holds")
                .withVerbalizes("an account holds a balance")
                .withRoles((OntologyRole) null)
                .build());
  }

  @Test
  public void testConceptValueSemanticsAndCopies() {
    String[] identifyBy = {"account_id"};
    OntologyConcept concept =
        OntologyConcept.builder()
            .withConcept("Account")
            .withType(ConceptType.ENTITY_TYPE)
            .withDescription("A bank account")
            .withExtendsConcepts("Party")
            .withIdentifyBy(identifyBy)
            .withIri("ex:Account")
            .build();

    identifyBy[0] = "mutated";

    assertEquals("Account", concept.concept());
    assertEquals(ConceptType.ENTITY_TYPE, concept.type());
    assertArrayEquals(new String[] {"account_id"}, concept.identifyBy());
    assertArrayEquals(new String[] {"Party"}, concept.extendsConcepts());
    assertEquals("ex:Account", concept.iri());
    assertNull(concept.relationships());
    assertTrue(concept.toString().contains("Account"));
  }

  @Test
  public void testConceptRequiresNameAndType() {
    assertThrows(IllegalArgumentException.class, () -> OntologyConcept.builder().build());
    assertThrows(
        IllegalArgumentException.class,
        () -> OntologyConcept.builder().withConcept("Account").build());
    assertThrows(
        IllegalArgumentException.class,
        () -> OntologyConcept.builder().withType(ConceptType.ENTITY_TYPE).build());
  }

  @Test
  public void testConceptRejectsDuplicateRelationshipNameWithinItself() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                OntologyConcept.builder()
                    .withConcept("Account")
                    .withType(ConceptType.ENTITY_TYPE)
                    .withRelationships(
                        relationship("holds", "an account holds a balance"),
                        relationship("holds", "an account holds a limit"))
                    .build());
    assertTrue(exception.getMessage().contains("holds"));
  }

  private static OntologyRelationship relationship(String name, String verbalization) {
    return OntologyRelationship.builder()
        .withName(name)
        .withVerbalizes(verbalization)
        .withMultiplicity(Multiplicity.ONE_TO_ONE)
        .build();
  }
}
