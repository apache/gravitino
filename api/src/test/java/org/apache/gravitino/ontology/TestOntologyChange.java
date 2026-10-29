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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestOntologyChange {

  @Test
  public void testRenameChange() {
    OntologyChange change = OntologyChange.rename("FinanceVocabulary");

    assertTrue(change instanceof OntologyChange.RenameOntology);
    assertEquals("FinanceVocabulary", ((OntologyChange.RenameOntology) change).getNewName());
    assertEquals(change, OntologyChange.rename("FinanceVocabulary"));
    assertEquals(change.hashCode(), OntologyChange.rename("FinanceVocabulary").hashCode());
    assertNotEquals(change, OntologyChange.rename("Other"));
    assertTrue(change.toString().contains("FinanceVocabulary"));

    assertThrows(IllegalArgumentException.class, () -> OntologyChange.rename(null));
    assertThrows(IllegalArgumentException.class, () -> OntologyChange.rename(" "));
  }

  @Test
  public void testUpdateCommentChange() {
    OntologyChange change = OntologyChange.updateComment("Finance concepts");
    OntologyChange removal = OntologyChange.updateComment(null);

    assertEquals("Finance concepts", ((OntologyChange.UpdateComment) change).getNewComment());
    assertNull(((OntologyChange.UpdateComment) removal).getNewComment());
    assertEquals(change, OntologyChange.updateComment("Finance concepts"));
    assertNotEquals(change, removal);
    assertTrue(change.toString().contains("Finance concepts"));
  }

  @Test
  public void testPropertyChanges() {
    OntologyChange set = OntologyChange.setProperty("owner", "finance");
    OntologyChange remove = OntologyChange.removeProperty("owner");

    assertEquals("owner", ((OntologyChange.SetProperty) set).getProperty());
    assertEquals("finance", ((OntologyChange.SetProperty) set).getValue());
    assertEquals("owner", ((OntologyChange.RemoveProperty) remove).getProperty());
    assertEquals(set, OntologyChange.setProperty("owner", "finance"));
    assertNotEquals(set, OntologyChange.setProperty("owner", "risk"));
    assertNotEquals(set, remove);

    assertThrows(IllegalArgumentException.class, () -> OntologyChange.setProperty(" ", "value"));
    assertThrows(IllegalArgumentException.class, () -> OntologyChange.setProperty("owner", null));
    assertThrows(IllegalArgumentException.class, () -> OntologyChange.removeProperty(" "));
  }

  @Test
  public void testReplaceDefinitionChange() {
    OntologyDefinition definition =
        OntologyDefinition.builder()
            .withConcepts(
                OntologyConcept.builder()
                    .withConcept("Account")
                    .withType(ConceptType.ENTITY_TYPE)
                    .build())
            .build();

    OntologyChange change = OntologyChange.replaceDefinition(definition);

    assertEquals(definition, ((OntologyChange.ReplaceDefinition) change).getDefinition());
    assertEquals(change, OntologyChange.replaceDefinition(definition));
    assertTrue(change.toString().contains("OntologyDefinition"));
    assertThrows(IllegalArgumentException.class, () -> OntologyChange.replaceDefinition(null));
  }

  @Test
  public void testUpdateStatusChange() {
    OntologyChange publish = OntologyChange.updateStatus(Ontology.Status.PUBLISHED);
    OntologyChange draft = OntologyChange.updateStatus(Ontology.Status.DRAFT);

    assertEquals(Ontology.Status.PUBLISHED, ((OntologyChange.UpdateStatus) publish).getStatus());
    assertEquals(publish, OntologyChange.updateStatus(Ontology.Status.PUBLISHED));
    assertNotEquals(publish, draft);
    assertTrue(publish.toString().contains("PUBLISHED"));
    assertThrows(IllegalArgumentException.class, () -> OntologyChange.updateStatus(null));
  }

  @Test
  public void testStatusEnumHasDomainShape() {
    assertEquals(2, Ontology.Status.values().length);
    assertEquals(Ontology.Status.DRAFT, Ontology.Status.valueOf("DRAFT"));
    assertEquals(Ontology.Status.PUBLISHED, Ontology.Status.valueOf("PUBLISHED"));
  }

  @Test
  public void testConceptTypeAndMultiplicityAreClosedSets() {
    assertEquals(2, ConceptType.values().length);
    assertEquals(2, Multiplicity.values().length);
  }
}
