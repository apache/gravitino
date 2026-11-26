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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.ontology;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TestOntologyApi {

  @Test
  void testDefinitionCopiesNestedFields() {
    Map<String, Object> concept = new LinkedHashMap<>(Map.of("concept", "Customer"));
    List<Map<String, Object>> concepts = new ArrayList<>(List.of(concept));
    Map<String, Object> fields = new LinkedHashMap<>(Map.of("ontology", concepts));
    OntologyDefinition definition = OntologyDefinition.of(fields);

    concept.put("concept", "Order");
    concepts.clear();
    fields.clear();

    List<?> storedConcepts = assertInstanceOf(List.class, definition.fields().get("ontology"));
    Map<?, ?> storedConcept = assertInstanceOf(Map.class, storedConcepts.get(0));
    assertEquals("Customer", storedConcept.get("concept"));
    assertThrows(UnsupportedOperationException.class, definition.fields()::clear);
    assertThrows(UnsupportedOperationException.class, storedConcepts::clear);
    assertThrows(UnsupportedOperationException.class, storedConcept::clear);
  }

  @Test
  void testDefinitionRejectsNonJsonFields() {
    assertThrows(
        IllegalArgumentException.class,
        () -> OntologyDefinition.of(Map.of("annotations", Map.of(1, "value"))));
    assertThrows(
        IllegalArgumentException.class,
        () -> OntologyDefinition.of(Map.of("annotations", Double.NaN)));
    assertThrows(
        IllegalArgumentException.class,
        () -> OntologyDefinition.of(Map.of("annotations", new Object())));
  }

  @Test
  void testChangeFactories() {
    OntologyDefinition definition = OntologyDefinition.of(Map.of("ontology", List.of()));
    OntologyChange.RenameOntology rename =
        assertInstanceOf(OntologyChange.RenameOntology.class, OntologyChange.rename("Sales"));
    OntologyChange.UpdateComment comment =
        assertInstanceOf(OntologyChange.UpdateComment.class, OntologyChange.updateComment(null));
    OntologyChange.SetProperty set =
        assertInstanceOf(
            OntologyChange.SetProperty.class, OntologyChange.setProperty("team", "bi"));
    OntologyChange.RemoveProperty remove =
        assertInstanceOf(
            OntologyChange.RemoveProperty.class, OntologyChange.removeProperty("team"));
    OntologyChange.ReplaceDefinition replace =
        assertInstanceOf(
            OntologyChange.ReplaceDefinition.class, OntologyChange.replaceDefinition(definition));

    assertEquals("Sales", rename.getNewName());
    assertNull(comment.getNewComment());
    assertEquals("team", set.getProperty());
    assertEquals("bi", set.getValue());
    assertEquals("team", remove.getProperty());
    assertEquals(definition, replace.getDefinition());
  }

  @Test
  void testRejectInvalidChanges() {
    assertThrows(IllegalArgumentException.class, () -> OntologyChange.rename(""));
    assertThrows(IllegalArgumentException.class, () -> OntologyChange.setProperty("", "value"));
    assertThrows(
        IllegalArgumentException.class, () -> OntologyChange.setProperty("property", null));
    assertThrows(IllegalArgumentException.class, () -> OntologyChange.removeProperty(""));
    assertThrows(IllegalArgumentException.class, () -> OntologyChange.replaceDefinition(null));
  }
}
