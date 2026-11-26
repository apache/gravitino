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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.gravitino.exceptions.IllegalOntologyException;

/** Indexes a structurally validated ontology and checks its structured references. */
public final class OntologyReferences {

  private static final Set<String> BUILT_INS =
      ImmutableSet.of(
          "Any", "Boolean", "Date", "DateTime", "Decimal", "Float", "Integer", "String");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Map<String, Concept> concepts = new LinkedHashMap<>();

  private OntologyReferences(OntologyDefinition definition) {
    JsonNode root = MAPPER.valueToTree(definition.fields());
    visit(
        root.path("ontology"),
        "$.ontology",
        (node, path) -> {
          String name = node.path("concept").asText();
          if (BUILT_INS.contains(name)
              || concepts.putIfAbsent(name, new Concept(node, path)) != null) {
            throw new IllegalOntologyException(
                "%s.concept: duplicate or built-in concept '%s'", path, name);
          }
        });
    concepts.values().forEach(this::validateDeclaration);
    validateInheritance();
    concepts.forEach(
        (name, concept) ->
            visit(
                concept.node.path("identifyBy"),
                concept.path + ".identifyBy",
                (item, path) -> {
                  String identifier = item.asText();
                  JsonNode relationship = findRelationshipByDeclaredName(name, identifier);
                  if (relationship == null) {
                    throw new IllegalOntologyException(
                        "%s: unknown identifying relationship '%s'", path, identifier);
                  }
                  if (relationship.path("roles").size() != 1) {
                    throw new IllegalOntologyException(
                        "%s: identifying relationship '%s' must be binary", path, identifier);
                  }
                }));
  }

  /**
   * Validates ontology names, inheritance, and relationship references and builds an index.
   *
   * @param definition The ontology definition after structural validation.
   * @return The validated ontology index.
   */
  public static OntologyReferences of(OntologyDefinition definition) {
    return new OntologyReferences(definition);
  }

  private void validateDeclaration(Concept concept) {
    String conceptName = concept.node.path("concept").asText();
    visit(
        concept.node.path("extends"),
        concept.path + ".extends",
        (item, path) -> {
          String parent = item.asText();
          requireOntologyConcept(parent, path);
          concept.parents.add(parent);
        });
    visit(
        concept.node.path("relationships"),
        concept.path + ".relationships",
        (node, path) -> {
          String name = node.path("name").asText();
          if (concept.relationships.putIfAbsent(name, node) != null) {
            throw new IllegalOntologyException("%s.name: duplicate relationship '%s'", path, name);
          }
          if (node.path("verbalizes").isEmpty()) {
            throw new IllegalOntologyException(
                "%s.verbalizes: at least one verbalization is required", path);
          }
          Set<String> roleNames = new HashSet<>();
          roleNames.add(conceptName);
          visit(
              node.path("roles"),
              path + ".roles",
              (role, rolePath) -> {
                String roleConcept = role.path("concept").asText();
                requireOntologyConcept(roleConcept, rolePath + ".concept");
                String roleName = role.path("name").asText(roleConcept);
                if (!roleNames.add(roleName)) {
                  throw new IllegalOntologyException(
                      "%s: duplicate role name '%s'", rolePath, roleName);
                }
              });
        });
  }

  private void validateInheritance() {
    // Removing root concepts and their edges leaves exactly the cycles and their dependents.
    Map<String, List<String>> children = new HashMap<>();
    Map<String, Integer> remainingParents = new LinkedHashMap<>();
    Deque<String> roots = new ArrayDeque<>(BUILT_INS);
    concepts.forEach(
        (name, concept) -> {
          remainingParents.put(name, concept.parents.size());
          if (concept.parents.isEmpty()) {
            roots.addLast(name);
          }
          concept.parents.forEach(
              parent -> children.computeIfAbsent(parent, ignored -> new ArrayList<>()).add(name));
        });
    while (!roots.isEmpty()) {
      String resolved = roots.removeFirst();
      remainingParents.remove(resolved);
      if (!children.containsKey(resolved)) {
        continue;
      }
      for (String child : children.get(resolved)) {
        if (remainingParents.computeIfPresent(child, (name, pending) -> pending - 1) == 0) {
          roots.addLast(child);
        }
      }
    }
    if (!remainingParents.isEmpty()) {
      String remaining = remainingParents.keySet().iterator().next();
      throw new IllegalOntologyException(
          "%s.extends: inheritance cycle involving or inherited by '%s'",
          concepts.get(remaining).path, remaining);
    }
  }

  @Nullable
  private JsonNode findRelationshipByDeclaredName(String concept, String name) {
    for (String ancestor : lineage(concept)) {
      Concept declaration = concepts.get(ancestor);
      if (declaration != null && declaration.relationships.containsKey(name)) {
        return declaration.relationships.get(name);
      }
    }
    return null;
  }

  private List<String> lineage(String name) {
    List<String> ordered = new ArrayList<>();
    Set<String> visited = new HashSet<>();
    Deque<String> pending = new ArrayDeque<>();
    pending.push(name);
    while (!pending.isEmpty()) {
      String current = pending.pop();
      if (!visited.add(current)) {
        continue;
      }
      ordered.add(current);
      Concept declaration = concepts.get(current);
      if (declaration != null) {
        for (int i = declaration.parents.size() - 1; i >= 0; i--) {
          pending.push(declaration.parents.get(i));
        }
      }
    }
    return ordered;
  }

  private boolean hasConcept(String name) {
    return BUILT_INS.contains(name) || concepts.containsKey(name);
  }

  private void requireOntologyConcept(String name, String path) {
    if (!hasConcept(name)) {
      throw new IllegalOntologyException("%s: unknown concept '%s'", path, name);
    }
  }

  private static void visit(JsonNode items, String path, BiConsumer<JsonNode, String> visitor) {
    for (int i = 0; i < items.size(); i++) {
      visitor.accept(items.get(i), path + "[" + i + "]");
    }
  }

  private static final class Concept {

    private final JsonNode node;
    private final String path;
    private final List<String> parents = new ArrayList<>();
    private final Map<String, JsonNode> relationships = new HashMap<>();

    private Concept(JsonNode node, String path) {
      this.node = node;
      this.path = path;
    }
  }
}
