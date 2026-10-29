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
package org.apache.gravitino.dto.ontology;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.semantic.AIContextDTO;
import org.apache.gravitino.ontology.OntologyConcept;
import org.apache.gravitino.ontology.OntologyDefinition;

/** The Data Transfer Object representing an ontology definition. */
@Getter
@EqualsAndHashCode
@ToString
public class OntologyDefinitionDTO {

  @JsonProperty("aiContext")
  @Nullable
  private final AIContextDTO aiContext;

  @JsonProperty("requires")
  private final String[] requires;

  @JsonProperty("concepts")
  private final OntologyConceptDTO[] concepts;

  @JsonProperty("prefixes")
  private final Map<String, String> prefixes;

  /**
   * Creates a new ontology definition DTO.
   *
   * @param aiContext The AI context shared by the whole ontology, or {@code null}.
   * @param requires The ontology-level SQL expressions, or {@code null}.
   * @param concepts The concept components.
   * @param prefixes The prefix to IRI mapping, or {@code null}.
   */
  public OntologyDefinitionDTO(
      @Nullable AIContextDTO aiContext,
      @Nullable String[] requires,
      OntologyConceptDTO[] concepts,
      @Nullable Map<String, String> prefixes) {
    this.aiContext = aiContext;
    this.requires = requires == null ? null : requires.clone();
    this.concepts = concepts == null ? new OntologyConceptDTO[0] : concepts.clone();
    this.prefixes = prefixes;
  }

  /** This is the constructor that is used by Jackson deserializer. */
  public OntologyDefinitionDTO() {
    this.aiContext = null;
    this.requires = null;
    this.concepts = new OntologyConceptDTO[0];
    this.prefixes = null;
  }

  /**
   * Returns the ontology-level SQL expressions.
   *
   * @return The expressions, never {@code null}.
   */
  public String[] requires() {
    return requires == null ? new String[0] : requires.clone();
  }

  /**
   * Returns the concept components.
   *
   * @return The concepts, never {@code null}.
   */
  public OntologyConceptDTO[] concepts() {
    return concepts.clone();
  }

  /**
   * Returns the prefix to IRI mapping.
   *
   * @return The prefixes, never {@code null}.
   */
  public Map<String, String> prefixes() {
    return prefixes == null ? Collections.emptyMap() : prefixes;
  }

  /**
   * Converts this DTO to the native ontology definition.
   *
   * @return The native definition.
   * @throws IllegalArgumentException If the definition is not a valid ontology.
   */
  public OntologyDefinition toOntologyDefinition() {
    List<OntologyConcept> nativeConcepts =
        Arrays.stream(concepts)
            .map(OntologyConceptDTO::toOntologyConcept)
            .collect(Collectors.toList());
    return OntologyDefinition.builder()
        .withAiContext(aiContext == null ? null : aiContext.toAIContext())
        .withRequires(requires())
        .withConcepts(nativeConcepts.toArray(new OntologyConcept[0]))
        .withPrefixes(prefixes())
        .build();
  }

  /**
   * Converts a native ontology definition to its DTO form.
   *
   * @param definition The native definition.
   * @return The DTO.
   */
  public static OntologyDefinitionDTO fromOntologyDefinition(OntologyDefinition definition) {
    Preconditions.checkArgument(definition != null, "definition must not be null");

    OntologyConcept[] nativeConcepts = definition.concepts();
    OntologyConceptDTO[] conceptDTOs = new OntologyConceptDTO[nativeConcepts.length];
    for (int index = 0; index < nativeConcepts.length; index++) {
      conceptDTOs[index] = OntologyConceptDTO.fromOntologyConcept(nativeConcepts[index]);
    }
    return new OntologyDefinitionDTO(
        definition.aiContext() == null ? null : AIContextDTO.fromAIContext(definition.aiContext()),
        definition.requires(),
        conceptDTOs,
        definition.prefixes());
  }

  /**
   * Validates the ontology definition DTO.
   *
   * @throws IllegalArgumentException If the definition is invalid.
   */
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(concepts != null && concepts.length > 0, "concepts must be set");
    for (int index = 0; index < concepts.length; index++) {
      Preconditions.checkArgument(concepts[index] != null, "concepts[%s] must not be null", index);
      concepts[index].validate();
    }
    if (requires != null) {
      for (int index = 0; index < requires.length; index++) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(requires[index]), "requires[%s] must not be blank", index);
      }
    }
    if (prefixes != null) {
      prefixes.forEach(
          (prefix, iri) -> {
            Preconditions.checkArgument(
                StringUtils.isNotBlank(prefix) && StringUtils.isNotBlank(iri),
                "prefixes must not contain a blank prefix or IRI");
          });
    }
    // The native type owns the cross-reference rules, so validate through it as well.
    toOntologyDefinition();
  }
}
