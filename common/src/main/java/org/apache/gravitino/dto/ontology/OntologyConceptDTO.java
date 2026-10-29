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
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.ontology.ConceptType;
import org.apache.gravitino.ontology.OntologyConcept;
import org.apache.gravitino.ontology.OntologyRelationship;

/** The Data Transfer Object representing one concept of an ontology. */
@Getter
@EqualsAndHashCode
@ToString
public class OntologyConceptDTO {

  @JsonProperty("concept")
  private final String concept;

  @JsonProperty("type")
  private final ConceptType type;

  @JsonProperty("description")
  @Nullable
  private final String description;

  @JsonProperty("extends")
  @Nullable
  private final String[] extendsConcepts;

  @JsonProperty("derivedBy")
  @Nullable
  private final String[] derivedBy;

  @JsonProperty("identifyBy")
  @Nullable
  private final String[] identifyBy;

  @JsonProperty("requires")
  @Nullable
  private final String[] requires;

  @JsonProperty("relationships")
  @Nullable
  private final OntologyRelationshipDTO[] relationships;

  @JsonProperty("iri")
  @Nullable
  private final String iri;

  /**
   * Creates a new ontology concept DTO.
   *
   * @param concept The concept name, unique across the ontology.
   * @param type The concept type.
   * @param description The description, or {@code null}.
   * @param extendsConcepts The concepts this one extends, or {@code null}.
   * @param derivedBy The derivation rules, or {@code null}.
   * @param identifyBy The identifying expressions, or {@code null}.
   * @param requires The SQL expressions, or {@code null}.
   * @param relationships The relationships this concept declares, or {@code null}.
   * @param iri The IRI, or {@code null}.
   */
  public OntologyConceptDTO(
      String concept,
      ConceptType type,
      @Nullable String description,
      @Nullable String[] extendsConcepts,
      @Nullable String[] derivedBy,
      @Nullable String[] identifyBy,
      @Nullable String[] requires,
      @Nullable OntologyRelationshipDTO[] relationships,
      @Nullable String iri) {
    this.concept = concept;
    this.type = type;
    this.description = description;
    this.extendsConcepts = extendsConcepts == null ? null : extendsConcepts.clone();
    this.derivedBy = derivedBy == null ? null : derivedBy.clone();
    this.identifyBy = identifyBy == null ? null : identifyBy.clone();
    this.requires = requires == null ? null : requires.clone();
    this.relationships = relationships == null ? null : relationships.clone();
    this.iri = iri;
  }

  /** This is the constructor that is used by Jackson deserializer. */
  public OntologyConceptDTO() {
    this.concept = null;
    this.type = null;
    this.description = null;
    this.extendsConcepts = null;
    this.derivedBy = null;
    this.identifyBy = null;
    this.requires = null;
    this.relationships = null;
    this.iri = null;
  }

  /**
   * Converts this DTO to the native concept.
   *
   * @return The native concept.
   */
  public OntologyConcept toOntologyConcept() {
    OntologyRelationship[] nativeRelationships =
        relationships == null
            ? null
            : Arrays.stream(relationships)
                .map(OntologyRelationshipDTO::toOntologyRelationship)
                .toArray(OntologyRelationship[]::new);
    return OntologyConcept.builder()
        .withConcept(concept)
        .withType(type)
        .withDescription(description)
        .withExtendsConcepts(extendsConcepts)
        .withDerivedBy(derivedBy)
        .withIdentifyBy(identifyBy)
        .withRequires(requires)
        .withRelationships(nativeRelationships)
        .withIri(iri)
        .build();
  }

  /**
   * Converts a native concept to its DTO form.
   *
   * @param concept The native concept.
   * @return The DTO.
   */
  public static OntologyConceptDTO fromOntologyConcept(OntologyConcept concept) {
    Preconditions.checkArgument(concept != null, "concept must not be null");

    OntologyRelationship[] nativeRelationships = concept.relationships();
    OntologyRelationshipDTO[] relationshipDTOs = null;
    if (nativeRelationships != null) {
      relationshipDTOs = new OntologyRelationshipDTO[nativeRelationships.length];
      for (int index = 0; index < nativeRelationships.length; index++) {
        relationshipDTOs[index] =
            OntologyRelationshipDTO.fromOntologyRelationship(nativeRelationships[index]);
      }
    }
    return new OntologyConceptDTO(
        concept.concept(),
        concept.type(),
        concept.description(),
        concept.extendsConcepts(),
        concept.derivedBy(),
        concept.identifyBy(),
        concept.requires(),
        relationshipDTOs,
        concept.iri());
  }

  /** Validates the concept DTO. */
  public void validate() {
    Preconditions.checkArgument(StringUtils.isNotBlank(concept), "concept must not be blank");
    Preconditions.checkArgument(type != null, "type must be set");
    if (relationships != null) {
      for (int index = 0; index < relationships.length; index++) {
        Preconditions.checkArgument(
            relationships[index] != null, "relationships[%s] must not be null", index);
        relationships[index].validate();
      }
    }
  }
}
