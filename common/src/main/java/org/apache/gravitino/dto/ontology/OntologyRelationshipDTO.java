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
import org.apache.gravitino.ontology.Multiplicity;
import org.apache.gravitino.ontology.OntologyRelationship;
import org.apache.gravitino.ontology.OntologyRole;

/** The Data Transfer Object representing one relationship of an ontology concept. */
@Getter
@EqualsAndHashCode
@ToString
public class OntologyRelationshipDTO {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("description")
  @Nullable
  private final String description;

  @JsonProperty("verbalizes")
  private final String[] verbalizes;

  @JsonProperty("roles")
  @Nullable
  private final OntologyRoleDTO[] roles;

  @JsonProperty("multiplicity")
  @Nullable
  private final Multiplicity multiplicity;

  @JsonProperty("derivedBy")
  @Nullable
  private final String[] derivedBy;

  @JsonProperty("requires")
  @Nullable
  private final String[] requires;

  @JsonProperty("iri")
  @Nullable
  private final String iri;

  /**
   * Creates a new ontology relationship DTO.
   *
   * @param name The relationship name, unique across the ontology.
   * @param description The description, or {@code null}.
   * @param verbalizes The phrasings that express the relationship, at least one.
   * @param roles The roles this relationship connects, or {@code null}.
   * @param multiplicity The multiplicity, or {@code null}.
   * @param derivedBy The derivation rules, or {@code null}.
   * @param requires The SQL expressions, or {@code null}.
   * @param iri The IRI, or {@code null}.
   */
  public OntologyRelationshipDTO(
      String name,
      @Nullable String description,
      String[] verbalizes,
      @Nullable OntologyRoleDTO[] roles,
      @Nullable Multiplicity multiplicity,
      @Nullable String[] derivedBy,
      @Nullable String[] requires,
      @Nullable String iri) {
    this.name = name;
    this.description = description;
    this.verbalizes = verbalizes == null ? null : verbalizes.clone();
    this.roles = roles == null ? null : roles.clone();
    this.multiplicity = multiplicity;
    this.derivedBy = derivedBy == null ? null : derivedBy.clone();
    this.requires = requires == null ? null : requires.clone();
    this.iri = iri;
  }

  /** This is the constructor that is used by Jackson deserializer. */
  public OntologyRelationshipDTO() {
    this.name = null;
    this.description = null;
    this.verbalizes = null;
    this.roles = null;
    this.multiplicity = null;
    this.derivedBy = null;
    this.requires = null;
    this.iri = null;
  }

  /**
   * Converts this DTO to the native relationship.
   *
   * @return The native relationship.
   */
  public OntologyRelationship toOntologyRelationship() {
    OntologyRole[] nativeRoles =
        roles == null
            ? null
            : Arrays.stream(roles)
                .map(OntologyRoleDTO::toOntologyRole)
                .toArray(OntologyRole[]::new);
    return OntologyRelationship.builder()
        .withName(name)
        .withDescription(description)
        .withVerbalizes(verbalizes)
        .withRoles(nativeRoles)
        .withMultiplicity(multiplicity)
        .withDerivedBy(derivedBy)
        .withRequires(requires)
        .withIri(iri)
        .build();
  }

  /**
   * Converts a native relationship to its DTO form.
   *
   * @param relationship The native relationship.
   * @return The DTO.
   */
  public static OntologyRelationshipDTO fromOntologyRelationship(
      OntologyRelationship relationship) {
    Preconditions.checkArgument(relationship != null, "relationship must not be null");

    OntologyRole[] nativeRoles = relationship.roles();
    OntologyRoleDTO[] roleDTOs = null;
    if (nativeRoles != null) {
      roleDTOs = new OntologyRoleDTO[nativeRoles.length];
      for (int index = 0; index < nativeRoles.length; index++) {
        roleDTOs[index] = OntologyRoleDTO.fromOntologyRole(nativeRoles[index]);
      }
    }
    return new OntologyRelationshipDTO(
        relationship.name(),
        relationship.description(),
        relationship.verbalizes(),
        roleDTOs,
        relationship.multiplicity(),
        relationship.derivedBy(),
        relationship.requires(),
        relationship.iri());
  }

  /** Validates the relationship DTO. */
  public void validate() {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
    Preconditions.checkArgument(
        verbalizes != null && verbalizes.length > 0, "verbalizes must have at least one entry");
    for (int index = 0; index < verbalizes.length; index++) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(verbalizes[index]), "verbalizes[%s] must not be blank", index);
    }
    if (roles != null) {
      for (int index = 0; index < roles.length; index++) {
        Preconditions.checkArgument(roles[index] != null, "roles[%s] must not be null", index);
        roles[index].validate();
      }
    }
  }
}
