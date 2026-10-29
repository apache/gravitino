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
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.ontology.OntologyRole;

/** The Data Transfer Object representing one role of an ontology relationship. */
@Getter
@EqualsAndHashCode
@ToString
public class OntologyRoleDTO {

  @JsonProperty("concept")
  private final String concept;

  @JsonProperty("name")
  @Nullable
  private final String name;

  /**
   * Creates a new ontology role DTO.
   *
   * @param concept The concept this role plays.
   * @param name The role name, or {@code null}.
   */
  public OntologyRoleDTO(String concept, @Nullable String name) {
    this.concept = concept;
    this.name = name;
  }

  /** This is the constructor that is used by Jackson deserializer. */
  public OntologyRoleDTO() {
    this.concept = null;
    this.name = null;
  }

  /**
   * Converts this DTO to the native role.
   *
   * @return The native role.
   */
  public OntologyRole toOntologyRole() {
    return OntologyRole.builder().withConcept(concept).withName(name).build();
  }

  /**
   * Converts a native role to its DTO form.
   *
   * @param role The native role.
   * @return The DTO.
   */
  public static OntologyRoleDTO fromOntologyRole(OntologyRole role) {
    Preconditions.checkArgument(role != null, "role must not be null");
    return new OntologyRoleDTO(role.concept(), role.name());
  }

  /** Validates the role DTO. */
  public void validate() {
    Preconditions.checkArgument(StringUtils.isNotBlank(concept), "concept must not be blank");
  }
}
