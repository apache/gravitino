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
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyDefinition;

/** The Data Transfer Object representing an ontology. */
@Getter
@EqualsAndHashCode
@ToString
public class OntologyDTO implements Ontology {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("comment")
  @Nullable
  private final String comment;

  @JsonProperty("properties")
  @Nullable
  private final Map<String, String> properties;

  @JsonProperty("status")
  private final Status status;

  @JsonProperty("definition")
  private final OntologyDefinitionDTO definition;

  @JsonProperty("audit")
  private final AuditDTO audit;

  /**
   * Creates a new ontology DTO.
   *
   * @param name The ontology name.
   * @param comment The comment, or {@code null}.
   * @param properties The Gravitino-specific properties, or {@code null}.
   * @param status The lifecycle status.
   * @param definition The concept graph.
   * @param audit The audit information.
   */
  public OntologyDTO(
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      Status status,
      OntologyDefinitionDTO definition,
      AuditDTO audit) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
    this.status = status;
    this.definition = definition;
    this.audit = audit;
  }

  /** This is the constructor that is used by Jackson deserializer. */
  public OntologyDTO() {
    this.name = null;
    this.comment = null;
    this.properties = null;
    this.status = null;
    this.definition = null;
    this.audit = null;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  @Nullable
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties == null ? Collections.emptyMap() : properties;
  }

  @Override
  public Status status() {
    return status;
  }

  @Override
  public OntologyDefinition definition() {
    return definition.toOntologyDefinition();
  }

  @Override
  public Audit auditInfo() {
    return audit;
  }

  /**
   * Converts a native ontology to its DTO form.
   *
   * @param ontology The native ontology.
   * @return The DTO.
   */
  public static OntologyDTO fromOntology(Ontology ontology) {
    Preconditions.checkArgument(ontology != null, "ontology must not be null");
    return new OntologyDTO(
        ontology.name(),
        ontology.comment(),
        ontology.properties().isEmpty() ? null : ontology.properties(),
        ontology.status(),
        OntologyDefinitionDTO.fromOntologyDefinition(ontology.definition()),
        DTOConverters.toDTO(ontology.auditInfo()));
  }

  /** Validates the ontology DTO. */
  public void validate() {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
    Preconditions.checkArgument(status != null, "status must be set");
    Preconditions.checkArgument(definition != null, "definition must be set");
    definition.validate();
    Preconditions.checkArgument(audit != null, "audit must be set");
  }
}
