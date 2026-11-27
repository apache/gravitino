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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.ontology.OntologyMapping;
import org.apache.gravitino.ontology.OntologyMappingDefinition;

/** The REST representation of the current mapping. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class OntologyMappingDTO implements OntologyMapping {

  private final String name;
  @Nullable private final String comment;
  private final String ontology;
  private final OntologyMappingDefinition definition;
  private final Map<String, String> properties;
  private final AuditDTO audit;

  /**
   * Creates the DTO for a stored mapping.
   *
   * @param name The current name.
   * @param comment The optional description.
   * @param ontology The referenced ontology name.
   * @param definition The native definition fields.
   * @param properties The Gravitino properties.
   * @param audit The audit information.
   */
  @JsonCreator
  public OntologyMappingDTO(
      @JsonProperty(value = "name", required = true) String name,
      @JsonProperty("comment") @Nullable String comment,
      @JsonProperty(value = "ontology", required = true) String ontology,
      @JsonProperty(value = "definition", required = true)
          @JsonDeserialize(using = OntologyDefinitionDeserializer.class)
          Map<String, Object> definition,
      @JsonProperty(value = "properties", required = true) Map<String, String> properties,
      @JsonProperty(value = "audit", required = true) AuditDTO audit) {
    this.name = name;
    this.comment = comment;
    this.ontology = ontology;
    this.definition = OntologyMappingDefinition.of(definition);
    this.properties = ImmutableMap.copyOf(properties);
    this.audit = audit;
  }

  /**
   * Copies the current mapping to the wire representation.
   *
   * @param item The current mapping.
   * @return The DTO.
   */
  public static OntologyMappingDTO from(OntologyMapping item) {
    return new OntologyMappingDTO(
        item.name(),
        item.comment(),
        item.ontology(),
        item.definition().fields(),
        item.properties(),
        DTOConverters.toDTO(item.auditInfo()));
  }

  /** {@inheritDoc} */
  @Override
  @JsonProperty("name")
  public String name() {
    return name;
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  @JsonProperty("comment")
  public String comment() {
    return comment;
  }

  /**
   * @return The name of the referenced ontology.
   */
  @Override
  @JsonProperty("ontology")
  public String ontology() {
    return ontology;
  }

  /** {@inheritDoc} */
  @Override
  public OntologyMappingDefinition definition() {
    return definition;
  }

  /**
   * @return The definition in its native JSON representation.
   */
  @JsonProperty("definition")
  public Map<String, Object> definitionFields() {
    return definition.fields();
  }

  /** {@inheritDoc} */
  @Override
  @JsonProperty("properties")
  public Map<String, String> properties() {
    return properties;
  }

  /** {@inheritDoc} */
  @Override
  @JsonProperty("audit")
  public AuditDTO auditInfo() {
    return audit;
  }
}
