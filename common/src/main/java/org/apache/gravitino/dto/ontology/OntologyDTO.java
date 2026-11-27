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
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyDefinition;

/** The REST representation of the current ontology. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class OntologyDTO implements Ontology {

  private final String name;
  @Nullable private final String comment;
  private final OntologyDefinition definition;
  private final Map<String, String> properties;
  private final AuditDTO audit;

  /**
   * Creates the DTO for a stored ontology.
   *
   * @param name The current name.
   * @param comment The optional description.
   * @param definition The native definition fields.
   * @param properties The Gravitino properties.
   * @param audit The audit information.
   */
  @JsonCreator
  public OntologyDTO(
      @JsonProperty(value = "name", required = true) String name,
      @JsonProperty("comment") @Nullable String comment,
      @JsonProperty(value = "definition", required = true)
          @JsonDeserialize(using = OntologyDefinitionDeserializer.class)
          Map<String, Object> definition,
      @JsonProperty(value = "properties", required = true) Map<String, String> properties,
      @JsonProperty(value = "audit", required = true) AuditDTO audit) {
    this.name = name;
    this.comment = comment;
    this.definition = OntologyDefinition.of(definition);
    this.properties = ImmutableMap.copyOf(properties);
    this.audit = audit;
  }

  /**
   * Copies the current ontology to the wire representation.
   *
   * @param item The current ontology.
   * @return The DTO.
   */
  public static OntologyDTO from(Ontology item) {
    return new OntologyDTO(
        item.name(),
        item.comment(),
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

  /** {@inheritDoc} */
  @Override
  public OntologyDefinition definition() {
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
