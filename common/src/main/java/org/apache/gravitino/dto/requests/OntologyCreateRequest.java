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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.ontology.OntologyDefinitionDeserializer;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.rest.RESTRequest;

/** A request to create a native ontology. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class OntologyCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("comment")
  @Nullable
  private final String comment;

  @JsonProperty("definition")
  private final Map<String, Object> definition;

  @JsonProperty("properties")
  private final Map<String, String> properties;

  /**
   * Creates a native create request.
   *
   * @param name The new ontology name.
   * @param comment The optional description.
   * @param definition The native definition fields.
   * @param properties The optional Gravitino properties.
   */
  @JsonCreator
  public OntologyCreateRequest(
      @JsonProperty(value = "name", required = true) String name,
      @JsonProperty("comment") @Nullable String comment,
      @JsonProperty(value = "definition", required = true)
          @JsonDeserialize(using = OntologyDefinitionDeserializer.class)
          Map<String, Object> definition,
      @JsonProperty("properties") @Nullable Map<String, String> properties) {
    this.name = name;
    this.comment = comment;
    this.definition = definition;
    this.properties = properties == null ? Collections.emptyMap() : ImmutableMap.copyOf(properties);
  }

  /**
   * @return The new ontology name.
   */
  public String name() {
    return name;
  }

  /**
   * @return The optional comment.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * @return The native definition.
   */
  public OntologyDefinition definition() {
    return OntologyDefinition.of(definition);
  }

  /**
   * @return The Gravitino properties.
   */
  public Map<String, String> properties() {
    return properties;
  }

  /** {@inheritDoc} */
  @Override
  public void validate() {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "name is required");
    Preconditions.checkArgument(definition != null, "definition is required");
  }
}
