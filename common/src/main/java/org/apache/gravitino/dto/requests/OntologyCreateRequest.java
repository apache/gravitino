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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.ontology.OntologyDefinitionDTO;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to create an ontology. */
@Getter
@EqualsAndHashCode
@ToString
public class OntologyCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("comment")
  @Nullable
  private final String comment;

  @JsonProperty("properties")
  @Nullable
  private final Map<String, String> properties;

  @JsonProperty("definition")
  private final OntologyDefinitionDTO definition;

  /**
   * Creates a new ontology create request.
   *
   * @param name The ontology name.
   * @param comment The comment, or {@code null}.
   * @param properties The Gravitino-specific properties, or {@code null}.
   * @param definition The concept graph.
   */
  public OntologyCreateRequest(
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      OntologyDefinitionDTO definition) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
    this.definition = definition;
  }

  /** This is the constructor that is used by Jackson deserializer. */
  public OntologyCreateRequest() {
    this.name = null;
    this.comment = null;
    this.properties = null;
    this.definition = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "\"name\" must not be blank");
    Preconditions.checkArgument(definition != null, "\"definition\" must not be null");
    definition.validate();
  }
}
