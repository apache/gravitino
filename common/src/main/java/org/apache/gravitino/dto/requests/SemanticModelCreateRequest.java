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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.semantic.SemanticModelDefinitionDTO;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.semantic.SemanticModelDefinition;

/** Represents a request to create a Semantic Model. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"name", "comment", "definition", "properties"})
public class SemanticModelCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @Nullable
  @JsonProperty("comment")
  private final String comment;

  @JsonProperty("definition")
  private final SemanticModelDefinitionDTO definition;

  @JsonProperty("properties")
  private final Map<String, String> properties;

  /** Default constructor for Jackson deserialization. */
  public SemanticModelCreateRequest() {
    this(null, null, null, null);
  }

  /**
   * Creates a Semantic Model create request.
   *
   * @param name The Semantic Model name.
   * @param comment The comment, or {@code null} if it is not set.
   * @param definition The required Semantic Model definition.
   * @param properties The required Gravitino-specific properties.
   */
  public SemanticModelCreateRequest(
      String name,
      @Nullable String comment,
      SemanticModelDefinitionDTO definition,
      Map<String, String> properties) {
    this.name = name;
    this.comment = comment;
    this.definition = definition;
    this.properties = properties;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
    Preconditions.checkArgument(
        definition != null, "\"definition\" field is required and cannot be null");
    Preconditions.checkArgument(
        properties != null, "\"properties\" field is required and cannot be null");

    toDefinition();
  }

  /**
   * Converts the definition in this request to an API definition.
   *
   * @return The Semantic Model definition.
   * @throws IllegalArgumentException If a definition field is invalid.
   */
  public SemanticModelDefinition toDefinition() {
    return definition.toDefinition();
  }
}
