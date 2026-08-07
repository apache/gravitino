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
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.secret.SecretBindingDTO;
import org.apache.gravitino.dto.secret.SecretReferenceDTO;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to create a schema. */
@Getter
@EqualsAndHashCode
@ToString
public class SchemaCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @Nullable
  @JsonProperty("comment")
  private final String comment;

  @Nullable
  @JsonProperty("properties")
  private final Map<String, String> properties;

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("secretBindings")
  private final Map<String, SecretBindingDTO> secretBindings;

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("secretReferences")
  private final Map<String, SecretReferenceDTO> secretReferences;

  /** Default constructor for Jackson deserialization. */
  public SchemaCreateRequest() {
    this(null, null, null, null, null);
  }

  /**
   * Creates a new SchemaCreateRequest without secret maps.
   *
   * @param name The name of the schema.
   * @param comment The comment of the schema.
   * @param properties The properties of the schema.
   */
  public SchemaCreateRequest(String name, String comment, Map<String, String> properties) {
    this(name, comment, properties, null, null);
  }

  /**
   * Creates a new SchemaCreateRequest.
   *
   * @param name The name of the schema.
   * @param comment The comment of the schema.
   * @param properties The properties of the schema.
   * @param secretBindings Optional property key → binding DTO ({@code provider} + {@code
   *     plaintext}) for write-through secrets.
   * @param secretReferences Optional property key → secret locator DTO ({@code provider} plus
   *     provider-specific attributes).
   */
  @JsonCreator
  public SchemaCreateRequest(
      @JsonProperty("name") String name,
      @JsonProperty("comment") String comment,
      @JsonProperty("properties") Map<String, String> properties,
      @JsonProperty("secretBindings") Map<String, SecretBindingDTO> secretBindings,
      @JsonProperty("secretReferences") Map<String, SecretReferenceDTO> secretReferences) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
    this.secretBindings = secretBindings;
    this.secretReferences = secretReferences;
  }

  /**
   * Validates the request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
  }
}
