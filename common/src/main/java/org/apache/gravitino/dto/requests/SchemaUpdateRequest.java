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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to update a schema. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = SchemaUpdateRequest.SetSchemaPropertyRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = SchemaUpdateRequest.RemoveSchemaPropertyRequest.class,
      name = "removeProperty")
})
public interface SchemaUpdateRequest extends RESTRequest {

  /**
   * The schema change that is requested.
   *
   * @return An instance of SchemaChange.
   */
  SchemaChange schemaChange();

  /** Represents a request to set a property of a schema. */
  @EqualsAndHashCode
  @ToString
  class SetSchemaPropertyRequest implements SchemaUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @Getter
    @JsonProperty("value")
    private final String value;

    /**
     * Creates a new SetSchemaPropertyRequest.
     *
     * @param property The property to set.
     * @param value The value to set.
     */
    public SetSchemaPropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /** Default constructor for Jackson deserialization. */
    public SetSchemaPropertyRequest() {
      this(null, null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(value), "\"value\" field is required and cannot be empty");
    }

    /**
     * Returns the schema change.
     *
     * @return An instance of SchemaChange.
     */
    @Override
    public SchemaChange schemaChange() {
      return SchemaChange.setProperty(property, value);
    }
  }

  /** Represents a request to remove a property of a schema. */
  @EqualsAndHashCode
  @ToString
  class RemoveSchemaPropertyRequest implements SchemaUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    /**
     * Creates a new RemoveSchemaPropertyRequest.
     *
     * @param property The property to remove.
     */
    public RemoveSchemaPropertyRequest(String property) {
      this.property = property;
    }

    /** Default constructor for Jackson deserialization. */
    public RemoveSchemaPropertyRequest() {
      this(null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
    }

    /**
     * Returns the schema change.
     *
     * @return An instance of SchemaChange.
     */
    @Override
    public SchemaChange schemaChange() {
      return SchemaChange.removeProperty(property);
    }
  }
}
