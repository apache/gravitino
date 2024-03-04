/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

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
