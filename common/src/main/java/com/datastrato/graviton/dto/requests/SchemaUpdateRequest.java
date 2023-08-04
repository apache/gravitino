/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.requests;

import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

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

  SchemaChange schemaChange();

  @EqualsAndHashCode
  @ToString
  class SetSchemaPropertyRequest implements SchemaUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @Getter
    @JsonProperty("value")
    private final String value;

    public SetSchemaPropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    public SetSchemaPropertyRequest() {
      this(null, null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          property != null && !property.isEmpty(),
          "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(
          value != null && !value.isEmpty(), "\"value\" field is required and cannot be empty");
    }

    @Override
    public SchemaChange schemaChange() {
      return SchemaChange.setProperty(property, value);
    }
  }

  @EqualsAndHashCode
  @ToString
  class RemoveSchemaPropertyRequest implements SchemaUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    public RemoveSchemaPropertyRequest(String property) {
      this.property = property;
    }

    public RemoveSchemaPropertyRequest() {
      this(null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          property != null && !property.isEmpty(),
          "\"property\" field is required and cannot be empty");
    }

    @Override
    public SchemaChange schemaChange() {
      return SchemaChange.removeProperty(property);
    }
  }
}
