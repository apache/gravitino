/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.rel.SchemaDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Represents a response for a schema. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class SchemaResponse extends BaseResponse {

  @JsonProperty("schema")
  private final SchemaDTO schema;

  /**
   * Creates a new SchemaResponse.
   *
   * @param schema The schema DTO object.
   */
  public SchemaResponse(SchemaDTO schema) {
    super(0);
    this.schema = schema;
  }

  /** This is the constructor that is used by Jackson deserializer */
  public SchemaResponse() {
    super();
    this.schema = null;
  }

  /**
   * Validates the response.
   *
   * @throws IllegalArgumentException If the response is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(schema != null, "schema must be non-null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(schema.name()), "schema 'name' must not be null and empty");
    Preconditions.checkArgument(schema.auditInfo() != null, "schema 'audit' must not be null");
  }
}
