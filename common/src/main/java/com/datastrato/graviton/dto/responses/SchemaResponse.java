/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.dto.rel.SchemaDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class SchemaResponse extends BaseResponse {

  @JsonProperty("schema")
  private final SchemaDTO schema;

  public SchemaResponse(SchemaDTO schema) {
    super(0);
    this.schema = schema;
  }

  // This is the constructor that is used by Jackson deserializer
  public SchemaResponse() {
    super();
    this.schema = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(schema != null, "schema must be non-null");
    Preconditions.checkArgument(
        schema.name() != null && !schema.name().isEmpty(),
        "schema 'name' must be non-null and non-empty");
    Preconditions.checkArgument(
        schema.auditInfo().creator() != null && !schema.auditInfo().creator().isEmpty(),
        "schema 'audit.creator' must be non-null and non-empty");
    Preconditions.checkArgument(
        schema.auditInfo().createTime() != null, "schema 'audit.createTime' must be non-null");
  }
}
