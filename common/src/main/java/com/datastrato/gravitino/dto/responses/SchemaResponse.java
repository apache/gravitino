/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
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
        StringUtils.isNotBlank(schema.name()), "schema 'name' must not be null and empty");
    Preconditions.checkArgument(schema.auditInfo() != null, "schema 'audit' must not be null");
  }
}
