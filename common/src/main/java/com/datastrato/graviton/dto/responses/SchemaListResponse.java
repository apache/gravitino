/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.json.JsonUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString
public class SchemaListResponse extends BaseResponse {

  @JsonSerialize(contentUsing = JsonUtils.NameIdentifierSerializer.class)
  @JsonDeserialize(contentUsing = JsonUtils.NameIdentifierDeserializer.class)
  @JsonProperty("schemas")
  private final NameIdentifier[] idents;

  public SchemaListResponse(NameIdentifier[] idents) {
    super(0);
    this.idents = idents;
  }

  // This is the constructor that is used by Jackson deserializer
  public SchemaListResponse() {
    super();
    this.idents = null;
  }

  public NameIdentifier[] schemas() {
    return idents;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(idents != null, "schemas must be non-null");
  }
}
