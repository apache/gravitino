/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.json.JsonUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a response containing a list of catalogs. */
@EqualsAndHashCode(callSuper = true)
@ToString
public class EntityListResponse extends BaseResponse {

  @JsonSerialize(contentUsing = JsonUtils.NameIdentifierSerializer.class)
  @JsonDeserialize(contentUsing = JsonUtils.NameIdentifierDeserializer.class)
  @JsonProperty("identifiers")
  private final NameIdentifier[] idents;

  /**
   * Constructor for EntityListResponse.
   *
   * @param idents The array of entity identifiers.
   */
  public EntityListResponse(NameIdentifier[] idents) {
    super(0);
    this.idents = idents;
  }

  /** Default constructor for EntityListResponse. (Used for Jackson deserialization.) */
  public EntityListResponse() {
    super();
    this.idents = null;
  }

  /**
   * Returns the array of entity identifiers.
   *
   * @return The array of identifiers.
   */
  public NameIdentifier[] identifiers() {
    return idents;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if catalog identifiers are not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(idents != null, "identifiers must not be null");
  }
}
