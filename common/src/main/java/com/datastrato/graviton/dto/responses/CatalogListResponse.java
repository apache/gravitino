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

/** Represents a response containing a list of catalogs. */
@EqualsAndHashCode(callSuper = true)
@ToString
public class CatalogListResponse extends BaseResponse {

  @JsonSerialize(contentUsing = JsonUtils.NameIdentifierSerializer.class)
  @JsonDeserialize(contentUsing = JsonUtils.NameIdentifierDeserializer.class)
  @JsonProperty("catalogs")
  private final NameIdentifier[] idents;

  /**
   * Constructor for CatalogListResponse. (Used for Jackson deserialization.)
   *
   * @param idents The array of catalog identifiers.
   */
  public CatalogListResponse(NameIdentifier[] idents) {
    super(0);
    this.idents = idents;
  }

  /** Default constructor for CatalogListResponse. */
  public CatalogListResponse() {
    super();
    this.idents = null;
  }

  /**
   * Returns the array of catalog identifiers.
   *
   * @return The array of identifiers.
   */
  public NameIdentifier[] getCatalogs() {
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
    Preconditions.checkArgument(idents != null, "catalogs must be non-null");
  }
}
