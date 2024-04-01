/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.CatalogDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response for a list of catalogs with their information. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class CatalogListResponse extends BaseResponse {

  @JsonProperty("catalogs")
  private final CatalogDTO[] catalogs;

  /**
   * Creates a new CatalogListResponse.
   *
   * @param catalogs The list of catalogs.
   */
  public CatalogListResponse(CatalogDTO[] catalogs) {
    super(0);
    this.catalogs = catalogs;
  }

  /**
   * This is the constructor that is used by Jackson deserializer to create an instance of
   * CatalogListResponse.
   */
  public CatalogListResponse() {
    super();
    this.catalogs = null;
  }
}
