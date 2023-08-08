/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.dto.CatalogDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Represents a response containing catalog information. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class CatalogResponse extends BaseResponse {

  @JsonProperty("catalog")
  private final CatalogDTO catalog;

  /**
   * Constructor for CatalogResponse.
   *
   * @param catalog The catalog data transfer object.
   */
  public CatalogResponse(CatalogDTO catalog) {
    super(0);
    this.catalog = catalog;
  }

  /** Default constructor for CatalogResponse. (Used for Jackson deserialization.) */
  public CatalogResponse() {
    super();
    this.catalog = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if teh catalog name, type or audit is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(catalog != null, "catalog must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog.name()), "catalog 'name' must not be null and empty");
    Preconditions.checkArgument(catalog.type() != null, "catalog 'type' must not be null");
    Preconditions.checkArgument(catalog.auditInfo() != null, "catalog 'audit' must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog.auditInfo().creator()),
        "catalog 'audit.creator' must not be null and empty");
    Preconditions.checkArgument(
        catalog.auditInfo().createTime() != null, "catalog 'audit.createTime' must not be null");
  }
}
