/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.dto.CatalogDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class CatalogListResponse extends BaseResponse {

  @JsonProperty("catalogs")
  private final CatalogDTO[] catalogs;

  public CatalogListResponse(CatalogDTO[] catalogs) {
    super(0);
    this.catalogs = catalogs;
  }

  // This is the constructor that is used by Jackson deserializer
  public CatalogListResponse() {
    super();
    this.catalogs = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(catalogs != null, "catalogs must be non-null");
    Arrays.stream(catalogs)
        .forEach(
            catalog -> {
              Preconditions.checkArgument(
                  catalog.name() != null && !catalog.name().isEmpty(),
                  "catalog 'name' must be non-null and non-empty");
              Preconditions.checkArgument(
                  catalog.type() != null, "catalog 'type' must be non-null and non-empty");
              Preconditions.checkArgument(
                  catalog.auditInfo().creator() != null && !catalog.auditInfo().creator().isEmpty(),
                  "catalog 'audit.creator' must be non-null and non-empty");
              Preconditions.checkArgument(
                  catalog.auditInfo().createTime() != null,
                  "catalog 'audit.createTime' must be non-null");
            });
  }
}
