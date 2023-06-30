package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.dto.CatalogDTO;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class CatalogResponse extends BaseResponse {

  @JsonProperty("catalog")
  private final CatalogDTO catalog;

  public CatalogResponse(CatalogDTO catalog) {
    super(0, null, null);
    this.catalog = catalog;
  }

  // This is the constructor that is used by Jackson deserializer
  public CatalogResponse() {
    super();
    this.catalog = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(catalog != null, "catalog must be non-null");
    Preconditions.checkArgument(
        catalog.name() != null && !catalog.name().isEmpty(),
        "catalog 'name' must be non-null and non-empty");
    Preconditions.checkArgument(
        catalog.type() != null,
        "catalog 'type' must be non-null and non-empty");
    Preconditions.checkArgument(
        catalog.auditInfo().creator() != null && !catalog.auditInfo().creator().isEmpty(),
        "catalog 'audit.creator' must be non-null and non-empty");
    Preconditions.checkArgument(
        catalog.auditInfo().createTime() != null, "catalog 'audit.createTime' must be non-null");
  }
}
