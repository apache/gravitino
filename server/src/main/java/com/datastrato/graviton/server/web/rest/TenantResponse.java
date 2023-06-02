package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.schema.Tenant;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class TenantResponse extends BaseResponse {

  @JsonProperty("tenant")
  Tenant tenant;

  TenantResponse(Tenant tenant) {
    super(0, null, null);
    this.tenant = tenant;
  }

  // This is the constructor that is used by Jackson deserializer
  TenantResponse() {
    super();
    this.tenant = null;
  }
}
