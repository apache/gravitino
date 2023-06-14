package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.meta.Lakehouse;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LakehouseResponse extends BaseResponse {

  @JsonProperty("lakehouse")
  private final Lakehouse lakehouse;

  LakehouseResponse(Lakehouse lakehouse) {
    super(0, null, null);
    this.lakehouse = lakehouse;
  }

  // This is the constructor that is used by Jackson deserializer
  LakehouseResponse() {
    super();
    this.lakehouse = null;
  }
}
