package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.dto.LakehouseDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LakehouseResponse extends BaseResponse {

  @JsonProperty("lakehouse")
  private final LakehouseDTO lakehouse;

  public LakehouseResponse(LakehouseDTO lakehouse) {
    super(0, null, null);
    this.lakehouse = lakehouse;
  }

  // This is the constructor that is used by Jackson deserializer
  public LakehouseResponse() {
    super();
    this.lakehouse = null;
  }
}
