package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.dto.MetalakeDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class MetalakeResponse extends BaseResponse {

  @JsonProperty("metalake")
  private final MetalakeDTO metalake;

  public MetalakeResponse(MetalakeDTO metalake) {
    super(0, null, null);
    this.metalake = metalake;
  }

  // This is the constructor that is used by Jackson deserializer
  public MetalakeResponse() {
    super();
    this.metalake = null;
  }
}
