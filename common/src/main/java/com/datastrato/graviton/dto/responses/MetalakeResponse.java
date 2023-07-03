package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.dto.MetalakeDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
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

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(metalake != null, "metalake must be non-null");
    Preconditions.checkArgument(
        metalake.name() != null && !metalake.name().isEmpty(),
        "metalake 'name' must be non-null and non-empty");
    Preconditions.checkArgument(
        metalake.auditInfo().creator() != null && !metalake.auditInfo().creator().isEmpty(),
        "metalake 'audit.creator' must be non-null and non-empty");
    Preconditions.checkArgument(
        metalake.auditInfo().createTime() != null, "metalake 'audit.createTime' must be non-null");
  }
}
