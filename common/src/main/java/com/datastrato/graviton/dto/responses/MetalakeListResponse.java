package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.dto.MetalakeDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class MetalakeListResponse extends BaseResponse {

  @JsonProperty("metalakes")
  private final MetalakeDTO[] metalakes;

  public MetalakeListResponse(MetalakeDTO[] metalakes) {
    super(0);
    this.metalakes = metalakes;
  }

  // This is the constructor that is used by Jackson deserializer
  public MetalakeListResponse() {
    super();
    this.metalakes = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(metalakes != null, "metalakes must be non-null");
    Arrays.stream(metalakes)
        .forEach(
            metalake -> {
              Preconditions.checkArgument(
                  metalake.name() != null && !metalake.name().isEmpty(),
                  "metalake 'name' must be non-null and non-empty");
              Preconditions.checkArgument(
                  metalake.auditInfo().creator() != null
                      && !metalake.auditInfo().creator().isEmpty(),
                  "metalake 'audit.creator' must be non-null and non-empty");
              Preconditions.checkArgument(
                  metalake.auditInfo().createTime() != null,
                  "metalake 'audit.createTime' must be non-null");
            });
  }
}
