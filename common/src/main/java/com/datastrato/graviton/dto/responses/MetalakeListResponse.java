/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.dto.MetalakeDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response containing a list of metalakes. */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class MetalakeListResponse extends BaseResponse {

  @JsonProperty("metalakes")
  private final MetalakeDTO[] metalakes;

  /**
   * Constructor for MetalakeListResponse.
   *
   * @param metalakes The array of metalake DTOs.
   */
  public MetalakeListResponse(MetalakeDTO[] metalakes) {
    super(0);
    this.metalakes = metalakes;
  }

  /** Default constructor for MetalakeListResponse. (Used for Jackson deserialization.) */
  public MetalakeListResponse() {
    super();
    this.metalakes = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if name or audit information is not set.
   */
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
