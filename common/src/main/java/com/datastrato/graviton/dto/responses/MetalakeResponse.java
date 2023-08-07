/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.dto.MetalakeDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response containing metalake information. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class MetalakeResponse extends BaseResponse {

  @JsonProperty("metalake")
  private final MetalakeDTO metalake;

  /**
   * Constructor for MetalakeResponse.
   *
   * @param metalake The metalake DTO.
   */
  public MetalakeResponse(MetalakeDTO metalake) {
    super(0);
    this.metalake = metalake;
  }

  /** Default constructor for MetalakeResponse. (Used for Jackson deserialization.) */
  public MetalakeResponse() {
    super();
    this.metalake = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if the metalake, name or audit information is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

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
