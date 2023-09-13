/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.dto.VersionDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response containing version of Graviton. */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class VersionResponse extends BaseResponse {

  @JsonProperty("version")
  private final VersionDTO version;

  /**
   * Constructor for VersionResponse.
   *
   * @param version The version of Graviton.
   */
  public VersionResponse(VersionDTO version) {
    super(0);
    this.version = version;
  }

  /** Default constructor for VersionResponse. (Used for Jackson deserialization.) */
  public VersionResponse() {
    super();
    this.version = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if name or audit information is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(version != null, "version DTO must be non-null");
    Preconditions.checkArgument(version.version() != null, "version must be non-null");
    Preconditions.checkArgument(version.compileDate() != null, "compile data must be non-null");
    Preconditions.checkArgument(version.gitCommit() != null, "Git commit id must be non-null");
  }
}
