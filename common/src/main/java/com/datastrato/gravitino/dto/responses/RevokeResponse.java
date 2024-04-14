/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a response for a revoke operation. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class RevokeResponse extends BaseResponse {

  @JsonProperty("revoked")
  private final boolean revoked;

  /**
   * Constructor for RemoveResponse.
   *
   * @param revoked Whether the remove operation was successful.
   */
  public RevokeResponse(boolean revoked) {
    super(0);
    this.revoked = revoked;
  }

  /** Default constructor for RemoveResponse (used by Jackson deserializer). */
  public RevokeResponse() {
    super();
    this.revoked = false;
  }

  /**
   * Returns whether the remove operation was successful.
   *
   * @return True if the remove operation was successful, otherwise false.
   */
  public boolean removed() {
    return revoked;
  }
}
