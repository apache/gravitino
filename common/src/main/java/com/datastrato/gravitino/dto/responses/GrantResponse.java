/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class GrantResponse extends BaseResponse {

  @JsonProperty("granted")
  private final boolean granted;

  /**
   * Constructor for GrantResponse.
   *
   * @param granted Whether the remove operation was successful.
   */
  public GrantResponse(boolean granted) {
    super(0);
    this.granted = granted;
  }

  /** Default constructor for GrantResponse (used by Jackson deserializer). */
  public GrantResponse() {
    super();
    this.granted = false;
  }

  /**
   * Returns whether the grant operation was successful.
   *
   * @return True if the grant operation was successful, otherwise false.
   */
  public boolean granted() {
    return granted;
  }
}
