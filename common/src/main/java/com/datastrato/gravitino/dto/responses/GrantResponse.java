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
  private final boolean added;

  /**
   * Constructor for GrantResponse.
   *
   * @param added Whether the remove operation was successful.
   */
  public GrantResponse(boolean added) {
    super(0);
    this.added = added;
  }

  /** Default constructor for GrantResponse (used by Jackson deserializer). */
  public GrantResponse() {
    super();
    this.added = false;
  }

  /**
   * Returns whether the grant operation was successful.
   *
   * @return True if the grant operation was successful, otherwise false.
   */
  public boolean added() {
    return added;
  }
}
