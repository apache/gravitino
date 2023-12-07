/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a response for a drop operation. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class DropResponse extends BaseResponse {

  @JsonProperty("dropped")
  private final boolean dropped;

  /**
   * Constructor for DropResponse.
   *
   * @param dropped Whether the drop operation was successful.
   */
  public DropResponse(boolean dropped) {
    super(0);
    this.dropped = dropped;
  }

  /** Default constructor for DropResponse (used by Jackson deserializer). */
  public DropResponse() {
    super();
    this.dropped = false;
  }

  /**
   * Returns whether the drop operation was successful.
   *
   * @return True if the drop operation was successful, otherwise false.
   */
  public boolean dropped() {
    return dropped;
  }
}
