/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a response for a remove operation. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class RemoveResponse extends BaseResponse {

  @JsonProperty("removed")
  private final boolean removed;

  /**
   * Constructor for RemoveResponse.
   *
   * @param removed Whether the remove operation was successful.
   */
  public RemoveResponse(boolean removed) {
    super(0);
    this.removed = removed;
  }

  /** Default constructor for RemoveResponse (used by Jackson deserializer). */
  public RemoveResponse() {
    super();
    this.removed = false;
  }

  /**
   * Returns whether the remove operation was successful.
   *
   * @return True if the remove operation was successful, otherwise false.
   */
  public boolean removed() {
    return removed;
  }
}
