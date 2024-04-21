/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a response for a delete operation. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class DeleteResponse extends BaseResponse {

  @JsonProperty("deleted")
  private final boolean deleted;

  /**
   * Constructor for DeleteResponse.
   *
   * @param deleted Whether the delete operation was successful.
   */
  public DeleteResponse(boolean deleted) {
    super(0);
    this.deleted = deleted;
  }

  /** Default constructor for DeleteResponse (used by Jackson deserializer). */
  public DeleteResponse() {
    super();
    this.deleted = false;
  }

  /**
   * Returns whether the delete operation was successful.
   *
   * @return True if the delete operation was successful, otherwise false.
   */
  public boolean deleted() {
    return deleted;
  }
}
