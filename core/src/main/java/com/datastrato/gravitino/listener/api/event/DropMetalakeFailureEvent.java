/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated when an attempt to remove a Metalake from the system fails
 * due to an exception.
 */
@DeveloperApi
public final class DropMetalakeFailureEvent extends MetalakeFailureEvent {
  /**
   * Constructs a new {@code DropMetalakeFailureEvent} instance, capturing detailed information
   * about the failed attempt to drop a metalake.
   *
   * @param user The user who initiated the drop metalake operation.
   * @param identifier The identifier of the metalake that the operation attempted to drop.
   * @param exception The exception that was thrown during the drop metalake operation, offering
   *     insights into what went wrong and why the operation failed.
   */
  public DropMetalakeFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
