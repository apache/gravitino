/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated when an attempt to load a Metalake into the system fails
 * due to an exception.
 */
@DeveloperApi
public final class LoadMetalakeFailureEvent extends MetalakeFailureEvent {
  /**
   * Constructs a {@code LoadMetalakeFailureEvent} instance.
   *
   * @param user The user who initiated the metalake loading operation.
   * @param identifier The identifier of the metalake that the loading attempt was made for.
   * @param exception The exception that was thrown during the metalake loading operation, offering
   *     insight into the issues encountered.
   */
  public LoadMetalakeFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
