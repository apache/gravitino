/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered when an attempt to list metalakes fails due to an
 * exception.
 */
@DeveloperApi
public final class ListMetalakeFailureEvent extends MetalakeFailureEvent {

  /**
   * Constructs a {@code ListMetalakeFailureEvent} instance.
   *
   * @param user The username of the individual who initiated the operation to list metalakes.
   * @param exception The exception encountered during the attempt to list metalakes.
   */
  public ListMetalakeFailureEvent(String user, Exception exception) {
    super(user, null, exception);
  }
}
