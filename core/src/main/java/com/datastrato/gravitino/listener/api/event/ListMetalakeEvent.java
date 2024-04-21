/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.annotation.DeveloperApi;

/** Represents an event that is triggered upon the successful list of metalakes. */
@DeveloperApi
public final class ListMetalakeEvent extends MetalakeEvent {
  /**
   * Constructs an instance of {@code ListMetalakeEvent}.
   *
   * @param user The username of the individual who initiated the metalake listing.
   */
  public ListMetalakeEvent(String user) {
    super(user, null);
  }
}
