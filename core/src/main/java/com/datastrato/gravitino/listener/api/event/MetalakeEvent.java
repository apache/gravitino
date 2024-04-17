/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an abstract base class for events related to Metalake operations. This class extends
 * {@link Event} to provide a more specific context involving operations on Metalakes, such as
 * creation, deletion, or modification. It captures essential information including the user
 * performing the operation and the identifier of the Metalake being operated on.
 */
@DeveloperApi
public abstract class MetalakeEvent extends Event {
  /**
   * Constructs a new {@code MetalakeEvent} with the specified user and Metalake identifier.
   *
   * @param user The user responsible for triggering the Metalake operation.
   * @param identifier The identifier of the Metalake involved in the operation.
   */
  protected MetalakeEvent(String user, NameIdentifier identifier) {
    super(user, identifier);
  }
}
