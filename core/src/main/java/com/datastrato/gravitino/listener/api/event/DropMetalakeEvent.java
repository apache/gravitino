/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated after a Metalake is successfully removed from the system.
 */
@DeveloperApi
public final class DropMetalakeEvent extends MetalakeEvent {
  private final boolean isExists;

  /**
   * Constructs a new {@code DropMetalakeEvent} instance, encapsulating information about the
   * outcome of a metalake drop operation.
   *
   * @param user The user who initiated the drop metalake operation.
   * @param identifier The identifier of the metalake that was attempted to be dropped.
   * @param isExists A boolean flag indicating whether the metalake existed at the time of the drop
   *     operation.
   */
  public DropMetalakeEvent(String user, NameIdentifier identifier, boolean isExists) {
    super(user, identifier);
    this.isExists = isExists;
  }

  /**
   * Retrieves the existence status of the Metalake at the time of the removal operation.
   *
   * @return A boolean value indicating whether the Metalake existed. {@code true} if the Metalake
   *     existed, otherwise {@code false}.
   */
  public boolean isExists() {
    return isExists;
  }
}
